"""Sentiment labeling web app: FastAPI entrypoint.

Serves the single-page UI plus a small JSON API over BigQuery. Every /api/*
endpoint except login requires an X-Labeler-Code header, validated against
gold.dim_labeler.

Each item is one (article, country) pair from silver.labeling_queue.
The labeler answers: is the article substantially about this country, and if
so, what is its sentiment toward it. Model answers are never shown.

Local run:
    BQ_PROJECT=your-gcp-project uvicorn main:app --port 8080
"""

import logging
import uuid
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse
from google.api_core import exceptions as gcloud_exceptions
from pydantic import BaseModel

import bq
from config import SENTIMENTS, config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("labeling_app")

app = FastAPI(title="Sentiment Labeling App", docs_url=None, redoc_url=None)

STATIC_DIR = Path(__file__).parent / "static"


def _error(status: int, error: str, detail: str = "") -> HTTPException:
    return HTTPException(status_code=status, detail={"error": error, "detail": detail})


def _require_config() -> None:
    err = config.config_error()
    if err:
        raise _error(503, "app_not_configured", err)


def _bq_call(fn, *args, **kwargs):
    """Run a bq.py operation, mapping BigQuery failures to JSON errors."""
    try:
        return fn(*args, **kwargs)
    except HTTPException:
        raise
    except bq.NotAvailable as exc:
        raise _error(409, "item_not_available", str(exc))
    except gcloud_exceptions.NotFound as exc:
        raise _error(503, "app_not_configured", f"BigQuery table missing: {exc}")
    except Exception as exc:
        logger.exception("BigQuery call failed")
        raise _error(502, "bq_error", str(exc))


def require_labeler(x_labeler_code: str = Header(default="")) -> dict:
    _require_config()
    code = x_labeler_code.strip()
    if not code:
        raise _error(401, "invalid_code", "missing X-Labeler-Code header")
    labeler = _bq_call(bq.find_labeler, code)
    if not labeler:
        raise _error(401, "invalid_code", "code not recognized or inactive")
    return labeler


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def healthz() -> dict:
    return {"status": "ok", "version": config.app_version}


class LoginRequest(BaseModel):
    code: str


@app.post("/api/login")
def login(body: LoginRequest) -> dict:
    _require_config()
    code = body.code.strip()
    if not code:
        raise _error(401, "invalid_code", "empty code")
    labeler = _bq_call(bq.find_labeler, code)
    if not labeler:
        raise _error(401, "invalid_code", "code not recognized or inactive")
    return {"valid": True, "display_name": code, "languages": labeler["languages"],
            "session_id": uuid.uuid4().hex[:12]}


@app.get("/api/meta")
def meta(labeler: dict = Depends(require_labeler)) -> dict:
    return {"languages": _bq_call(bq.queue_languages, labeler), "sentiments": list(SENTIMENTS)}
