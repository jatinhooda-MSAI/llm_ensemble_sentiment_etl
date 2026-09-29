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


@app.get("/api/next-item")
def next_item(languages: str = Query(""), labeler: dict = Depends(require_labeler)) -> dict:
    wanted = [lang.strip() for lang in languages.split(",") if lang.strip()]
    item, remaining = _bq_call(bq.next_item, labeler, bq.allowed_languages(labeler, wanted))
    return {"item": item, "remaining": remaining}


class LabelRequest(BaseModel):
    doc_id: str
    source_version: str
    country_iso3: str
    is_about_country: bool | None = None
    sentiment: str | None = None
    is_skip: bool = False
    session_id: str = ""


@app.post("/api/labels")
def post_label(body: LabelRequest, labeler: dict = Depends(require_labeler)) -> dict:
    country = body.country_iso3.strip().upper()
    if not (body.doc_id.strip() and body.source_version.strip() and len(country) == 3):
        raise _error(422, "invalid_item", "doc_id, source_version and country_iso3 are required")
    sentiment = (body.sentiment or "").strip().upper() or None
    if not body.is_skip:
        if body.is_about_country is None:
            raise _error(422, "missing_answer", "is_about_country is required")
        if body.is_about_country and sentiment not in SENTIMENTS:
            raise _error(422, "invalid_sentiment", f"must be one of {list(SENTIMENTS)}")
    result = _bq_call(bq.insert_label, labeler, body.doc_id.strip(), body.source_version.strip(),
                      country, body.is_about_country, sentiment, body.is_skip, body.session_id)
    return {"ok": True, **result}


@app.get("/api/my-labels")
def my_labels(order: str = Query("desc"), limit: int = Query(200),
              labeler: dict = Depends(require_labeler)) -> dict:
    limit = max(1, min(limit, config.labels_page_limit))
    return {"labels": _bq_call(bq.my_labels, labeler, order.lower() != "asc", limit)}


@app.get("/api/items")
def get_item(doc_id: str, source_version: str, country_iso3: str,
             labeler: dict = Depends(require_labeler)) -> dict:
    result = _bq_call(bq.get_item, labeler, doc_id, source_version, country_iso3.upper())
    if result is None:
        raise _error(404, "item_not_found", doc_id)
    return result
