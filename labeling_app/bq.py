"""All BigQuery access for the labeling app.

User-supplied values travel only as query parameters; table paths come from
config and are built from a validated project id.

Answers are appended to silver.sentiment_label as HUMAN rows. Nothing
is updated or deleted: a changed answer is a new row, and the latest row per
(article version, country, labeler) is the current one. The queue view decides
what still needs a human (model disagreement, random audit, second opinion)
and caps every item at two humans.
"""

import threading
import time
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

from config import config

_client: bigquery.Client | None = None
_client_lock = threading.Lock()

_labelers: dict[str, dict] = {}
_labelers_at: float | None = None
_labelers_lock = threading.Lock()
_MIN_FORCED_REFRESH_INTERVAL = 5.0


def get_client() -> bigquery.Client:
    global _client
    with _client_lock:
        if _client is None:
            _client = bigquery.Client(project=config.bq_project, location=config.bq_location)
        return _client


def _query(sql: str, params: list) -> list[dict]:
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in get_client().query(sql, job_config=job_config).result()]


def _p(name, value, kind="STRING"):
    return bigquery.ScalarQueryParameter(name, kind, value)


# ── labelers ─────────────────────────────────────────────────────────────────

def refresh_labelers(force: bool = False) -> dict[str, dict]:
    """{login_code: {labeler_id, languages}} for active labelers, cached with a
    TTL. A stale cache keeps serving if a refresh fails."""
    global _labelers, _labelers_at
    with _labelers_lock:
        age = None if _labelers_at is None else time.monotonic() - _labelers_at
        if age is not None:
            if not force and age <= config.code_cache_ttl:
                return _labelers
            if force and age < _MIN_FORCED_REFRESH_INTERVAL:
                return _labelers
        try:
            rows = _query(f"""
                SELECT labeler_id, login_code, languages, active
                FROM `{config.labelers_table}`
                WHERE TRUE
                QUALIFY ROW_NUMBER() OVER (PARTITION BY labeler_id ORDER BY valid_from DESC) = 1
            """, [])
        except Exception:
            if _labelers_at is not None:
                return _labelers
            raise
        _labelers = {r["login_code"]: {"labeler_id": r["labeler_id"],
                                       "languages": list(r["languages"] or [])}
                     for r in rows if r["active"]}
        _labelers_at = time.monotonic()
        return _labelers


def find_labeler(code: str) -> dict | None:
    labelers = refresh_labelers()
    if code not in labelers:
        labelers = refresh_labelers(force=True)
    return labelers.get(code)
