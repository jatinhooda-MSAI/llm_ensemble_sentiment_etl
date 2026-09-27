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


# ── queue ────────────────────────────────────────────────────────────────────

def _language_filter(languages: list[str] | None, params: list) -> str:
    if not languages:
        return ""
    params.append(bigquery.ArrayQueryParameter("languages", "STRING", languages))
    return "AND q.language_iso639_3 IN UNNEST(@languages)"


def allowed_languages(labeler: dict, wanted: list[str] | None) -> list[str] | None:
    """The labeler's own languages, narrowed by the UI filter. None = no limit."""
    own = labeler["languages"]
    if wanted:
        return [l for l in wanted if not own or l in own] or own or None
    return own or None


def next_item(labeler: dict, languages: list[str] | None) -> tuple[dict | None, int]:
    params = [_p("labeler_id", labeler["labeler_id"])]
    lang_sql = _language_filter(languages, params)
    rows = _query(f"""
        SELECT q.doc_id, q.source_version, q.country_iso3, q.language_iso639_3 AS language,
               q.reason, c.country_territory AS country_name, d.title, d.body_text AS body,
               COUNT(*) OVER () AS remaining
        FROM `{config.queue_view}` AS q
        JOIN `{config.document_view}` AS d USING (doc_id, source_version)
        LEFT JOIN `{config.countries_table}` AS c ON c.iso3 = q.country_iso3 AND c.is_current
        WHERE @labeler_id NOT IN UNNEST(q.labeled_by)
          {lang_sql}
        ORDER BY q.priority, q.published_date DESC, q.doc_id, q.country_iso3
        LIMIT 1
    """, params)
    if not rows:
        return None, 0
    row = rows[0]
    remaining = int(row.pop("remaining"))
    row.pop("reason", None)  # labelers are not told why an item is in the queue
    return row, remaining


def queue_languages(labeler: dict) -> list[str]:
    params = []
    lang_sql = _language_filter(labeler["languages"] or None, params)
    rows = _query(f"""
        SELECT DISTINCT q.language_iso639_3 AS language
        FROM `{config.queue_view}` AS q
        WHERE q.language_iso639_3 IS NOT NULL {lang_sql}
        ORDER BY language
    """, params)
    return [r["language"] for r in rows]


def _queue_row(doc_id: str, source_version: str, country: str) -> dict | None:
    rows = _query(f"""
        SELECT reason, labeled_by, published_date FROM `{config.queue_view}`
        WHERE doc_id = @doc_id AND source_version = @version AND country_iso3 = @country
    """, [_p("doc_id", doc_id), _p("version", source_version), _p("country", country)])
    return rows[0] if rows else None


def _my_latest(labeler_id: str, doc_id: str, source_version: str, country: str) -> dict | None:
    rows = _query(f"""
        SELECT is_about_country, sentiment_label, is_skip, reason, published_date, created_at
        FROM `{config.labels_table}`
        WHERE labeler_type = 'HUMAN' AND labeler_id = @labeler_id
          AND doc_id = @doc_id AND source_version = @version AND country_iso3 = @country
        ORDER BY created_at DESC LIMIT 1
    """, [_p("labeler_id", labeler_id), _p("doc_id", doc_id), _p("version", source_version),
          _p("country", country)])
    return rows[0] if rows else None


class NotAvailable(Exception):
    """The item no longer needs this labeler (already answered by two humans)."""


def insert_label(labeler: dict, doc_id: str, source_version: str, country: str,
                 is_about: bool | None, sentiment: str | None, is_skip: bool, session_id: str) -> dict:
    previous = _my_latest(labeler["labeler_id"], doc_id, source_version, country)
    if previous:  # a revision of this labeler's own answer
        reason, published_date = previous["reason"], previous["published_date"]
    else:
        queued = _queue_row(doc_id, source_version, country)
        if not queued or labeler["labeler_id"] in (queued["labeled_by"] or []):
            raise NotAvailable("this item no longer needs an answer")
        reason, published_date = queued["reason"], queued["published_date"]
    now = datetime.now(timezone.utc)
    row = {
        "label_id": str(uuid.uuid4()),
        "doc_id": doc_id,
        "source_version": source_version,
        "country_iso3": country,
        "labeler_type": "HUMAN",
        "labeler_id": labeler["labeler_id"],
        "tag_status": None,
        "is_about_country": None if is_skip else is_about,
        "sentiment_label": None if (is_skip or not is_about) else sentiment,
        "sentiment_score": None,
        "evidence": [],
        "reason": reason,
        "is_skip": is_skip,
        "run_id": f"labeling-app:{config.app_version}:{session_id}"[:200],
        "published_date": published_date.isoformat() if published_date else None,
        "created_at": now.isoformat(),
        "error_detail": None,
    }
    errors = get_client().insert_rows_json(config.labels_table, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    return {"label_id": row["label_id"], "labeled_at": row["created_at"], "is_revision": bool(previous)}
