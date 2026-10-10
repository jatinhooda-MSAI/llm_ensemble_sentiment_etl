"""
BigQuery reads and writes for sentiment scoring. Everything is append-only.
"""

import json
import logging
import uuid
from datetime import date, datetime, timezone

from etl.sentiment_scoring.config import (
    BATCH_JOB_TABLE,
    BQ_PROJECT,
    COUNTRY_TABLE,
    DOCUMENT_VIEW,
    LABEL_TABLE,
    MODEL_VERSION_TABLE,
    RUN_TABLE,
    TASK,
    fq,
)

log = logging.getLogger("sentiment_scoring")

IN_FLIGHT_STATES = ("SUBMITTED", "RUNNING", "SUCCEEDED")  # SUCCEEDED = done, not collected yet


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def get_client():
    """BigQuery client. Local runs can pass GCLOUD_TOKEN (gcloud auth
    print-access-token) when application-default credentials have expired;
    Cloud Run uses its service account."""
    import os

    from google.cloud import bigquery

    fq(LABEL_TABLE)  # fail early if BQ_PROJECT is unset
    token = os.getenv("GCLOUD_TOKEN")
    if token:
        from google.oauth2.credentials import Credentials

        return bigquery.Client(project=BQ_PROJECT, credentials=Credentials(token))
    return bigquery.Client(project=BQ_PROJECT)


def _query(client, sql: str, params: dict = None):
    from google.cloud import bigquery

    qp = []
    for name, value in (params or {}).items():
        if isinstance(value, (list, tuple)):
            qp.append(bigquery.ArrayQueryParameter(name, "STRING", list(value)))
        elif isinstance(value, int):
            qp.append(bigquery.ScalarQueryParameter(name, "INT64", value))
        else:
            qp.append(bigquery.ScalarQueryParameter(name, "STRING", value))
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result())


def _stream(client, table: str, rows: list):
    if not rows:
        return
    errors = client.insert_rows_json(fq(table), [{k: _iso(v) for k, v in r.items()} for r in rows])
    if errors:
        raise RuntimeError(f"insert into {table} failed: {errors[:3]}")


# ── Registries ────────────────────────────────────────────────────────────────

def load_countries(client) -> list:
    """(iso3, name) for the current AU-55 rows of the existing dim_country."""
    rows = _query(client, f"""
        SELECT iso3, country_territory FROM `{fq(COUNTRY_TABLE)}`
        WHERE is_current
    """)
    return [(r.iso3, r.country_territory) for r in rows]


def current_model_versions(client) -> dict:
    rows = _query(client, f"""
        SELECT model_version, status, params FROM `{fq(MODEL_VERSION_TABLE)}`
        WHERE task = @task
        QUALIFY ROW_NUMBER() OVER (PARTITION BY model_version ORDER BY recorded_at DESC) = 1
    """, {"task": TASK})
    return {r.model_version: r.status for r in rows}


def register_model_versions(client, entries: list) -> list:
    """Make exactly the configured versions ACTIVE: register new ones, retire
    any other ACTIVE version. Returns the rows written."""
    current = current_model_versions(client)
    wanted = {e["model_version"] for e in entries}
    now = utcnow()
    rows = []
    for e in entries:
        if current.get(e["model_version"]) != "ACTIVE":
            rows.append({**e, "task": TASK, "status": "ACTIVE", "recorded_at": now,
                         "params": json.dumps(e.get("params") or {})})
    for model_version, status in current.items():
        if status == "ACTIVE" and model_version not in wanted:
            old = _query(client, f"""
                SELECT * FROM `{fq(MODEL_VERSION_TABLE)}` WHERE model_version = @mv
                QUALIFY ROW_NUMBER() OVER (ORDER BY recorded_at DESC) = 1
            """, {"mv": model_version})[0]
            row = {k: old[k] for k in old.keys()}
            params = row.get("params")
            row.update(status="RETIRED", recorded_at=now, notes="replaced in config",
                       params=params if isinstance(params, str) else json.dumps(params or {}))
            rows.append(row)
    _stream(client, MODEL_VERSION_TABLE, rows)
    return rows


# ── Work selection ───────────────────────────────────────────────────────────

def latest_batch_jobs(client, states=IN_FLIGHT_STATES, model_version: str = None) -> list:
    sql = f"""
        SELECT * FROM `{fq(BATCH_JOB_TABLE)}`
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY recorded_at DESC) = 1
          AND state IN UNNEST(@states)
          {"AND model_version = @mv" if model_version else ""}
    """
    params = {"states": list(states)}
    if model_version:
        params["mv"] = model_version
    return _query(client, sql, params)


def _existing(client, table_ids: list) -> list:
    from google.api_core.exceptions import NotFound

    out = []
    for t in table_ids:
        try:
            client.get_table(t)
            out.append(t)
        except NotFound:
            pass  # expired scratch table: those articles become pending again
    return out


def pending_docs(client, model_version: str, limit: int, lookback_days: int = None) -> list:
    """Current article versions this model has no usable answer for (an unusable
    answer is retried once) and that are not already in one of its unfinished
    batch jobs. Newest first."""
    in_flight = _existing(client, [j.input_table for j in
                                   latest_batch_jobs(client, model_version=model_version)
                                   if j.input_table])
    flight_cte = ("in_flight AS (" + " UNION DISTINCT ".join(
        f"SELECT doc_id, source_version FROM `{t}`" for t in in_flight) + "),") if in_flight else ""
    flight_join = ("LEFT JOIN in_flight AS f USING (doc_id, source_version)" if in_flight else "")
    flight_where = "AND f.doc_id IS NULL" if in_flight else ""
    window = ("AND d.published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)"
              if lookback_days else "")
    params = {"mv": model_version, "limit": int(limit)}
    if lookback_days:
        params["days"] = int(lookback_days)
    rows = _query(client, f"""
        WITH {flight_cte}
        answered AS (
          SELECT doc_id, source_version FROM `{fq(LABEL_TABLE)}`
          WHERE labeler_type = 'MODEL' AND labeler_id = @mv
          GROUP BY doc_id, source_version
          HAVING COUNTIF(tag_status != 'ERROR') > 0 OR COUNTIF(tag_status = 'ERROR') >= 2
        )
        SELECT d.doc_id, d.source_version, d.source_id, d.title, d.body_text,
               d.language_iso639_3, d.published_at
        FROM `{fq(DOCUMENT_VIEW)}` AS d
        LEFT JOIN answered AS a USING (doc_id, source_version)
        {flight_join}
        WHERE a.doc_id IS NULL
          {flight_where}
          {window}
        ORDER BY d.published_at DESC
        LIMIT @limit
    """, params)
    return [dict(r.items()) for r in rows]
