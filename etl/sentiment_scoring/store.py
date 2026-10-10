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
