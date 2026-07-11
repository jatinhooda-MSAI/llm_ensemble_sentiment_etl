"""
BigQuery writer for bronze.source_record.

The table exists and is owned by the schema owner: no table creation, no
schema changes here — a column mismatch must fail loudly with the BQ error.
BigQuery enforces no primary keys, so idempotency is a lookback query on
(source_id, source_record_id, source_version) plus normalized-content
fingerprints and client-side filtering.
"""

import json
import logging

from etl.sentiment_ingestion.config import BQ_PROJECT, bronze_fqtn

log = logging.getLogger("sentiment_ingestion")


def _struct_field(value, field):
    """Read a BigQuery STRUCT returned as either dict or Row."""
    if isinstance(value, dict):
        return value.get(field)
    return getattr(value, field, None)


def get_client():
    from google.cloud import bigquery

    bronze_fqtn()  # fail early with a clear message if BQ_PROJECT is unset
    return bigquery.Client(project=BQ_PROJECT)


def existing_keys(client, source_ids: list, lookback_days: int) -> set:
    """(source_id, source_record_id, source_version) tuples already in bronze."""
    from google.cloud import bigquery

    query = f"""
        SELECT DISTINCT source_id, source_record_id, source_version
        FROM `{bronze_fqtn()}`
        WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback DAY)
          AND source_id IN UNNEST(@source_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lookback", "INT64", lookback_days),
            bigquery.ArrayQueryParameter("source_ids", "STRING", source_ids),
        ]
    )
    result = client.query(query, job_config=job_config).result()
    return {(r.source_id, r.source_record_id, r.source_version) for r in result}


def known_record_metadata(client, source_ids: list, lookback_days: int) -> dict:
    """
    Latest feed stamp and HTTP validators per source-native record.

    Lets the fetch engine skip the article-page download for GUIDs whose feed
    entry hasn't changed since we last ingested them. Rows loaded through the
    string-payload fallback yield '' here, which never matches — those GUIDs
    just get re-fetched and content-hash dedup handles them.
    """
    from google.cloud import bigquery

    query = f"""
        SELECT source_id, source_record_id,
               ARRAY_AGG(STRUCT(
                   COALESCE(JSON_VALUE(payload, '$.feed_entry.updated_raw'),
                            JSON_VALUE(payload, '$.feed_entry.published_raw'),
                            '') AS feed_stamp,
                   JSON_VALUE(payload, '$.fetch.etag') AS etag,
                   JSON_VALUE(payload, '$.fetch.last_modified') AS last_modified
               ) ORDER BY ingested_at DESC LIMIT 1)[OFFSET(0)] AS latest
        FROM `{bronze_fqtn()}`
        WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback DAY)
          AND source_id IN UNNEST(@source_ids)
        GROUP BY source_id, source_record_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lookback", "INT64", lookback_days),
            bigquery.ArrayQueryParameter("source_ids", "STRING", source_ids),
        ]
    )
    result = client.query(query, job_config=job_config).result()
    return {
        (r.source_id, r.source_record_id): {
            "feed_stamp": _struct_field(r.latest, "feed_stamp"),
            "etag": _struct_field(r.latest, "etag"),
            "last_modified": _struct_field(r.latest, "last_modified"),
        }
        for r in result
    }
