"""
Configuration for sentiment ingestion.

Source definitions live in config/sentiment_sources.yaml so the schema owner
can adjust source_ids, feeds, or selectors without code changes. The BigQuery
target is env-overridable.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCES_YAML = REPO_ROOT / "config" / "sentiment_sources.yaml"

# BigQuery target — set via env. Deliberately no default project: the
# bronze dataset lives in a specific GCP project (auth via the
# pipeline's service account), and silently writing to a guessed project
# would be worse than failing.
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_BRONZE_DATASET = os.getenv("BQ_BRONZE_DATASET", "bronze")
BQ_BRONZE_TABLE = os.getenv("BQ_BRONZE_TABLE", "source_record")


def bronze_fqtn() -> str:
    """Fully-qualified bronze table name; live loads require BQ_PROJECT."""
    if not BQ_PROJECT:
        raise RuntimeError(
            "BQ_PROJECT is not set. Export BQ_PROJECT=<GCP project id> "
            "and GOOGLE_APPLICATION_CREDENTIALS=<pipeline service account "
            "key> before a live load (dry-run mode needs neither)."
        )
    return f"{BQ_PROJECT}.{BQ_BRONZE_DATASET}.{BQ_BRONZE_TABLE}"

PAYLOAD_SCHEMA_VERSION = "1.1.0"
USER_AGENT = (
    "Mozilla/5.0 (compatible; news-sentiment-ingestion/1.0)"
)
DEFAULT_TIMEOUT = 15  # seconds per HTTP request
MIN_BODY_CHARS = 100  # below this a scraped body is considered a failure
MIN_BODY_WORDS = 80   # short blurbs/teasers are not accepted as articles
