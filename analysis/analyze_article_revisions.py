"""Download and analyze 30 days of revised multilingual news records.

Read-only against BigQuery. Writes local evidence under analysis/revision_audit/.
"""

from __future__ import annotations

import difflib
import json
from collections import Counter, defaultdict
from pathlib import Path

from google.cloud import bigquery


PROJECT = "your-gcp-project"
OUT = Path(__file__).parent / "revision_audit"
SOURCES = (
    "SRC_BBC_SWAHILI",
    "SRC_BBC_ARABIC",
    "SRC_AFRICANEWS_FR",
)


QUERY = """
WITH candidates AS (
  SELECT source_id, source_record_id
  FROM `your-gcp-project.bronze.source_record`
  WHERE source_id IN UNNEST(@sources)
    AND ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY source_id, source_record_id
  HAVING COUNT(DISTINCT source_version) > 1
)
SELECT
  r.source_id,
  r.source_record_id,
  r.source_version,
  r.ingested_at,
  JSON_VALUE(r.payload, '$.article.title') AS title,
  JSON_VALUE(r.payload, '$.article.canonical_url') AS canonical_url,
  JSON_VALUE(r.payload, '$.article.body_text') AS body_text,
  SAFE_CAST(JSON_VALUE(r.payload, '$.article.word_count') AS INT64) AS word_count,
  JSON_VALUE(r.payload, '$.feed_entry.published_raw') AS published_raw,
  JSON_VALUE(r.payload, '$.feed_entry.updated_raw') AS updated_raw
FROM `your-gcp-project.bronze.source_record` r
JOIN candidates USING (source_id, source_record_id)
WHERE r.ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY source_id, source_record_id, ingested_at
"""


def similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(
        None, (left or "").split(), (right or "").split(), autojunk=True
    ).ratio()


def change_kind(previous: dict, current: dict) -> str:
    old = previous.get("body_text") or ""
    new = current.get("body_text") or ""
    ratio = similarity(old, new)
    if old and new.startswith(old):
        return "append_only"
    if new and old.startswith(new):
        return "truncation"
    if ratio >= 0.995:
        return "tiny_edit"
    if ratio >= 0.95:
        return "minor_edit"
    if ratio >= 0.75:
        return "material_edit"
    return "major_rewrite"
