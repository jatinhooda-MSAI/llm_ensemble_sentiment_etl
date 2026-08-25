"""
Offline unit tests for the sentiment ingestion (bronze) helpers.

No network, no BigQuery — pure functions in etl/sentiment_ingestion/common.py
plus config loading and the writer's dedup filter.
"""

import re
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from etl.sentiment_ingestion import common
from etl.sentiment_ingestion.bq_writer import _struct_field, filter_new
from etl.sentiment_ingestion.common import (
    build_bronze_row,
    canonicalize_url,
    content_version,
    document_id,
    feed_page_url,
    feed_stamp,
    fetch_from_feeds,
    normalized_content_version,
    parse_entry_date,
)
from etl.sentiment_ingestion.config import SourceConfig, load_sources
from etl.sentiment_ingestion.fetchers.hespress import _strip_read_also

SOURCE_ID_RE = re.compile(r"^SRC_[A-Z0-9_]+$")


# ── parse_entry_date ──────────────────────────────────────────────────────────

class TestParseEntryDate:
    def test_struct_time_is_utc(self):
        entry = {"published_parsed": time.struct_time((2026, 8, 19, 4, 25, 13, 2, 231, 0)),
                 "published": "Wed, 19 Aug 2026 04:25:13 GMT"}
        dt, raw = parse_entry_date(entry)
        assert dt == datetime(2026, 8, 19, 4, 25, 13, tzinfo=timezone.utc)
        assert raw["published_raw"] == "Wed, 19 Aug 2026 04:25:13 GMT"

    def test_rfc822_string_fallback(self):
        entry = {"published": "Tue, 18 Aug 2026 20:13:48 GMT"}
        dt, raw = parse_entry_date(entry)
        assert dt is not None
        assert (dt.year, dt.month, dt.day, dt.hour) == (2026, 8, 18, 20)
        assert dt.tzinfo is not None

    def test_iso_string_fallback(self):
        entry = {"updated": "2026-08-19T04:25:13+00:00"}
        dt, _ = parse_entry_date(entry)
        assert dt == datetime(2026, 8, 19, 4, 25, 13, tzinfo=timezone.utc)

    def test_garbage_returns_none_and_keeps_raw(self):
        entry = {"published": "yesterday-ish"}
        dt, raw = parse_entry_date(entry)
        assert dt is None
        assert raw["published_raw"] == "yesterday-ish"  # source-native string retained


# ── content_version / canonicalize_url ────────────────────────────────────────

class TestHashingAndUrls:
    def test_content_version_is_stable_64_hex(self):
        v1 = content_version("Titre", "Corps de l'article")
        v2 = content_version("Titre", "Corps de l'article")
        assert v1 == v2
        assert re.fullmatch(r"[0-9a-f]{64}", v1)

    def test_content_version_changes_with_body(self):
        assert content_version("T", "a") != content_version("T", "b")

    def test_normalized_version_ignores_presentation_only_changes(self):
        left = normalized_content_version("Titre", "Croissance—forte…  5 %")
        right = normalized_content_version("titre", "Croissance forte... 5 %")
        assert left == right

    def test_normalized_version_keeps_word_changes(self):
        assert normalized_content_version("T", "growth rose 5%") != \
            normalized_content_version("T", "growth fell 5%")

    def test_tracking_params_stripped(self):
        url = ("https://www.bbc.com/swahili/articles/x1?utm_source=rss"
               "&at_medium=RSS&ocid=wsswahili&page=2#section")
        assert canonicalize_url(url) == "https://www.bbc.com/swahili/articles/x1?page=2"

    def test_document_id_uses_canonical_url_not_rotating_guid(self):
        url = "https://example.com/a?utm_source=rss"
        assert document_id("SRC_X", "guid-v1", url) == \
            document_id("SRC_X", "guid-v2", "https://example.com/a")
