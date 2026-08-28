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


# ── config + bronze row shape ─────────────────────────────────────────────────

class TestConfigAndRowShape:
    def test_only_full_text_sources_configured(self):
        sources = load_sources()
        assert set(sources) == {
            "bbc_swahili", "bbc_arabic", "hespress",
            "allafrica_fr", "africanews_fr",
        }
        for cfg in sources.values():
            assert SOURCE_ID_RE.fullmatch(cfg.source_id), cfg.source_id
            assert cfg.language_iso639_3 in {"swa", "ara", "fra"}
            assert cfg.script_iso15924 in {"Latn", "Arab"}
            assert cfg.feeds

    def test_build_bronze_row_shape(self):
        cfg = load_sources()["hespress"]
        fetched = datetime(2026, 8, 19, 10, 0, 0, tzinfo=timezone.utc)
        row = build_bronze_row(
            cfg,
            record_id="https://www.hespress.com/example-123.html",
            title="عنوان تجريبي",
            summary_html="<p>ملخص</p>",
            body_text="نص المقال الكامل هنا " * 10,
            body_source="feed_content_encoded",
            link="https://www.hespress.com/example-123.html?utm_source=feed",
            author="هسبريس",
            tags=["اقتصاد"],
            published_utc=datetime(2026, 8, 19, 7, 0, 10, tzinfo=timezone.utc),
            raw_dates={"published_raw": "Wed, 19 Aug 2026 07:00:10 +0100",
                       "updated_raw": None},
            fetch_meta={"http_status": None, "final_url": None, "error": None},
            feed_url="https://www.hespress.com/feed",
            fetched_at=fetched,
        )
        # exactly the live bronze.source_record columns
        assert set(row) == {"source_id", "source_record_id", "source_version",
                            "country_iso3", "object_uri", "payload", "ingested_at"}
        assert row["source_id"] == "SRC_HESPRESS"
        assert row["country_iso3"] is None
        assert row["object_uri"] is None
        assert row["ingested_at"] == "2026-08-19T10:00:00Z"
        assert re.fullmatch(r"[0-9a-f]{64}", row["source_version"])

        payload = row["payload"]
        assert set(payload) == {"schema_version", "source", "feed_entry",
                                "article", "dates", "fetch", "language"}
        assert payload["fetch"]["content_sha256"] == row["source_version"]
        assert re.fullmatch(r"[0-9a-f]{64}", payload["article"]["document_id"])
        assert re.fullmatch(
            r"[0-9a-f]{64}", payload["fetch"]["normalized_content_sha256"]
        )
        # tracking param stripped from the canonical URL
        assert payload["article"]["canonical_url"] == \
            "https://www.hespress.com/example-123.html"
        assert payload["language"]["declared_iso639_3"] == "ara"
        assert payload["dates"]["published_at_utc"] == "2026-08-19T07:00:10Z"
        assert payload["feed_entry"]["published_raw"].endswith("+0100")

    def test_non_text_filters_are_configured(self):
        sources = load_sources()
        assert "/bbc_swahili_radio/" in \
            sources["bbc_swahili"].excluded_url_patterns
        assert {"صوت وصورة", "كاريكاتير اليوم"} <= \
            set(sources["hespress"].excluded_tags)

    def test_only_hespress_feed_is_paginated(self):
        sources = load_sources()
        assert sources["hespress"].feed_pagination == "wordpress"
        assert sources["hespress"].max_feed_pages == 10
        for key, cfg in sources.items():
            if key != "hespress":
                assert cfg.feed_pagination is None, key


class TestHespressCleanup:
    def test_wordpress_footer_removed(self):
        text = (
            "نص المقال المفيد هنا. "
            "The post عنوان المقال appeared first on Hespress - "
            "هسبريس جريدة إلكترونية مغربية ."
        )
        assert _strip_read_also(text) == "نص المقال المفيد هنا."


# ── feed_stamp (pre-fetch skip key) ───────────────────────────────────────────

class TestFeedStamp:
    def test_prefers_updated_over_published(self):
        assert feed_stamp({"updated": "U", "published": "P"}) == "U"

    def test_falls_back_to_published(self):
        assert feed_stamp({"published": "P"}) == "P"

    def test_undated_entry_is_empty_and_never_matches(self):
        # empty stamps must not be used to skip — engine requires truthy match
        assert feed_stamp({}) == ""


# ── feed paging (fetch_from_feeds with a fake HTTP session) ───────────────────

FEED = "https://example.com/feed"
_BODY = "كلمة " * 120  # comfortably above MIN_BODY_WORDS / MIN_BODY_CHARS


def _rss(items):
    """Minimal WordPress-style RSS page; items are (guid, published) pairs."""
    parts = [
        f"<item><title>T {guid}</title><link>{guid}</link><guid>{guid}</guid>"
        f"<pubDate>{format_datetime(published)}</pubDate>"
        f"<content:encoded><![CDATA[<p>{_BODY}</p>]]></content:encoded></item>"
        for guid, published in items
    ]
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">'
        f"<channel><title>t</title>{''.join(parts)}</channel></rss>"
    ).encode("utf-8")


def _page_items(page, n=3, age=timedelta(hours=1)):
    when = (datetime.now(timezone.utc) - age).replace(microsecond=0)
    return [(f"https://example.com/p{page}-{i}", when) for i in range(n)]


class _FakeResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """Serves canned feed pages keyed by URL; any other URL is a 404."""

    def __init__(self, pages):
        self.pages = pages
        self.requested = []

    def get(self, url, timeout=None, headers=None):
        self.requested.append(url)
        if url in self.pages:
            return _FakeResponse(200, self.pages[url])
        return _FakeResponse(404)


def _paged_cfg(**overrides):
    fields = dict(
        key="wp", source_id="SRC_TEST_WP", name="Test WP", source_class="news_rss",
        feeds=[FEED], language_iso639_3="ara", script_iso15924="Arab",
        prefer_feed_content=True, article_delay=0,
        feed_pagination="wordpress", max_feed_pages=5,
    )
    fields.update(overrides)
    return SourceConfig(**fields)


def _known(guid, published):
    return {("SRC_TEST_WP", guid): {"feed_stamp": format_datetime(published)}}


class TestFeedPaging:
    def _run(self, monkeypatch, pages, cfg, known=None):
        session = _FakeSession(pages)
        monkeypatch.setattr(common, "make_session", lambda *_: session)
        rows, stats = fetch_from_feeds(cfg, 2, 1000, known_records=known)
        return session, rows, stats

    def test_page_urls(self):
        assert feed_page_url(FEED, 1, "wordpress") == FEED
        assert feed_page_url(FEED, 3, "wordpress") == FEED + "?paged=3"
        assert feed_page_url(FEED + "?lang=ar&paged=2", 4, "wordpress") == \
            FEED + "?lang=ar&paged=4"
        assert feed_page_url(FEED, 3, None) == FEED

    def test_unpaginated_feed_reads_one_page(self, monkeypatch):
        pages = {FEED: _rss(_page_items(1)),
                 FEED + "?paged=2": _rss(_page_items(2))}
        session, rows, stats = self._run(
            monkeypatch, pages, _paged_cfg(feed_pagination=None))
        assert session.requested == [FEED]
        assert len(rows) == 3
        assert "feed_page_stop" not in stats

    def test_stops_after_page_with_known_entry(self, monkeypatch):
        p2 = _page_items(2)
        pages = {FEED: _rss(_page_items(1)), FEED + "?paged=2": _rss(p2),
                 FEED + "?paged=3": _rss(_page_items(3))}
        session, rows, stats = self._run(
            monkeypatch, pages, _paged_cfg(), known=_known(*p2[1]))
        assert session.requested == [FEED, FEED + "?paged=2"]
        assert stats["feed_page_stop"] == "caught_up"
        assert stats["skipped_known"] == 1
        assert len(rows) == 5  # 3 from page 1, 2 new from page 2

    def test_backfill_pages_past_known_until_cutoff(self, monkeypatch):
        p1 = _page_items(1)
        pages = {FEED: _rss(p1), FEED + "?paged=2": _rss(_page_items(2)),
                 FEED + "?paged=3": _rss(_page_items(3, age=timedelta(days=10))),
                 FEED + "?paged=4": _rss(_page_items(4, age=timedelta(days=11)))}
        session, rows, stats = self._run(
            monkeypatch, pages, _paged_cfg(stop_paging_on_known=False),
            known=_known(*p1[0]))
        assert session.requested == [FEED, FEED + "?paged=2", FEED + "?paged=3"]
        assert stats["feed_page_stop"] == "cutoff"
        assert stats["skipped_known"] == 1  # known entries are still skipped
        assert stats["too_old"] == 3
        assert len(rows) == 5

    def test_page_cap_is_flagged(self, monkeypatch):
        pages = {FEED: _rss(_page_items(1)), FEED + "?paged=2": _rss(_page_items(2)),
                 FEED + "?paged=3": _rss(_page_items(3))}
        session, rows, stats = self._run(
            monkeypatch, pages, _paged_cfg(max_feed_pages=2), known={})
        assert len(session.requested) == 2
        assert stats["feed_pages"] == 2
        assert stats["feed_page_stop"] == "page_cap"

    def test_404_past_last_page_ends_cleanly(self, monkeypatch):
        pages = {FEED: _rss(_page_items(1))}
        session, rows, stats = self._run(monkeypatch, pages, _paged_cfg(), known={})
        assert session.requested == [FEED, FEED + "?paged=2"]
        assert stats["feed_page_stop"] == "end_of_feed"
        assert stats["feed_errors"] == 0
        assert len(rows) == 3

    def test_page_of_repeats_does_not_stop_as_cutoff(self, monkeypatch):
        p1 = _page_items(1)
        pages = {FEED: _rss(p1), FEED + "?paged=2": _rss(p1),
                 FEED + "?paged=3": _rss(_page_items(3))}
        session, rows, stats = self._run(
            monkeypatch, pages, _paged_cfg(max_feed_pages=3), known={})
        assert len(session.requested) == 3
        assert len(rows) == 6
