"""
Shared helpers for sentiment ingestion fetchers.

Feed-first fetching (dictionary: "prefer feeds over HTML scraping; store
canonical URL and feed GUID"), article-page scraping only for body text the
feed doesn't carry, language validation, and bronze row assembly.
"""

import hashlib
import json
import logging
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import feedparser
import requests
from bs4 import BeautifulSoup

from etl.sentiment_ingestion.config import (
    DEFAULT_TIMEOUT,
    MIN_BODY_CHARS,
    MIN_BODY_WORDS,
    PAYLOAD_SCHEMA_VERSION,
    USER_AGENT,
    SourceConfig,
)

log = logging.getLogger("sentiment_ingestion")

# langdetect is optional at import time so offline paths (tests, dry-run
# payload assembly) degrade gracefully if it's missing.
try:
    from langdetect import DetectorFactory, detect_langs

    DetectorFactory.seed = 0  # deterministic results
    _LANGDETECT_AVAILABLE = True
except ImportError:  # pragma: no cover
    _LANGDETECT_AVAILABLE = False

# ── Language code mapping (langdetect emits ISO 639-1; dictionary wants 639-3) ──

ISO639_1_TO_3 = {
    "sw": "swa", "ar": "ara", "fr": "fra", "en": "eng", "pt": "por",
    "am": "amh", "so": "som", "af": "afr", "mg": "mlg", "rw": "kin",
    "es": "spa", "it": "ita", "de": "deu", "nl": "nld", "tr": "tur",
}

# Tracking params stripped from canonical URLs (dictionary: "strip tracking params")
_TRACKING_PARAM_RE = re.compile(
    r"^(utm_|at_|ns_)|^(fbclid|gclid|msclkid|ocid|cmp|xtor|ref)$", re.IGNORECASE
)

# ── HTTP ──────────────────────────────────────────────────────────────────────

def make_session(accept_language: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    if accept_language:
        s.headers.update({"Accept-Language": accept_language})
    return s


# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_entry_date(entry) -> tuple:
    """
    Return (published_utc: datetime|None, raw: dict).

    feedparser's *_parsed struct_times are already UTC. Source-native strings
    are retained verbatim (dictionary: keep source timezone separately).
    Undateable entries return None — never a silent now().
    """
    raw = {
        "published_raw": entry.get("published") or None,
        "updated_raw": entry.get("updated") or None,
    }
    for attr in ("published_parsed", "updated_parsed"):
        parsed = entry.get(attr)
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc), raw
            except (TypeError, ValueError):
                pass
    for raw_str in (raw["published_raw"], raw["updated_raw"]):
        if not raw_str:
            continue
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(raw_str.strip(), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc), raw
            except ValueError:
                continue
    return None, raw


def strip_html(html: str) -> str:
    if not html:
        return ""
    try:
        return BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    except Exception:
        return html


def canonicalize_url(url: str) -> str:
    """Strip tracking query params and fragments; keep everything else."""
    if not url:
        return url
    try:
        parts = urlparse(url)
        kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                if not _TRACKING_PARAM_RE.match(k)]
        return urlunparse(parts._replace(query=urlencode(kept), fragment=""))
    except Exception:
        return url


def content_version(title: str, body: str) -> str:
    """sha256 over title+body — the bronze source_version / content hash."""
    payload = f"{title or ''}\n{body or ''}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def normalize_for_fingerprint(text: str) -> str:
    """Normalize presentation-only differences without rewriting words.

    NFKC unifies equivalent Unicode forms; combining marks, format controls,
    punctuation and repeated whitespace are ignored. Letters, digits and
    symbols remain, so changed facts still create a new fingerprint.
    """
    normalized = unicodedata.normalize("NFKC", text or "").casefold()
    chars = []
    for char in normalized:
        category = unicodedata.category(char)
        if category in {"Mn", "Me", "Cf"}:
            continue
        if category.startswith("P") or category.startswith("Z"):
            chars.append(" ")
        else:
            chars.append(char)
    return " ".join("".join(chars).split())


def normalized_content_version(title: str, body: str) -> str:
    """Hash lexical content after presentation-only normalization."""
    payload = normalize_for_fingerprint(f"{title or ''}\n{body or ''}")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def document_id(source_id: str, record_id: str, canonical_url: str) -> str:
    """Stable article identity, independent of article revisions.

    Prefer the canonical URL because some feeds rotate GUIDs for the same
    page. Fall back to the provider-native record ID when no URL exists.
    """
    identity = canonicalize_url(canonical_url) or (record_id or "").strip()
    payload = f"{source_id}\n{identity}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def feed_stamp(entry) -> str:
    """
    Feed-level change stamp for an entry: its updated (or published) string.

    Compared exactly against the stamp stored in bronze
    (payload.feed_entry.updated_raw/published_raw) to decide whether a known
    GUID needs its article page re-fetched — edit detection without a
    download. Empty stamps never match, so undated entries are always
    re-fetched and left to the content-hash dedup.
    """
    return entry.get("updated") or entry.get("published") or ""


FEED_PAGE_PARAMS = {"wordpress": "paged"}


def feed_page_url(feed_url: str, page: int, pagination: Optional[str]) -> str:
    """URL of the given 1-based feed page; page 1 is the feed URL itself."""
    if page <= 1 or not pagination:
        return feed_url
    if pagination not in FEED_PAGE_PARAMS:
        raise ValueError(f"Unknown feed_pagination {pagination!r}; "
                         f"expected one of {sorted(FEED_PAGE_PARAMS)}")
    param = FEED_PAGE_PARAMS[pagination]
    parts = urlparse(feed_url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if k != param]
    query.append((param, str(page)))
    return urlunparse(parts._replace(query=urlencode(query)))


def utc_str(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Language validation ──────────────────────────────────────────────

def detect_language(text: str) -> dict:
    """Return {'detected_iso639_1', 'detected_iso639_3', 'detection_confidence'}."""
    result = {"detected_iso639_1": None, "detected_iso639_3": None,
              "detection_confidence": None}
    if not _LANGDETECT_AVAILABLE or not text or len(text.strip()) < 20:
        return result
    try:
        best = detect_langs(text[:1000])[0]
        code1 = best.lang.split("-")[0]  # zh-cn style → zh
        result["detected_iso639_1"] = code1
        result["detected_iso639_3"] = ISO639_1_TO_3.get(code1)
        result["detection_confidence"] = round(float(best.prob), 4)
    except Exception:
        pass
    return result


# ── Article body scraping ─────────────────────────────────────────────────────

def scrape_body(session: requests.Session, url: str, selectors: list, *,
                allowed_domains: list, delay: float,
                exclude_selectors: Optional[list] = None,
                conditional_headers: Optional[dict] = None,
                timeout: int = DEFAULT_TIMEOUT,
                min_chars: int = MIN_BODY_CHARS) -> tuple:
    """
    Fetch an article page and extract body text via the first selector that
    yields >= min_chars. Returns (text, fetch_meta) — meta carries what the
    raw-manifest layer would normally record.
    """
    meta = {"http_status": None, "final_url": None, "error": None,
            "etag": None, "last_modified": None}
    if not url or (allowed_domains and not any(d in url for d in allowed_domains)):
        meta["error"] = "domain_not_allowed"
        return "", meta
    try:
        time.sleep(delay)  # politeness before every article fetch
        r = session.get(url, timeout=timeout, headers=conditional_headers or {})
        meta["http_status"] = r.status_code
        meta["final_url"] = r.url
        meta["etag"] = r.headers.get("ETag")
        meta["last_modified"] = r.headers.get("Last-Modified")
        if r.status_code == 304:
            meta["error"] = "not_modified"
            return "", meta
        if r.status_code != 200:
            meta["error"] = f"http_{r.status_code}"
            return "", meta
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup(["script", "style", "aside", "nav", "footer"]):
            tag.decompose()
        for selector in exclude_selectors or []:
            for node in soup.select(selector):
                node.decompose()
        for selector in selectors:
            paras = soup.select(selector)
            if paras:
                text = " ".join(p.get_text(" ", strip=True) for p in paras)
                if len(text) >= min_chars:
                    return text, meta
        meta["error"] = "no_selector_matched"
    except Exception as exc:
        meta["error"] = f"{type(exc).__name__}: {exc}"
    return "", meta


# ── Bronze row assembly ───────────────────────────────────────────────────────

def build_bronze_row(cfg: SourceConfig, *, record_id: str, title: str,
                     summary_html: str, body_text: str, body_source: Optional[str],
                     link: str, author: Optional[str], tags: list,
                     published_utc: Optional[datetime], raw_dates: dict,
                     fetch_meta: dict, feed_url: str,
                     fetched_at: datetime,
                     raw_title: Optional[str] = None) -> dict:
    """Assemble one bronze.source_record row (payload as a dict)."""
    language = {
        "declared_iso639_3": cfg.language_iso639_3,
        "script_iso15924": cfg.script_iso15924,
        **detect_language(f"{title} {body_text[:1000]}"),
    }
    canonical_url = canonicalize_url(link) or None
    exact_hash = content_version(title, body_text)
    normalized_hash = normalized_content_version(title, body_text)
    stable_doc_id = document_id(cfg.source_id, record_id, canonical_url or "")
    payload = {
        "schema_version": PAYLOAD_SCHEMA_VERSION,
        "source": {
            "source_id": cfg.source_id,
            "name": cfg.name,
            "source_class": cfg.source_class,
            "feed_url": feed_url,
        },
        "feed_entry": {
            "guid": record_id,
            "link": link or None,
            "title": (raw_title if raw_title is not None else title) or None,
            "summary_html": summary_html or None,
            "author": author or None,
            "tags": tags,
            "published_raw": raw_dates.get("published_raw"),
            "updated_raw": raw_dates.get("updated_raw"),
        },
        "article": {
            "document_id": stable_doc_id,
            "document_type": "text_article",
            "canonical_url": canonical_url,
            "final_url": fetch_meta.get("final_url"),
            "title": title or None,
            "body_text": body_text or None,
            "body_source": body_source,
            "char_count": len(body_text or ""),
            "word_count": len((body_text or "").split()),
        },
        "dates": {
            "published_at_utc": utc_str(published_utc),
            "fetched_at_utc": utc_str(fetched_at),
        },
        "fetch": {
            "http_status": fetch_meta.get("http_status"),
            "error": fetch_meta.get("error"),
            "etag": fetch_meta.get("etag"),
            "last_modified": fetch_meta.get("last_modified"),
            "content_sha256": exact_hash,
            "normalized_content_sha256": normalized_hash,
            "user_agent": USER_AGENT,
        },
        "language": language,
    }
    return {
        "source_id": cfg.source_id,
        "source_record_id": record_id,
        "source_version": exact_hash,
        # Subject-country assignment is intentionally deferred to a later
        # multi-country tagging agent. Publisher origin is not a sentiment
        # target and is not substituted here.
        "country_iso3": None,
        "object_uri": None,  # raw/object-storage layer not in scope (bronze-only)
        "payload": payload,
        "ingested_at": utc_str(fetched_at),
    }
