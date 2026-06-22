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
