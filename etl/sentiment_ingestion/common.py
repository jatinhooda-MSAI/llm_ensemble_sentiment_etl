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
