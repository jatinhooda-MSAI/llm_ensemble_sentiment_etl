"""Hespress (ara/Arab) — WordPress feed carries full article text
in content:encoded, so the page scrape is fallback only."""

import re

from etl.sentiment_ingestion.common import fetch_from_feeds

# "اقرأ أيضا" (read-also) cross-link blocks sometimes ride along inside
# content:encoded; drop the phrase and its trailing link text up to the next
# sentence boundary.
_READ_ALSO_RE = re.compile(r"اقرأ أيضا[^.؟!]*[.؟!]?")


_WORDPRESS_FOOTER_RE = re.compile(
    r"\s*The post\s+.*?\s+appeared first on\s+Hespress"
    r"(?:\s*-\s*هسبريس جريدة إلكترونية مغربية)?\s*\.?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _strip_read_also(text: str) -> str:
    without_related = _READ_ALSO_RE.sub(" ", text)
    return _WORDPRESS_FOOTER_RE.sub("", without_related).strip()


def fetch(cfg, days_back: int, max_articles: int, known_records=None):
    return fetch_from_feeds(
        cfg, days_back, max_articles, body_postprocess=_strip_read_also,
        known_records=known_records,
    )
