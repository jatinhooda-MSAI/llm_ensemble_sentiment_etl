"""BBC Arabic (ara/Arab) — RSS discovery + BBC article-page scrape."""

from etl.sentiment_ingestion.common import fetch_from_feeds


def fetch(cfg, days_back: int, max_articles: int, known_records=None):
    return fetch_from_feeds(cfg, days_back, max_articles,
                            known_records=known_records)
