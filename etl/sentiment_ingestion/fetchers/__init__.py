"""Fetcher registry: config key → fetch(cfg, days_back, max_articles)."""

from etl.sentiment_ingestion.fetchers import (
    africanews_fr,
    allafrica_fr,
    bbc_arabic,
    bbc_swahili,
    hespress,
)

FETCHERS = {
    "bbc_swahili": bbc_swahili.fetch,
    "bbc_arabic": bbc_arabic.fetch,
    "hespress": hespress.fetch,
    "allafrica_fr": allafrica_fr.fetch,
    "africanews_fr": africanews_fr.fetch,
}
