"""AllAfrica French (fra/Latn) — RDF headline feed + article-page scrape.

Descriptions in the RDF are sparse, so the page scrape is mandatory for body
text. Country tagging is intentionally deferred to a multi-country tagger.

Rights note: AllAfrica aggregates member-newspaper content — production-scale
scraping needs the schema owner's rights_status sign-off in dim_source.
"""

from etl.sentiment_ingestion.common import fetch_from_feeds


def fetch(cfg, days_back: int, max_articles: int, known_records=None):
    return fetch_from_feeds(
        cfg, days_back, max_articles, known_records=known_records,
    )
