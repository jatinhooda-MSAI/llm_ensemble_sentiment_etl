"""
Sentiment ingestion (bronze layer).

Fetches full news articles from multilingual African sources (Arabic, French,
Swahili) and writes them to BigQuery `bronze.source_record`, following
an existing data dictionary. Bronze only: one row per
source-native record, append-only, full payload preserved as JSON.

Entry point: run_sentiment_ingestion.py at the repo root.
"""
