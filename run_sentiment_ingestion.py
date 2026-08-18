#!/usr/bin/env python3
"""
Sentiment ingestion runner (bronze layer).

Fetches full articles from the multilingual news sources configured in
config/sentiment_sources.yaml and appends them to BigQuery
`bronze.source_record` (contract from an existing data dictionary).

Usage:
    python run_sentiment_ingestion.py --sources all --days 7 --max 100
    python run_sentiment_ingestion.py --sources bbc_swahili,hespress --days 2 --max 5
    python run_sentiment_ingestion.py --sources all --days 3 --max 5 --dry-run /tmp/bronze_dryrun
    python run_sentiment_ingestion.py --sources hespress --days 56 --max 6000 --backfill-pages 450

--dry-run writes one NDJSON file per source (exact bronze row shape) and
never touches BigQuery — no credentials needed.

--backfill-pages walks paginated feeds (feed_pagination in the YAML) past
already-ingested entries, up to N pages or the --days cutoff. Known entries
are still skipped and duplicate filtering still applies.

BigQuery target env overrides: BQ_PROJECT, BQ_BRONZE_DATASET, BQ_BRONZE_TABLE.
"""

import argparse
import dataclasses
import json
import logging
import sys
from pathlib import Path

from etl.sentiment_ingestion.config import bronze_fqtn, load_sources
from etl.sentiment_ingestion.fetchers import FETCHERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sentiment_ingestion")

# Extra days added to --days for the BQ duplicate-lookback window, covering
# clock skew and late re-runs.
LOOKBACK_MARGIN_DAYS = 3


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch multilingual news into bronze.source_record"
    )
    parser.add_argument("--sources", default="all",
                        help="'all' or comma-separated keys from "
                             "config/sentiment_sources.yaml (default: all)")
    parser.add_argument("--days", type=int, default=7,
                        help="How many days back to fetch (default: 7)")
    parser.add_argument("--max", type=int, default=100,
                        help="Max articles per source (default: 100)")
    parser.add_argument("--dry-run", metavar="DIR", default=None,
                        help="Write NDJSON to DIR instead of loading BigQuery")
    parser.add_argument(
        "--force-refresh", action="store_true",
        help="Bypass feed-stamp/HTTP prefetch skips and re-extract full text; "
             "content fingerprints still prevent duplicate inserts",
    )
    parser.add_argument(
        "--backfill-pages", type=int, metavar="N", default=None,
        help="For paginated feeds: read up to N pages without stopping at "
             "already-ingested entries (paging still stops at the --days "
             "cutoff or the end of the feed)",
    )
    args = parser.parse_args()
    if args.backfill_pages is not None and args.backfill_pages < 1:
        parser.error("--backfill-pages must be at least 1")

    sources_cfg = load_sources()
    if args.sources.strip() == "all":
        keys = list(FETCHERS)
    else:
        keys = [k.strip() for k in args.sources.split(",") if k.strip()]
    unknown = [k for k in keys if k not in FETCHERS]
    if unknown:
        parser.error(f"Unknown source(s) {unknown}. Valid: {sorted(FETCHERS)}")

    # Live mode: connect to BQ up front (fail fast on auth/project problems)
    # and pull the feed-stamp map so unchanged known articles are skipped
    # before any page download.
    client = None
    known_records = None
    if not args.dry_run:
        from etl.sentiment_ingestion import bq_writer

        client = bq_writer.get_client()
        all_source_ids = [sources_cfg[k].source_id for k in keys]
        if not args.force_refresh:
            known_records = bq_writer.known_record_metadata(
                client, all_source_ids, args.days + LOOKBACK_MARGIN_DAYS
            )
            log.info("Prefetch skip map: %d known feed entries", len(known_records))
        else:
            log.info("Force refresh enabled: bypassing prefetch skip map")

    # Fetch phase — one broken source must not kill the run.
    rows_by_source = {}
    summary = {}
    for key in keys:
        cfg = sources_cfg[key]
        if args.backfill_pages and cfg.feed_pagination:
            cfg = dataclasses.replace(
                cfg, max_feed_pages=args.backfill_pages,
                stop_paging_on_known=False,
            )
        try:
            rows, stats = FETCHERS[key](cfg, args.days, args.max,
                                        known_records=known_records)
            rows_by_source[key] = rows
            summary[key] = {"status": "ok", **stats}
        except Exception as exc:
            log.exception("[%s] fetch failed", key)
            summary[key] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    # Write phase
    if args.dry_run:
        outdir = Path(args.dry_run)
        outdir.mkdir(parents=True, exist_ok=True)
        for key, rows in rows_by_source.items():
            out = outdir / f"{key}.ndjson"
            with out.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            summary[key]["ndjson"] = str(out)
            log.info("[%s] dry-run: %d rows -> %s", key, len(rows), out)
    elif rows_by_source:
        from etl.sentiment_ingestion import bq_writer

        source_ids = [sources_cfg[k].source_id for k in rows_by_source]
        existing = bq_writer.existing_keys(
            client, source_ids, args.days + LOOKBACK_MARGIN_DAYS
        )
        existing_normalized = bq_writer.existing_normalized_keys(
            client, source_ids, args.days + LOOKBACK_MARGIN_DAYS
        )
        log.info("Duplicate lookback: %d existing (id, record, version) keys",
                 len(existing))
        for key, rows in rows_by_source.items():
            new_rows = bq_writer.filter_new(
                rows, existing, existing_normalized
            )
            if new_rows:
                bq_writer.write_rows(client, new_rows)
            summary[key].update(
                new=len(new_rows), skipped_duplicate=len(rows) - len(new_rows)
            )
            log.info("[%s] loaded %d new rows (%d duplicates skipped) -> %s",
                     key, len(new_rows), len(rows) - len(new_rows),
                     bronze_fqtn())

    # End-of-run summary — the data a audit.etl_run row would carry.
    log.info("Run summary: %s", json.dumps(summary, ensure_ascii=False))
    any_ok = any(s.get("status") == "ok" for s in summary.values())
    return 0 if any_ok else 1


if __name__ == "__main__":
    sys.exit(main())
