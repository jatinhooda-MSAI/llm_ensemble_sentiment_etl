#!/usr/bin/env python3
"""
Sentiment scoring runner: country tagging + per-country sentiment from the
models in config/sentiment_models.yaml, via Vertex AI.

Usage:
    python run_sentiment_scoring.py register
    python run_sentiment_scoring.py submit   [--limit N] [--all-history]
    python run_sentiment_scoring.py serial   [--limit N] [--all-history]
    python run_sentiment_scoring.py collect
    python run_sentiment_scoring.py aggregate
    python run_sentiment_scoring.py bias
    python run_sentiment_scoring.py scheduled              # twice-daily: register, submit, serial, aggregate, bias
    python run_sentiment_scoring.py collect-and-aggregate  # hourly: collect finished batches, aggregate
    python run_sentiment_scoring.py claude-history [--limit N] [--wait] [--collect-only]
        # one-off: Claude on the backlog via Anthropic's Message Batches API (needs an
        # Anthropic API key); labels stay out of the consensus until Claude is on Vertex
    python run_sentiment_scoring.py agreement              # compare models -> model_evaluation_slice

Scheduled runs only score articles published in the last run_limits.lookback_days;
--all-history scores everything pending (backlog).

Env: BQ_PROJECT (required). Auth: application-default credentials, or
GCLOUD_TOKEN for local runs.
"""

import argparse
import logging
import sys

from etl.sentiment_scoring import agreement, anthropic_history, jobs
from etl.sentiment_scoring import store
from etl.sentiment_scoring.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
for noisy in ("google", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=["register", "submit", "serial", "collect", "aggregate",
                                            "bias", "scheduled", "collect-and-aggregate",
                                            "claude-history", "agreement"])
    parser.add_argument("--limit", type=int, default=None, help="Max articles per model this run")
    parser.add_argument("--all-history", action="store_true",
                        help="Score every pending article, not only recent ones")
    parser.add_argument("--window-days", type=int, default=None,
                        help="aggregate/bias: rebuild this many trailing days (default from config)")
    parser.add_argument("--wait", action="store_true",
                        help="claude-history: wait for the batches to end, then collect")
    parser.add_argument("--collect-only", action="store_true",
                        help="claude-history: only collect batches submitted earlier")
    args = parser.parse_args()

    cfg = load_config()
    ctx = jobs.Context(cfg, store.get_client())
    lookback = None if args.all_history else cfg.lookback_days

    if args.command in ("register", "scheduled"):
        jobs.register(ctx)
    if args.command in ("submit", "scheduled"):
        jobs.submit(ctx, limit=args.limit, lookback_days=lookback)
    if args.command in ("serial", "scheduled"):
        jobs.serial(ctx, limit=args.limit, lookback_days=lookback)
    # Only one job collects (the hourly one), so two runs never process the
    # same finished batch at once.
    if args.command in ("collect", "collect-and-aggregate"):
        jobs.collect(ctx)
    if args.command in ("aggregate", "scheduled", "collect-and-aggregate"):
        jobs.aggregate(ctx, window_days=args.window_days)
    if args.command in ("bias", "scheduled"):
        jobs.bias(ctx, window_days=args.window_days)
    if args.command == "claude-history":
        if not args.collect_only:
            jobs._logged("CLAUDE_HISTORY_SUBMIT", anthropic_history.BILLING_TAG)(
                anthropic_history.submit)(ctx, limit=args.limit)
        if args.wait:
            anthropic_history.wait_until_ended(ctx)
        if args.wait or args.collect_only:
            jobs._logged("CLAUDE_HISTORY_COLLECT", anthropic_history.BILLING_TAG)(
                anthropic_history.collect)(ctx)
    if args.command == "agreement":
        jobs._logged("MODEL_AGREEMENT")(agreement.run)(ctx)
    return 0


if __name__ == "__main__":
    sys.exit(main())
