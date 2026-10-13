"""
Scoring runs.

  register   make the configured models + prompt the ACTIVE versions
  submit     BATCH models: pending articles -> scratch table -> Vertex batch job
  collect    finished batch jobs -> label rows
  serial     SERIAL models: one request per pending article -> label rows
  aggregate  rebuild the daily country series (sql/04)
  bias       KL check across languages (sql/05)

Every run is logged in audit.etl_run.
"""

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from etl.sentiment_scoring import prompt as P
from etl.sentiment_scoring import providers as V
from etl.sentiment_scoring import store as S
from etl.sentiment_scoring.config import BQ_PROJECT, ScoringConfig

log = logging.getLogger("sentiment_scoring")
SQL_DIR = Path(__file__).parent / "sql"
MODEL_PATHS = {"GOOGLE": "publishers/google/models/{}", "ANTHROPIC": "publishers/anthropic/models/{}"}
DONE_STATES = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED",
               "JOB_STATE_EXPIRED", "JOB_STATE_PARTIALLY_SUCCEEDED"}
WRITE_CHUNK = 200


class Context:
    """Everything a run needs: config, BigQuery client, rendered prompt."""

    def __init__(self, cfg: ScoringConfig, client, vertex=None):
        self.cfg = cfg
        self.client = client
        self.countries = S.load_countries(client)
        if not self.countries:
            raise RuntimeError("gold.dim_country returned no current countries")
        self.codes = {c for c, _ in self.countries}
        self.system = P.render_system_prompt(cfg.prompt_path, self.countries)
        self.schema = P.response_schema(sorted(self.codes))
        self.free_schema = P.response_schema(sorted(self.codes), free_country=True)
        self.vertex = vertex or V.VertexClient(BQ_PROJECT)

    def schema_for(self, spec) -> dict:
        return self.free_schema if spec.country_field == "free" else self.schema

    def version(self, spec) -> str:
        variant = "free_country" if spec.country_field == "free" else None
        return P.model_version(spec.model_id, self.cfg.prompt_name, self.system, variant)


def _logged(pipeline, billing_tag=None):
    """Decorator: log the run to etl_run with its outcome (and who paid, if
    not the project's own billing)."""
    def wrap(fn):
        def inner(ctx, *args, **kwargs):
            run_id = S.new_run_id(pipeline)
            started = S.utcnow()
            try:
                result = fn(ctx, run_id, *args, **kwargs)
                S.log_run(ctx.client, pipeline, run_id, started,
                          result.pop("_status", "SUCCEEDED"), result.get("rows_written"),
                          billing_tag=billing_tag)
                log.info("%s %s: %s", pipeline, run_id, json.dumps(result, default=str))
                return result
            except Exception as exc:
                S.log_run(ctx.client, pipeline, run_id, started, "FAILED",
                          error=f"{type(exc).__name__}: {exc}", billing_tag=billing_tag)
                raise
        return inner
    return wrap
