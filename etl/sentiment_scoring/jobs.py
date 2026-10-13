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


# ── register ─────────────────────────────────────────────────────────────────

@_logged("SCORE_REGISTER")
def register(ctx, run_id):
    entries = [{
        "model_version": ctx.version(m),
        "provider": m.provider,
        "model_id": m.model_id,
        "call_mode": m.call_mode,
        "prompt_name": ctx.cfg.prompt_name,
        "prompt_sha256": P.prompt_sha256(ctx.system),
        "prompt_text": ctx.system,
        "params": {"slot": m.slot, "location": m.location,
                   "max_output_tokens": m.max_output_tokens, "country_field": m.country_field},
        "notes": None,
    } for m in ctx.cfg.enabled_models]
    written = S.register_model_versions(ctx.client, entries)
    return {"active": [e["model_version"] for e in entries],
            "changes": [(r["model_version"], r["status"]) for r in written],
            "rows_written": len(written)}


# ── submit (BATCH) ───────────────────────────────────────────────────────────

def _request_for(ctx, spec, doc):
    user = P.user_message(doc)
    if spec.provider == "GOOGLE":
        return V.gemini_request(ctx.system, user, ctx.schema_for(spec), spec.max_output_tokens)
    if spec.provider == "ANTHROPIC":
        return V.claude_request(ctx.system, user, spec.max_output_tokens)
    raise ValueError(f"no batch request builder for provider {spec.provider}")


@_logged("SCORE_SUBMIT")
def submit(ctx, run_id, limit=None, lookback_days=None):
    out = {}
    for spec in ctx.cfg.enabled_models:
        if spec.call_mode != "BATCH":
            continue
        mv = ctx.version(spec)
        docs = S.pending_docs(ctx.client, mv, limit or ctx.cfg.max_docs_per_model, lookback_days)
        if not docs:
            out[spec.slot] = "nothing pending"
            continue
        batch_id = f"{spec.slot}_{run_id.replace('-', '_')}"
        input_table = f"{BQ_PROJECT}.{spec.batch_dataset}.in_{batch_id}"
        output_table = f"{BQ_PROJECT}.{spec.batch_dataset}.out_{batch_id}"
        S.create_input_table(ctx.client, input_table, [{
            "doc_id": d["doc_id"], "source_version": d["source_version"],
            "custom_id": f"{d['doc_id']}|{d['source_version']}",
            "request": _request_for(ctx, spec, d),
        } for d in docs])
        job = ctx.vertex.create_batch_job(spec.location, MODEL_PATHS[spec.provider].format(spec.model_id),
                                          input_table, output_table, f"sentiment-{batch_id}")
        S.record_batch_state(ctx.client, {
            "batch_id": batch_id, "run_id": run_id, "model_version": mv, "provider": spec.provider,
            "vertex_job_name": job["name"], "input_table": input_table, "output_table": output_table,
            "n_requests": len(docs), "state": "SUBMITTED", "submitted_at": S.utcnow()})
        out[spec.slot] = {"batch_id": batch_id, "n_requests": len(docs), "job": job["name"]}
    return out
