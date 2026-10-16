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


# ── answers -> rows ──────────────────────────────────────────────────────────

def _rows_for_answer(ctx, doc, model_version, run_id, text=None, error=None):
    if error is None:
        try:
            return S.label_rows(doc, model_version, run_id, countries=P.validate(text, ctx.codes))
        except P.ResponseError as exc:
            error = str(exc)
    return S.label_rows(doc, model_version, run_id, error=error)


def _flush(ctx, buffer):
    if buffer:
        S.write_labels(ctx.client, buffer)
    return []


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


# ── collect (BATCH) ──────────────────────────────────────────────────────────

def _job_location(job_name: str) -> str:
    return job_name.split("/locations/")[1].split("/")[0]


def _doc_key(row):
    if row.get("doc_id") and row.get("source_version"):
        return row["doc_id"], row["source_version"]
    doc_id, _, version = (row.get("custom_id") or "").partition("|")
    return doc_id, version


@_logged("SCORE_COLLECT")
def collect(ctx, run_id):
    out, written = {}, 0
    for j in S.latest_batch_jobs(ctx.client):
        if j.provider == "ANTHROPIC_API":  # collected by anthropic_history, not Vertex
            continue
        info = ctx.vertex.get_job(j.vertex_job_name, _job_location(j.vertex_job_name))
        state = info.get("state")
        base = {k: j[k] for k in ("batch_id", "run_id", "model_version", "provider", "vertex_job_name",
                                  "input_table", "output_table", "n_requests", "submitted_at")}
        if state not in DONE_STATES:
            if j.state != "RUNNING" and state == "JOB_STATE_RUNNING":
                S.record_batch_state(ctx.client, {**base, "state": "RUNNING"})
            out[j.batch_id] = state
            continue
        if state not in ("JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED"):
            S.record_batch_state(ctx.client, {**base, "state": "FAILED", "completed_at": S.utcnow(),
                                              "error_summary": json.dumps(info.get("error") or {})[:2000]})
            out[j.batch_id] = state
            continue
        rows = S.read_rows(ctx.client, j.output_table)
        dates = S.doc_published_at(ctx.client, [_doc_key(r)[0] for r in rows])
        extract = V.gemini_text if j.provider == "GOOGLE" else V.claude_text
        buffer, n_ok, n_err, n_rejected = [], 0, 0, 0
        for r in rows:
            doc_id, version = _doc_key(r)
            doc = {"doc_id": doc_id, "source_version": version, "published_at": dates.get(doc_id)}
            if not r.get("response"):  # None or {} (Vertex fills {} plus a status)
                # Vertex rejected the request itself (no model answer): write nothing,
                # so the article is pending again for the next run.
                n_rejected += 1
                log.warning("[%s] request rejected for %s: %s", j.batch_id, doc_id,
                            str(r.get("status"))[:200])
                continue
            labels = _rows_for_answer(ctx, doc, j.model_version, run_id, text=extract(r["response"]))
            n_err += labels[0]["tag_status"] == "ERROR"
            n_ok += labels[0]["tag_status"] != "ERROR"
            buffer.extend(labels)
            if len(buffer) >= WRITE_CHUNK:
                written += len(buffer)
                buffer = _flush(ctx, buffer)
        written += len(buffer)
        _flush(ctx, buffer)
        S.record_batch_state(ctx.client, {**base, "state": "COLLECTED", "completed_at": S.utcnow(),
                                          "n_succeeded": n_ok, "n_failed": n_err + n_rejected,
                                          "error_summary": f"{n_rejected} requests rejected" if n_rejected else None})
        out[j.batch_id] = {"state": state, "ok": n_ok, "answer_errors": n_err, "rejected": n_rejected}
    return {"jobs": out, "rows_written": written}


# ── aggregate and bias ───────────────────────────────────────────────────────

def aggregation_version(ctx) -> str:
    consensus = S.consensus_version(ctx.client) or "none"
    params = json.dumps(ctx.cfg.aggregation, sort_keys=True)
    return hashlib.sha256(f"{consensus}|{params}".encode()).hexdigest()[:16]


@_logged("SENTIMENT_AGGREGATE")
def aggregate(ctx, run_id, window_days=None):
    agg = ctx.cfg.aggregation
    version = aggregation_version(ctx)
    job = S.run_sql_file(ctx.client, SQL_DIR / "04_aggregate_signal.sql", {
        "window_days": int(window_days or agg.get("window_days", 14)),
        "aggregation_version": version,
        "min_docs": int(agg.get("min_docs", 3)),
        "min_outlets": int(agg.get("min_outlets", 2)),
    })
    return {"aggregation_version": version, "rows_written": job.num_dml_affected_rows}


@_logged("SENTIMENT_BIAS")
def bias(ctx, run_id, window_days=None):
    b = ctx.cfg.bias
    job = S.run_sql_file(ctx.client, SQL_DIR / "05_bias_kl.sql", {
        "window_days": int(window_days or b.get("window_days", 14)),
        "min_per_language": int(b.get("min_per_language", 10)),
        "evaluation_id": run_id,
    })
    return {"rows_written": job.num_dml_affected_rows}
