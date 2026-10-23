"""
One-off historical load: score the article backlog with Claude through
Anthropic's Message Batches API (not Vertex), writing the same label rows the
Vertex route writes, under the same model_version.

Claude's version is registered as STANDBY, so these labels do not count in the
consensus until Claude is enabled on Vertex (config: enabled: true). After that
the scheduled jobs only score articles Claude has not answered yet.

API key: ANTHROPIC_API_KEY, or the file named by ANTHROPIC_API_KEY_FILE
(default ~/.config/news-sentiment/anthropic_api_key). The key is never logged.
"""

import json
import logging
import os
import time
from pathlib import Path

from etl.sentiment_scoring import prompt as P
from etl.sentiment_scoring import store as S
from etl.sentiment_scoring.config import BQ_PROJECT, MODEL_VERSION_TABLE, TASK

log = logging.getLogger("sentiment_scoring")

PROVIDER = "ANTHROPIC_API"
# Who pays for runs on this route: they bill the key's Anthropic account, not
# the project's own billing. Recorded in etl_run and llm_batch_job.
BILLING_TAG = os.getenv("ANTHROPIC_BILLING_TAG", "personal:anthropic_api")
CHUNK = 4000            # requests per batch; keeps each well under the 256 MB limit
KEY_FILE = "~/.config/news-sentiment/anthropic_api_key"
# Batch rates (50% of list) for Claude Sonnet 5.5, USD per million tokens.
# Used only for the cost estimate recorded in llm_batch_job.
RATES = {"input": 1.00, "output": 5.00, "cache_read": 0.10, "cache_write": 1.25}


def client():
    import anthropic

    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        path = Path(os.getenv("ANTHROPIC_API_KEY_FILE", KEY_FILE)).expanduser()
        if not path.is_file() or not path.read_text().strip():
            raise RuntimeError(f"No Anthropic API key: set ANTHROPIC_API_KEY or save it to {path}")
        key = path.read_text().strip()
    return anthropic.Anthropic(api_key=key)


def claude_spec(cfg):
    specs = [m for m in cfg.models if m.provider == "ANTHROPIC"]
    if not specs:
        raise RuntimeError("no ANTHROPIC model in config/sentiment_models.yaml")
    return specs[0]


def request_params(spec, system: str, user: str, schema: dict) -> dict:
    """Messages body: schema-enforced JSON, shared instructions cached,
    adaptive thinking at low effort, no sampling parameters."""
    return {
        "model": spec.model_id,
        "max_tokens": spec.max_output_tokens,
        "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        "messages": [{"role": "user", "content": user}],
        "output_config": {"effort": "low",
                          "format": {"type": "json_schema", "schema": schema}},
    }


def answer_from_message(message):
    """(text, error) for one succeeded result; exactly one of them is set."""
    stop = getattr(message, "stop_reason", None)
    if stop == "refusal":
        details = getattr(message, "stop_details", None)
        return None, f"refusal: {getattr(details, 'category', None) or 'unspecified'}"
    if stop == "max_tokens":
        return None, "max_tokens reached before the answer was complete"
    text = "".join(b.text for b in message.content if getattr(b, "type", None) == "text")
    return text, None


def _cost(usage) -> float:
    return ((getattr(usage, "input_tokens", 0) or 0) * RATES["input"]
            + (getattr(usage, "output_tokens", 0) or 0) * RATES["output"]
            + (getattr(usage, "cache_read_input_tokens", 0) or 0) * RATES["cache_read"]
            + (getattr(usage, "cache_creation_input_tokens", 0) or 0) * RATES["cache_write"]) / 1e6


def register_standby(ctx, spec) -> str:
    """Record Claude's version in the registry without making it ACTIVE."""
    mv = ctx.version(spec)
    if mv not in S.current_model_versions(ctx.client):
        S._stream(ctx.client, MODEL_VERSION_TABLE, [{
            "model_version": mv, "task": TASK, "provider": spec.provider,
            "model_id": spec.model_id, "call_mode": "BATCH",
            "prompt_name": ctx.cfg.prompt_name, "prompt_sha256": P.prompt_sha256(ctx.system),
            "prompt_text": ctx.system,
            "params": json.dumps({"slot": spec.slot, "routes": ["anthropic_api_batch", "vertex_batch"],
                                  "max_output_tokens": spec.max_output_tokens, "effort": "low"}),
            "status": "STANDBY", "recorded_at": S.utcnow(),
            "notes": "historical load via Anthropic API; joins the consensus once enabled on Vertex",
        }])
    return mv


def submit(ctx, run_id, limit=None) -> dict:
    spec = claude_spec(ctx.cfg)
    mv = register_standby(ctx, spec)
    docs = S.pending_docs(ctx.client, mv, limit or 100_000, lookback_days=None)
    return submit_docs(ctx, run_id, spec, mv, docs)


def submit_docs(ctx, run_id, spec, mv, docs) -> dict:
    """Submit the given articles (dicts with doc_id, source_version, title,
    body_text, language_iso639_3, published_at) as Anthropic batches."""
    api = client()
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    batches = []
    for start in range(0, len(docs), CHUNK):
        chunk = docs[start:start + CHUNK]
        batch_id = f"claude_api_{run_id.replace('-', '_')}_{start // CHUNK}"
        mapping_table = f"{BQ_PROJECT}.batch.in_{batch_id}"
        S.create_input_table(ctx.client, mapping_table, [{
            "doc_id": d["doc_id"], "source_version": d["source_version"],
            "custom_id": f"r{start + i:06d}", "request": {"route": "anthropic_api"},
        } for i, d in enumerate(chunk)])
        batch = api.messages.batches.create(requests=[
            Request(custom_id=f"r{start + i:06d}",
                    params=MessageCreateParamsNonStreaming(
                        **request_params(spec, ctx.system, P.user_message(d), ctx.schema_for(spec))))
            for i, d in enumerate(chunk)])
        S.record_batch_state(ctx.client, {
            "batch_id": batch_id, "run_id": run_id, "model_version": mv, "provider": PROVIDER,
            "vertex_job_name": batch.id, "input_table": mapping_table, "output_table": None,
            "n_requests": len(chunk), "state": "SUBMITTED", "submitted_at": S.utcnow(),
            "billing_tag": BILLING_TAG})
        batches.append({"batch_id": batch_id, "anthropic_batch": batch.id, "n_requests": len(chunk)})
        log.info("submitted %s (%d requests)", batch.id, len(chunk))
    return {"model_version": mv, "pending": len(docs), "batches": batches}


def collect(ctx, run_id) -> dict:
    api = client()
    out, written = {}, 0
    for j in S.latest_batch_jobs(ctx.client):
        if j.provider != PROVIDER:
            continue
        base = {k: j[k] for k in ("batch_id", "run_id", "model_version", "provider", "vertex_job_name",
                                  "input_table", "output_table", "n_requests", "submitted_at")}
        base["billing_tag"] = BILLING_TAG
        batch = api.messages.batches.retrieve(j.vertex_job_name)
        if batch.processing_status != "ended":
            if j.state != "RUNNING":
                S.record_batch_state(ctx.client, {**base, "state": "RUNNING"})
            out[j.batch_id] = f"{batch.processing_status} ({batch.request_counts.processing} processing)"
            continue
        mapping = {r["custom_id"]: (r["doc_id"], r["source_version"])
                   for r in S.read_rows(ctx.client, j.input_table)}
        dates = S.doc_published_at(ctx.client, [d for d, _ in mapping.values()])
        buffer, n_ok, n_err, n_rejected, cost = [], 0, 0, 0, 0.0
        for result in api.messages.batches.results(j.vertex_job_name):
            doc_id, version = mapping.get(result.custom_id, (None, None))
            if doc_id is None:
                continue
            if result.result.type != "succeeded":  # errored / canceled / expired: pending again
                n_rejected += 1
                continue
            message = result.result.message
            cost += _cost(message.usage)
            doc = {"doc_id": doc_id, "source_version": version, "published_at": dates.get(doc_id)}
            text, error = answer_from_message(message)
            if error is None:
                try:
                    rows = S.label_rows(doc, j.model_version, run_id, countries=P.validate(text, ctx.codes))
                except P.ResponseError as exc:
                    rows = S.label_rows(doc, j.model_version, run_id, error=str(exc))
            else:
                rows = S.label_rows(doc, j.model_version, run_id, error=error)
            n_err += rows[0]["tag_status"] == "ERROR"
            n_ok += rows[0]["tag_status"] != "ERROR"
            buffer.extend(rows)
            if len(buffer) >= 500:
                S.write_labels(ctx.client, buffer)
                written += len(buffer)
                buffer = []
        if buffer:
            S.write_labels(ctx.client, buffer)
            written += len(buffer)
        S.record_batch_state(ctx.client, {
            **base, "state": "COLLECTED", "completed_at": S.utcnow(), "n_succeeded": n_ok,
            "n_failed": n_err + n_rejected, "est_cost_usd": round(cost, 2),
            "error_summary": f"{n_rejected} requests errored/expired" if n_rejected else None})
        out[j.batch_id] = {"ok": n_ok, "answer_errors": n_err, "rejected": n_rejected,
                           "est_cost_usd": round(cost, 2)}
    return {"batches": out, "rows_written": written}


def wait_until_ended(ctx, poll_seconds=60, max_hours=24):
    """Block until every submitted Anthropic batch has ended. BigQuery is read
    once up front; the wait itself only polls Anthropic, so an expiring Google
    token during a long wait does not matter."""
    api = client()
    open_jobs = [(j.batch_id, j.vertex_job_name)
                 for j in S.latest_batch_jobs(ctx.client, states=("SUBMITTED", "RUNNING"))
                 if j.provider == PROVIDER]
    deadline = time.time() + max_hours * 3600
    while time.time() < deadline:
        states = {batch_id: api.messages.batches.retrieve(name).processing_status
                  for batch_id, name in open_jobs}
        if all(s == "ended" for s in states.values()):
            return
        log.info("waiting: %s", states)
        time.sleep(poll_seconds)
    raise TimeoutError("Anthropic batches still running after the wait limit")
