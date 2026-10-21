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
