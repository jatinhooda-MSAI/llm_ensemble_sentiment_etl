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
