"""
Vertex AI calls for the three scorers.

- Gemini and Claude: Vertex batch prediction jobs with BigQuery input/output.
- Gemma: Model-as-a-Service chat completions, one request at a time (no batch).

Request and response shapes follow the Vertex docs (Oct 2026):
- Gemini batch input column `request` = GenerateContentRequest; output `response`.
- Claude batch input columns `custom_id`, `request` = Anthropic Messages body
  with anthropic_version "vertex-2023-10-16"; output `response`.
- Gemma: OpenAI-compatible /endpoints/openapi/chat/completions, model
  "google/<id>", global location only.
"""

import json
import os
import time

JSON_ONLY = ("\n\nReturn only the JSON object described above. No prose, no code fences.")


# ── Request builders ─────────────────────────────────────────────────────────

def gemini_request(system: str, user: str, schema: dict, max_tokens: int) -> dict:
    return {
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "systemInstruction": {"parts": [{"text": system}]},
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": _openapi_schema(schema),
            "maxOutputTokens": max_tokens,
            "thinkingConfig": {"thinkingLevel": "low"},
        },
    }


def claude_request(system: str, user: str, max_tokens: int) -> dict:
    # Structured outputs are off by default on Vertex (org policy), so the
    # schema is enforced by the prompt plus prompt.validate(). Adaptive thinking
    # at low effort, matching the Anthropic-API history load. No sampling params.
    return {
        "anthropic_version": "vertex-2023-10-16",
        "max_tokens": max_tokens,
        "system": system + JSON_ONLY,
        "messages": [{"role": "user", "content": user}],
        "output_config": {"effort": "low"},
    }


def _openapi_schema(schema: dict) -> dict:
    """Gemini's responseSchema is an OpenAPI subset: drop additionalProperties."""
    if isinstance(schema, dict):
        return {k: _openapi_schema(v) for k, v in schema.items() if k != "additionalProperties"}
    if isinstance(schema, list):
        return [_openapi_schema(v) for v in schema]
    return schema
