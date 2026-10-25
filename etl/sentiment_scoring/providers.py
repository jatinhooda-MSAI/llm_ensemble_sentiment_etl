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


def gemma_request(model_id: str, system: str, user: str, schema: dict, max_tokens: int) -> dict:
    # json_object mode alone let Gemma answer "{}" for no countries; the full
    # schema (verified 2026-10-24) keeps the required shape.
    return {
        "model": model_id if "/" in model_id else f"google/{model_id}",
        "messages": [{"role": "system", "content": system + JSON_ONLY},
                     {"role": "user", "content": user}],
        "max_tokens": max_tokens,
        "response_format": {"type": "json_schema", "json_schema": {
            "name": "country_sentiment", "schema": schema, "strict": True}},
    }


def _openapi_schema(schema: dict) -> dict:
    """Gemini's responseSchema is an OpenAPI subset: drop additionalProperties."""
    if isinstance(schema, dict):
        return {k: _openapi_schema(v) for k, v in schema.items() if k != "additionalProperties"}
    if isinstance(schema, list):
        return [_openapi_schema(v) for v in schema]
    return schema


# ── Response text extraction ─────────────────────────────────────────────────

def _as_json(value):
    if isinstance(value, str):
        return json.loads(value)
    return value or {}


def gemini_text(response) -> str:
    data = _as_json(response)
    parts = ((data.get("candidates") or [{}])[0].get("content") or {}).get("parts") or []
    return "".join(p.get("text", "") for p in parts if not p.get("thought"))


def claude_text(response) -> str:
    data = _as_json(response)
    return "".join(b.get("text", "") for b in data.get("content") or [] if b.get("type") == "text")


def gemma_text(response) -> str:
    data = _as_json(response)
    return ((data.get("choices") or [{}])[0].get("message") or {}).get("content") or ""


# ── Vertex REST client ───────────────────────────────────────────────────────

def _host(location: str) -> str:
    if location == "global":
        return "aiplatform.googleapis.com"
    if location in ("us", "eu"):
        return f"aiplatform.{location}.rep.googleapis.com"
    return f"{location}-aiplatform.googleapis.com"


class VertexClient:
    """Thin REST wrapper. Auth: GCLOUD_TOKEN env (local runs) or ADC."""

    def __init__(self, project: str):
        self.project = project
        self._session = None

    def _http(self):
        if self._session is None:
            token = os.getenv("GCLOUD_TOKEN")
            if token:
                import requests

                self._session = requests.Session()
                self._session.headers["Authorization"] = f"Bearer {token}"
            else:
                import google.auth
                from google.auth.transport.requests import AuthorizedSession

                creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
                self._session = AuthorizedSession(creds)
        return self._session

    def _url(self, location: str, path: str) -> str:
        return f"https://{_host(location)}/v1/projects/{self.project}/locations/{location}/{path}"

    def create_batch_job(self, location: str, model_path: str, input_table: str,
                         output_table: str, display_name: str) -> dict:
        body = {
            "displayName": display_name,
            "model": model_path,
            "inputConfig": {"instancesFormat": "bigquery",
                            "bigquerySource": {"inputUri": f"bq://{input_table}"}},
            "outputConfig": {"predictionsFormat": "bigquery",
                             "bigqueryDestination": {"outputUri": f"bq://{output_table}"}},
        }
        r = self._http().post(self._url(location, "batchPredictionJobs"), json=body, timeout=60)
        if r.status_code >= 300:
            raise RuntimeError(f"batch job create failed ({r.status_code}): {r.text[:500]}")
        return r.json()

    def get_job(self, job_name: str, location: str) -> dict:
        r = self._http().get(f"https://{_host(location)}/v1/{job_name}", timeout=60)
        r.raise_for_status()
        return r.json()

    def chat_completion(self, location: str, body: dict, retries: int = 6) -> dict:
        url = self._url(location, "endpoints/openapi/chat/completions")
        for attempt in range(retries + 1):
            r = self._http().post(url, json=body, timeout=180)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(min(60, 2 ** attempt * 3))
                continue
            raise RuntimeError(f"chat completion failed ({r.status_code}): {r.text[:300]}")
