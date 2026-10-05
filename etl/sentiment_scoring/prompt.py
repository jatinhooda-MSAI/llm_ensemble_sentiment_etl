"""
Prompt rendering, response schema and response validation.

The same rendered system prompt goes to every model. Its hash is part of the
model_version, so any change to the instructions or the country list creates
a new version automatically.
"""

import hashlib
import json
from pathlib import Path

LABELS = ("POSITIVE", "NEGATIVE", "NEUTRAL", "MIXED", "UNCERTAIN")
MAX_COUNTRIES = 5
MAX_EVIDENCE = 2
MAX_EVIDENCE_CHARS = 300


class ResponseError(ValueError):
    """A model answer that is not usable JSON in the required shape."""


def render_system_prompt(template_path: Path, countries: list) -> str:
    """countries: [(iso3, name), ...] from dim_country."""
    country_list = "\n".join(f"{iso3}: {name}" for iso3, name in sorted(countries))
    return Path(template_path).read_text(encoding="utf-8").replace("{country_list}", country_list)


def prompt_sha256(rendered_prompt: str) -> str:
    return hashlib.sha256(rendered_prompt.encode("utf-8")).hexdigest()


def model_version(model_id: str, prompt_name: str, rendered_prompt: str, variant: str = None) -> str:
    """variant marks a non-default response schema (e.g. "free_country"), so
    the same prompt answered under a different schema is a separate version."""
    name = f"{prompt_name}+{variant}" if variant else prompt_name
    return f"{model_id}|{name}|{prompt_sha256(rendered_prompt)[:12]}"


def user_message(doc: dict) -> str:
    """The article itself. The outlet name is left out on purpose."""
    published = doc.get("published_at") or "unknown"
    return (
        f"Language: {doc.get('language_iso639_3') or 'unknown'}\n"
        f"Published: {published}\n"
        f"Title: {doc.get('title') or ''}\n\n"
        f"Article:\n{doc.get('body_text') or ''}"
    )


def response_schema(country_codes: list, free_country: bool = False) -> dict:
    """Kept to the JSON Schema subset all three providers accept (types, enums,
    required, no extra keys). Ranges and caps are enforced in validate().

    free_country: country_iso3 is any string instead of an enum of our codes.
    Under strict decoding the enum forced a model with no thinking step onto a
    look-alike code (Saudi Arabia -> Sudan, Kuwait -> Kenya); free, it names the
    real country and validate() drops codes that are not in the list."""
    country = {"type": "string"} if free_country else {"type": "string", "enum": sorted(country_codes)}
    return {
        "type": "object",
        "properties": {
            "countries": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "country_iso3": country,
                        "evidence": {"type": "array", "items": {"type": "string"}},
                        "sentiment_label": {"type": "string", "enum": list(LABELS)},
                        "sentiment_score": {"type": "number"},
                    },
                    "required": ["country_iso3", "evidence", "sentiment_label", "sentiment_score"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["countries"],
        "additionalProperties": False,
    }


def _extract_json(text: str):
    text = (text or "").strip()
    if text.startswith("```"):  # tolerate a fenced block
        text = text.strip("`")
        text = text[text.find("{"):] if "{" in text else text
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ResponseError("no JSON object in the answer")
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError as exc:
        raise ResponseError(f"invalid JSON: {exc}") from exc


def validate(answer_text: str, country_codes: set) -> list:
    """Return a clean list of {country_iso3, evidence, sentiment_label,
    sentiment_score}. Unknown countries are dropped; duplicates keep the first
    entry; at most MAX_COUNTRIES are kept."""
    data = _extract_json(answer_text)
    if data == {}:  # an empty object means "no countries"
        return []
    if not isinstance(data, dict) or not isinstance(data.get("countries"), list):
        raise ResponseError("answer has no 'countries' list")
    cleaned, seen = [], set()
    for item in data["countries"]:
        if not isinstance(item, dict):
            continue
        code = str(item.get("country_iso3") or "").strip().upper()
        if code not in country_codes or code in seen:
            continue
        label = str(item.get("sentiment_label") or "").strip().upper()
        if label not in LABELS:
            raise ResponseError(f"unknown label {label!r} for {code}")
        try:
            score = float(item.get("sentiment_score"))
        except (TypeError, ValueError):
            score = 0.0
        evidence = [str(e)[:MAX_EVIDENCE_CHARS] for e in (item.get("evidence") or [])
                    if str(e).strip()][:MAX_EVIDENCE]
        cleaned.append({
            "country_iso3": code,
            "evidence": evidence,
            "sentiment_label": label,
            "sentiment_score": max(-1.0, min(1.0, score)),
        })
        seen.add(code)
        if len(cleaned) == MAX_COUNTRIES:
            break
    return cleaned
