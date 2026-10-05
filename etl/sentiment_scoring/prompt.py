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
