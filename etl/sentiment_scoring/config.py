"""
Configuration for sentiment scoring (country tagging + sentiment per country).

Model choices and run limits live in config/sentiment_models.yaml. The GCP
project comes from BQ_PROJECT, deliberately with no default.
"""

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
MODELS_YAML = REPO_ROOT / "config" / "sentiment_models.yaml"

BQ_PROJECT = os.getenv("BQ_PROJECT")

DOCUMENT_VIEW = "silver.v_document"
LABEL_TABLE = "silver.sentiment_label"
COUNTRY_TABLE = "gold.dim_country"
MODEL_VERSION_TABLE = "gold.dim_model_version"
SIGNAL_TABLE = "gold.fact_sentiment_signal"
EVAL_TABLE = "gold.model_evaluation_slice"
RUN_TABLE = "audit.etl_run"
BATCH_JOB_TABLE = "audit.llm_batch_job"
DQ_TABLE = "audit.dq_issue"

TASK = "TAG_AND_SENTIMENT"


def fq(table: str) -> str:
    """Fully-qualified table name; live runs require BQ_PROJECT."""
    if not BQ_PROJECT:
        raise RuntimeError("BQ_PROJECT is not set (expected your-gcp-project).")
    return f"{BQ_PROJECT}.{table}"


@dataclass
class ModelSpec:
    slot: str            # stable role name: gemini / claude / gemma
    provider: str        # GOOGLE | ANTHROPIC | GOOGLE_OPEN
    model_id: str        # exact, pinned Vertex model ID
    call_mode: str       # BATCH | SERIAL
    location: str        # Vertex location for this model's calls/jobs
    batch_dataset: str = None   # BATCH only: dataset in the job's location
    max_output_tokens: int = 2048
    enabled: bool = True
    country_field: str = "enum"  # enum: schema lists our codes | free: any code, unknown ones dropped


@dataclass
class ScoringConfig:
    prompt_name: str
    prompt_path: Path
    max_docs_per_model: int
    lookback_days: int
    serial_concurrency: int
    models: list = field(default_factory=list)
    schedule: dict = field(default_factory=dict)
    aggregation: dict = field(default_factory=dict)
    bias: dict = field(default_factory=dict)

    @property
    def enabled_models(self) -> list:
        return [m for m in self.models if m.enabled]

    def config_hash(self) -> str:
        payload = {k: (str(v) if isinstance(v, Path) else v)
                   for k, v in asdict(self).items()}
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def load_config(path: Path = MODELS_YAML) -> ScoringConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    limits = raw.get("run_limits", {})
    models = [ModelSpec(**m) for m in raw["models"]]
    slots = [m.slot for m in models]
    if len(set(slots)) != len(slots):
        raise ValueError(f"duplicate model slots in {path}: {slots}")
    for m in models:
        if m.country_field not in ("enum", "free"):
            raise ValueError(f"{m.slot}: country_field must be enum or free, not {m.country_field!r}")
    return ScoringConfig(
        prompt_name=raw["prompt_name"],
        prompt_path=REPO_ROOT / raw["prompt_path"],
        max_docs_per_model=int(limits.get("max_docs_per_model", 2000)),
        lookback_days=int(limits.get("lookback_days", 7)),
        serial_concurrency=int(limits.get("serial_concurrency", 4)),
        models=models,
        schedule=raw.get("schedule", {}),
        aggregation=raw.get("aggregation", {}),
        bias=raw.get("bias", {}),
    )
