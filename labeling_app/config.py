"""Environment-driven configuration for the sentiment labeling app.

The app reads the human-labeling queue (silver.labeling_queue), shows
the article from silver.v_document, and appends answers to
silver.sentiment_label. Labelers come from gold.dim_labeler.
The app boots without BQ_PROJECT and returns 503 until it is set.
"""

import os
import re

_PROJECT_RE = re.compile(r"^[a-z][a-z0-9\-]{4,61}[a-z0-9]$")

# The dictionary's five labels (silver.sentiment_result contract).
SENTIMENTS = ("POSITIVE", "NEGATIVE", "NEUTRAL", "MIXED", "UNCERTAIN")


class Config:
    def __init__(self) -> None:
        self.bq_project = os.getenv("BQ_PROJECT", "").strip()
        self.bq_location = os.getenv("BQ_LOCATION", "us-central1").strip()
        self.code_cache_ttl = float(os.getenv("CODE_CACHE_TTL_SECONDS", "60"))
        self.labels_page_limit = int(os.getenv("LABELS_PAGE_LIMIT", "200"))
        self.app_version = os.getenv("APP_VERSION", "dev")

    def table(self, name: str) -> str:
        return f"{self.bq_project}.{name}"

    @property
    def queue_view(self) -> str:
        return self.table("silver.labeling_queue")

    @property
    def document_view(self) -> str:
        return self.table("silver.v_document")

    @property
    def labels_table(self) -> str:
        return self.table("silver.sentiment_label")

    @property
    def labelers_table(self) -> str:
        return self.table("gold.dim_labeler")

    @property
    def countries_table(self) -> str:
        return self.table("gold.dim_country")

    def config_error(self) -> str | None:
        if not self.bq_project:
            return "missing env var: BQ_PROJECT"
        if not _PROJECT_RE.match(self.bq_project):
            return f"BQ_PROJECT is not a valid project id: {self.bq_project!r}"
        return None


config = Config()
