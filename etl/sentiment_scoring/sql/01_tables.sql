-- Sentiment scoring: new tables (BigQuery Standard SQL, project your-gcp-project).
-- Reuses existing tables rather than duplicating them: gold.dim_country
-- (country vocabulary), gold.dim_source (source registry) and
-- audit.etl_run (run log).
-- Everything is append-only except the derived gold series, which each run
-- recomputes for a trailing window. Idempotent: IF NOT EXISTS throughout.

-- Scratch datasets for Vertex batch inputs/outputs; tables expire after 7 days.
-- A batch job must use a dataset in its own location: Gemini batch runs on the
-- global endpoint, Claude batch only in the US multi-region.
CREATE SCHEMA IF NOT EXISTS `your-gcp-project.batch`
OPTIONS (location = 'us-central1', default_table_expiration_days = 7,
         description = 'Vertex AI batch prediction inputs and outputs (auto-expire)');

-- batch_us (location US) cannot be created from a us-central1 script:
--   bq --project_id=your-gcp-project mk --dataset --location=US \
--      --default_table_expiration=604800 your-gcp-project:batch_us

-- Every judgment about an (article, country): one row per model output or
-- human label. No article text; the article lives in bronze.
CREATE TABLE IF NOT EXISTS `your-gcp-project.silver.sentiment_label` (
  label_id         STRING    NOT NULL OPTIONS (description = 'UUID'),
  doc_id           STRING    NOT NULL OPTIONS (description = 'Stable article ID (source + canonical URL hash)'),
  source_version   STRING    NOT NULL OPTIONS (description = 'Exact article version (title/body hash)'),
  country_iso3     STRING             OPTIONS (description = 'African country; NULL on a model NO_COUNTRY row'),
  labeler_type     STRING    NOT NULL OPTIONS (description = 'MODEL | HUMAN'),
  labeler_id       STRING    NOT NULL OPTIONS (description = 'dim_model_version.model_version or dim_labeler.labeler_id'),
  tag_status       STRING             OPTIONS (description = 'Models: TAGGED | NO_COUNTRY'),
  is_about_country BOOL               OPTIONS (description = 'Humans: is the article substantially about this country'),
  sentiment_label  STRING             OPTIONS (description = 'POSITIVE | NEGATIVE | NEUTRAL | MIXED | UNCERTAIN'),
  sentiment_score  FLOAT64            OPTIONS (description = 'Models only: -1.0 to 1.0'),
  evidence         ARRAY<STRING>      OPTIONS (description = 'Models only: up to 2 short supporting quotes'),
  reason           STRING             OPTIONS (description = 'Humans: MODEL_DISAGREEMENT | RANDOM_AUDIT | SECOND_OPINION'),
  is_skip          BOOL               OPTIONS (description = 'Humans: skipped without labeling'),
  run_id           STRING             OPTIONS (description = 'Scoring run or labeling session'),
  error_detail     STRING             OPTIONS (description = 'Models: why an answer was unusable (tag_status ERROR)'),
  published_date   DATE               OPTIONS (description = 'Article publish date, copied for partition pruning'),
  created_at       TIMESTAMP NOT NULL
)
PARTITION BY published_date
CLUSTER BY country_iso3, labeler_type, labeler_id
OPTIONS (description = 'All model and human sentiment judgments per (article, country)');

-- Daily sentiment series per country (and per language), derived from the
-- consensus view. Recomputed for a trailing window each run.
CREATE TABLE IF NOT EXISTS `your-gcp-project.gold.fact_sentiment_signal` (
  country_iso3           STRING    NOT NULL,
  sector_code            STRING    NOT NULL OPTIONS (description = 'ALL until sectors exist'),
  narrative_code         STRING    NOT NULL OPTIONS (description = 'ALL until narratives exist'),
  language_code          STRING    NOT NULL OPTIONS (description = 'ALL, or ISO 639-3 for a per-language series'),
  period_start           DATE      NOT NULL,
  period_grain           STRING    NOT NULL OPTIONS (description = 'DAY'),
  aggregation_version    STRING    NOT NULL OPTIONS (description = 'Consensus models + weighting config hash'),
  score_0_100            FLOAT64            OPTIONS (description = '50 = neutral; 50 + 50 x weighted (positive - negative) share'),
  score_low              FLOAT64            OPTIONS (description = 'Lower bound of the ~95% range'),
  score_high             FLOAT64            OPTIONS (description = 'Upper bound of the ~95% range'),
  coverage_confidence    FLOAT64            OPTIONS (description = '0-1 from article and outlet counts'),
  coverage_flag          STRING             OPTIONS (description = 'OK | LOW. Shown, never hidden'),
  bias_flag              STRING             OPTIONS (description = 'PASS | FAIL | NOT_CHECKED. Shown, never hidden'),
  n_docs                 INT64,
  n_outlets              INT64,
  n_languages            INT64,
  n_human_resolved       INT64,
  n_provisional          INT64              OPTIONS (description = 'Articles still awaiting a human, counted at model majority'),
  share_positive         FLOAT64,
  share_negative         FLOAT64,
  share_neutral          FLOAT64,
  share_mixed            FLOAT64,
  n_uncertain            INT64              OPTIONS (description = 'Counted but excluded from the score'),
  velocity               FLOAT64            OPTIONS (description = 'Change vs the previous 7-day mean'),
  intensity              FLOAT64            OPTIONS (description = 'Share of articles that are POSITIVE or NEGATIVE'),
  representative_doc_ids ARRAY<STRING>      OPTIONS (description = '1-20 example articles'),
  data_as_of             TIMESTAMP          OPTIONS (description = 'Latest article included'),
  computed_at            TIMESTAMP NOT NULL
)
PARTITION BY period_start
CLUSTER BY country_iso3, language_code
OPTIONS (description = 'Per-country daily sentiment signal (basis of the country sentiment score)');

-- Accuracy, agreement and bias metrics, one row per metric per slice.
CREATE TABLE IF NOT EXISTS `your-gcp-project.gold.model_evaluation_slice` (
  evaluation_id   STRING    NOT NULL,
  evaluation_type STRING    NOT NULL OPTIONS (description = 'ACCURACY | AGREEMENT | BIAS'),
  subject_version STRING    NOT NULL OPTIONS (description = 'model_version or aggregation_version evaluated'),
  eval_set_id     STRING             OPTIONS (description = 'Locked human-labeled set, when applicable'),
  window_start    DATE,
  window_end      DATE,
  segment_type    STRING    NOT NULL OPTIONS (description = 'ALL | LANGUAGE | COUNTRY | SOURCE | LABEL'),
  segment_key     STRING    NOT NULL,
  metric_name     STRING    NOT NULL OPTIONS (description = 'e.g. f1_macro, kappa_human_human, kl_divergence, weat_effect, name_swap_shift'),
  metric_value    FLOAT64,
  metric_json     JSON               OPTIONS (description = 'e.g. a confusion table'),
  n_items         INT64,
  threshold       FLOAT64,
  passed          BOOL,
  computed_at     TIMESTAMP NOT NULL
)
PARTITION BY DATE(computed_at)
CLUSTER BY metric_name, segment_key;

-- Human labelers (login codes only, no personal details). New row per change.
CREATE TABLE IF NOT EXISTS `your-gcp-project.gold.dim_labeler` (
  labeler_id  STRING    NOT NULL,
  login_code  STRING    NOT NULL,
  languages   ARRAY<STRING>      OPTIONS (description = 'ISO 639-3 codes the labeler reads'),
  active      BOOL      NOT NULL,
  valid_from  TIMESTAMP NOT NULL,
  valid_to    TIMESTAMP,
  created_by  STRING
);



-- Every model + prompt combination used, with the prompt text itself.
-- Status changes are new rows; the latest row per model_version is current.
CREATE TABLE IF NOT EXISTS `your-gcp-project.gold.dim_model_version` (
  model_version STRING    NOT NULL OPTIONS (description = 'model_id|prompt_name|prompt_sha256[:12]'),
  task          STRING    NOT NULL OPTIONS (description = 'TAG_AND_SENTIMENT | EMBEDDING | ...'),
  provider      STRING    NOT NULL OPTIONS (description = 'GOOGLE | ANTHROPIC | GOOGLE_OPEN'),
  model_id      STRING    NOT NULL OPTIONS (description = 'Exact, pinned Vertex model ID'),
  call_mode     STRING    NOT NULL OPTIONS (description = 'BATCH | SERIAL'),
  prompt_name   STRING    NOT NULL,
  prompt_sha256 STRING    NOT NULL,
  prompt_text   STRING    NOT NULL,
  params        JSON,
  status        STRING    NOT NULL OPTIONS (description = 'ACTIVE | RETIRED'),
  recorded_at   TIMESTAMP NOT NULL,
  notes         STRING
);


-- Data-quality problems and failed checks, with evidence.
CREATE TABLE IF NOT EXISTS `your-gcp-project.audit.dq_issue` (
  dq_issue_id    STRING    NOT NULL,
  rule_id        STRING    NOT NULL,
  severity       STRING    NOT NULL OPTIONS (description = 'BLOCKER | HIGH | MEDIUM | LOW'),
  source_id      STRING,
  run_id         STRING,
  affected_grain JSON,
  evidence       JSON,
  disposition    STRING,
  owner          STRING,
  opened_at      TIMESTAMP NOT NULL,
  resolved_at    TIMESTAMP
)
PARTITION BY DATE(opened_at)
CLUSTER BY rule_id, severity;

-- Vertex batch jobs: one row per state change (latest row per batch_id is current).
CREATE TABLE IF NOT EXISTS `your-gcp-project.audit.llm_batch_job` (
  batch_id        STRING    NOT NULL,
  run_id          STRING    NOT NULL,
  model_version   STRING    NOT NULL,
  provider        STRING    NOT NULL,
  vertex_job_name STRING,
  input_table     STRING,
  output_table    STRING,
  n_requests      INT64,
  state           STRING    NOT NULL OPTIONS (description = 'SUBMITTED | RUNNING | SUCCEEDED | FAILED | COLLECTED'),
  n_succeeded     INT64,
  n_failed        INT64,
  est_cost_usd    FLOAT64,
  submitted_at    TIMESTAMP,
  completed_at    TIMESTAMP,
  recorded_at     TIMESTAMP NOT NULL,
  error_summary   STRING,
  billing_tag     STRING             OPTIONS (description = 'NULL = the project\'s own billing; otherwise who paid')
)
PARTITION BY DATE(recorded_at)
CLUSTER BY state, model_version;
