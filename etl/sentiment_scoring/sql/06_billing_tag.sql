-- Who paid for a run. Most runs use the project's own billing and leave this NULL.
-- Runs paid outside GCP (e.g. the Claude history load on the author's personal
-- Anthropic API key) carry a tag such as 'personal:anthropic_api'.
-- Nullable column: existing writers are unaffected.

ALTER TABLE `your-gcp-project.audit.etl_run`
  ADD COLUMN IF NOT EXISTS billing_tag STRING
  OPTIONS (description = 'NULL = the project\'s own billing; otherwise who paid, e.g. personal:anthropic_api');

ALTER TABLE `your-gcp-project.audit.llm_batch_job`
  ADD COLUMN IF NOT EXISTS billing_tag STRING
  OPTIONS (description = 'NULL = the project\'s own billing; otherwise who paid, e.g. personal:anthropic_api');

-- Backfill: the Claude history load of 2026-10-25 ran on the author's personal key.
UPDATE `your-gcp-project.audit.etl_run`
SET billing_tag = 'personal:anthropic_api'
WHERE source_id LIKE 'CLAUDE_HISTORY%' AND billing_tag IS NULL;

UPDATE `your-gcp-project.audit.llm_batch_job`
SET billing_tag = 'personal:anthropic_api'
WHERE provider = 'ANTHROPIC_API' AND billing_tag IS NULL;
