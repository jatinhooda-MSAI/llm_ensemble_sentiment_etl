-- Read-only quality audit for the six multilingual sentiment news sources.
-- Run in BigQuery Standard SQL against project your-gcp-project.

DECLARE audit_start TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

CREATE TEMP TABLE latest_articles AS
SELECT
  source_id,
  source_record_id,
  source_version,
  country_iso3,
  object_uri,
  payload,
  ingested_at,
  JSON_VALUE(payload, '$.schema_version') AS schema_version,
  JSON_VALUE(payload, '$.article.title') AS title,
  JSON_VALUE(payload, '$.article.canonical_url') AS canonical_url,
  JSON_VALUE(payload, '$.article.body_text') AS body_text,
  SAFE_CAST(JSON_VALUE(payload, '$.article.word_count') AS INT64) AS word_count,
  SAFE_CAST(JSON_VALUE(payload, '$.article.char_count') AS INT64) AS char_count,
  JSON_VALUE(payload, '$.article.body_source') AS body_source,
  JSON_VALUE(payload, '$.dates.published_at_utc') AS published_at_raw,
  SAFE_CAST(JSON_VALUE(payload, '$.dates.published_at_utc') AS TIMESTAMP) AS published_at,
  JSON_VALUE(payload, '$.language.declared_iso639_3') AS declared_language,
  JSON_VALUE(payload, '$.language.detected_iso639_3') AS detected_language,
  SAFE_CAST(JSON_VALUE(payload, '$.language.detection_confidence') AS FLOAT64) AS language_confidence,
  JSON_VALUE(payload, '$.country.method') AS country_method,
  JSON_VALUE(payload, '$.fetch.error') AS fetch_error,
  SAFE_CAST(JSON_VALUE(payload, '$.fetch.http_status') AS INT64) AS http_status,
  JSON_VALUE(payload, '$.fetch.content_sha256') AS content_sha256
FROM `your-gcp-project.bronze.source_record`
WHERE source_id IN (
  'SRC_BBC_SWAHILI', 'SRC_BBC_ARABIC', 'SRC_HESPRESS',
  'SRC_ALLAFRICA_FR', 'SRC_AFRICANEWS_FR', 'SRC_FINANCIAL_AFRIK'
)
AND ingested_at >= audit_start
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY source_id, source_record_id
  ORDER BY ingested_at DESC, source_version DESC
) = 1;

-- 1. Executive scorecard. Any non-zero "bad_*" count needs investigation.
SELECT
  source_id,
  COUNT(*) AS latest_articles,
  MIN(ingested_at) AS first_ingested,
  MAX(ingested_at) AS last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR) AS hours_since_last_ingest,
  COUNTIF(schema_version != '1.0.0' OR schema_version IS NULL) AS bad_schema_version,
  COUNTIF(title IS NULL OR TRIM(title) = '') AS bad_missing_title,
  COUNTIF(body_text IS NULL OR TRIM(body_text) = '') AS bad_missing_body,
  COUNTIF(word_count < 100) AS short_under_100_words,
  COUNTIF(body_source = 'feed_summary') AS summary_fallbacks,
  COUNTIF(fetch_error IS NOT NULL) AS fetch_errors,
  COUNTIF(content_sha256 != source_version OR content_sha256 IS NULL) AS bad_hash_contract,
  COUNTIF(declared_language != detected_language) AS language_mismatches,
  COUNTIF(language_confidence < 0.80) AS low_language_confidence,
  COUNTIF(country_iso3 IS NULL) AS missing_country,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(country_iso3 IS NOT NULL), COUNT(*)), 1) AS country_coverage_pct,
  APPROX_QUANTILES(word_count, 100)[OFFSET(50)] AS median_words,
  APPROX_QUANTILES(word_count, 100)[OFFSET(10)] AS p10_words,
  APPROX_QUANTILES(word_count, 100)[OFFSET(90)] AS p90_words
FROM latest_articles
GROUP BY source_id
ORDER BY source_id;

-- 2. Daily volume and freshness. Look for missing days and abrupt volume changes.
SELECT
  DATE(ingested_at) AS ingest_date,
  source_id,
  COUNT(*) AS latest_articles,
  COUNTIF(body_source = 'feed_summary') AS summary_fallbacks,
  COUNTIF(fetch_error IS NOT NULL) AS fetch_errors
FROM latest_articles
GROUP BY ingest_date, source_id
ORDER BY ingest_date DESC, source_id;

-- 3. Physical duplicate and version audit across all retained bronze rows.
WITH all_rows AS (
  SELECT source_id, source_record_id, source_version, ingested_at
  FROM `your-gcp-project.bronze.source_record`
  WHERE source_id IN (
    'SRC_BBC_SWAHILI', 'SRC_BBC_ARABIC', 'SRC_HESPRESS',
    'SRC_ALLAFRICA_FR', 'SRC_AFRICANEWS_FR', 'SRC_FINANCIAL_AFRIK'
  )
  AND ingested_at >= audit_start
),
source_totals AS (
  SELECT
    source_id,
    COUNT(*) AS physical_rows,
    COUNT(DISTINCT source_record_id) AS distinct_articles,
    COUNT(DISTINCT FORMAT('%s|%s|%s', source_id, source_record_id, source_version)) AS distinct_versions,
    COUNT(*) - COUNT(DISTINCT FORMAT('%s|%s|%s', source_id, source_record_id, source_version)) AS exact_duplicate_rows
  FROM all_rows
  GROUP BY source_id
),
article_versions AS (
  SELECT source_id, source_record_id, COUNT(DISTINCT source_version) AS version_count
  FROM all_rows
  GROUP BY source_id, source_record_id
)
SELECT
  t.source_id,
  t.physical_rows,
  t.distinct_articles,
  t.distinct_versions,
  t.exact_duplicate_rows,
  COUNTIF(v.version_count > 1) AS articles_with_revisions,
  MAX(v.version_count) AS max_versions_for_one_article
FROM source_totals t
JOIN article_versions v USING (source_id)
GROUP BY t.source_id, t.physical_rows, t.distinct_articles, t.distinct_versions, t.exact_duplicate_rows
ORDER BY t.source_id;

-- 4. Date anomalies. published_at may be NULL, but future dates or published-after-ingested
-- timestamps indicate parser/source problems.
SELECT
  source_id, source_record_id, title, published_at_raw, ingested_at,
  CASE
    WHEN published_at_raw IS NOT NULL AND published_at IS NULL THEN 'UNPARSEABLE_DATE'
    WHEN published_at > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) THEN 'FUTURE_DATE'
    WHEN published_at > TIMESTAMP_ADD(ingested_at, INTERVAL 1 DAY) THEN 'PUBLISHED_AFTER_INGEST'
    WHEN published_at < TIMESTAMP_SUB(ingested_at, INTERVAL 10 DAY) THEN 'STALE_AT_INGEST'
  END AS anomaly
FROM latest_articles
WHERE (published_at_raw IS NOT NULL AND published_at IS NULL)
   OR published_at > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
   OR published_at > TIMESTAMP_ADD(ingested_at, INTERVAL 1 DAY)
   OR published_at < TIMESTAMP_SUB(ingested_at, INTERVAL 10 DAY)
ORDER BY ingested_at DESC;

-- 5. Suspected text extraction failures/boilerplate. Review every returned row.
SELECT
  source_id, source_record_id, title, canonical_url, body_source, word_count,
  fetch_error, LEFT(body_text, 500) AS body_preview
FROM latest_articles
WHERE body_text IS NULL
   OR word_count < 100
   OR body_source = 'feed_summary'
   OR fetch_error IS NOT NULL
   OR REGEXP_CONTAINS(LOWER(body_text), r'(cookie policy|privacy policy|javascript|abonnez-vous|connectez-vous|tous droits réservés)')
ORDER BY source_id, word_count;

-- 6. Language exceptions. Short texts can confuse detection, so review rather than
-- automatically reject mismatches.
SELECT
  source_id, source_record_id, title, canonical_url, word_count,
  declared_language, detected_language, language_confidence,
  LEFT(body_text, 500) AS body_preview
FROM latest_articles
WHERE declared_language != detected_language
   OR language_confidence < 0.80
   OR detected_language IS NULL
ORDER BY language_confidence, source_id;

-- 7. Country-assignment distribution. Hespress should be MAR/source_default;
-- other sources may legitimately have NULL because they are regional/global.
SELECT
  source_id,
  COALESCE(country_iso3, 'NULL') AS country_iso3,
  country_method,
  COUNT(*) AS articles
FROM latest_articles
GROUP BY source_id, country_iso3, country_method
ORDER BY source_id, articles DESC;

-- 8. Human-review sample: 10 deterministic articles per source, deliberately mixed
-- across assigned/unassigned country and body-source strata. A reviewer should compare
-- title, body, language, country and canonical URL against the publisher page.
SELECT
  source_id, source_record_id, title, canonical_url, published_at,
  body_source, word_count, declared_language, detected_language,
  language_confidence, country_iso3, country_method,
  LEFT(body_text, 1000) AS body_preview
FROM latest_articles
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY source_id
  ORDER BY FARM_FINGERPRINT(CONCAT(source_record_id, COALESCE(country_iso3, 'NULL'), body_source))
) <= 10
ORDER BY source_id, source_record_id;
