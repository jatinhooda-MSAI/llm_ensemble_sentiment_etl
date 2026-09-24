-- Post-deployment checks for the multilingual full-text ingestion pipeline.

-- 1. Invalid historical records must be gone.
SELECT
  COUNTIF(source_id = 'SRC_FINANCIAL_AFRIK') AS financial_afrik_rows,
  COUNTIF(
    source_id = 'SRC_BBC_SWAHILI'
    AND JSON_VALUE(payload, '$.article.canonical_url')
        LIKE '%/bbc_swahili_radio/%'
  ) AS radio_rows,
  COUNTIF(JSON_VALUE(payload, '$.article.body_source') = 'feed_summary')
    AS summary_only_rows,
  COUNTIF(
    source_id IN (
      'SRC_BBC_SWAHILI', 'SRC_BBC_ARABIC', 'SRC_HESPRESS',
      'SRC_ALLAFRICA_FR', 'SRC_AFRICANEWS_FR'
    )
    AND SAFE_CAST(JSON_VALUE(payload, '$.article.word_count') AS INT64) < 80
  ) AS short_rows
FROM `your-gcp-project.bronze.source_record`;

-- 2. Current-view coverage and minimum text length.
SELECT
  source_id,
  COUNT(*) AS article_count,
  MIN(word_count) AS min_words,
  APPROX_QUANTILES(word_count, 100)[OFFSET(50)] AS median_words,
  MAX(word_count) AS max_words,
  COUNTIF(country_iso3 IS NOT NULL) AS assigned_country_rows
FROM `your-gcp-project.bronze.current_text_article`
GROUP BY source_id
ORDER BY source_id;

-- 3. New-contract rows from the forced refresh.
SELECT
  source_id,
  COUNT(*) AS row_count,
  COUNTIF(country_iso3 IS NOT NULL) AS assigned_country_rows,
  COUNTIF(JSON_VALUE(payload, '$.country') IS NOT NULL) AS payload_country_rows,
  COUNTIF(JSON_VALUE(payload, '$.article.document_id') IS NULL)
    AS missing_document_id,
  COUNTIF(JSON_VALUE(payload, '$.fetch.normalized_content_sha256') IS NULL)
    AS missing_normalized_hash,
  COUNTIF(JSON_VALUE(payload, '$.schema_version') != '1.1.0')
    AS wrong_schema_version,
  COUNTIF(
    source_id = 'SRC_BBC_ARABIC'
    AND (
      CONTAINS_SUBSTR(JSON_VALUE(payload, '$.article.body_text'), 'الأكثر قراءة نهاية')
      OR CONTAINS_SUBSTR(
        JSON_VALUE(payload, '$.article.body_text'),
        'شرح معمق لأبرز الأحداث والموضوعات، لمساعدتك على فهم أهم المتغيرات حولك وأثرها على حياتك الحلقات البودكاست نهاية'
      )
    )
  ) AS bbc_arabic_boilerplate,
  COUNTIF(
    source_id = 'SRC_BBC_SWAHILI'
    AND CONTAINS_SUBSTR(
      JSON_VALUE(payload, '$.article.body_text'), 'End of Iliyosomwa zaidi'
    )
  ) AS bbc_swahili_most_read_marker
FROM `your-gcp-project.bronze.source_record`
WHERE ingested_at >= TIMESTAMP('2026-09-23T02:28:50Z')
GROUP BY source_id
ORDER BY source_id;

-- 4. Boilerplate still visible through the current view (usually older rows
-- not covered by the two-day forced refresh).
SELECT
  source_id,
  COUNTIF(
    source_id = 'SRC_BBC_ARABIC'
    AND (
      CONTAINS_SUBSTR(body_text, 'الأكثر قراءة نهاية')
      OR CONTAINS_SUBSTR(
        body_text,
        'شرح معمق لأبرز الأحداث والموضوعات، لمساعدتك على فهم أهم المتغيرات حولك وأثرها على حياتك الحلقات البودكاست نهاية'
      )
    )
  ) AS arabic_boilerplate_rows,
  COUNTIF(
    source_id = 'SRC_BBC_SWAHILI'
    AND CONTAINS_SUBSTR(body_text, 'End of Iliyosomwa zaidi')
  ) AS swahili_most_read_rows
FROM `your-gcp-project.bronze.current_text_article`
WHERE source_id IN ('SRC_BBC_ARABIC', 'SRC_BBC_SWAHILI')
GROUP BY source_id
ORDER BY source_id;
