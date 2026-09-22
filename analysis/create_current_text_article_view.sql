-- One current, usable full-text article per stable document identity.
-- This view is the handoff contract for country tagging and sentiment.
-- Bronze remains append-only and retains valid publisher revisions.
--
-- The fixed-string replacements are a compatibility cleanup for BBC rows
-- captured before HTML container exclusion was deployed on 2026-09-22.
-- New rows are cleaned from the DOM before their source_version is hashed.

CREATE OR REPLACE VIEW `your-gcp-project.bronze.current_text_article` AS
WITH source_rows AS (
  SELECT
    source_id,
    COALESCE(
      JSON_VALUE(payload, '$.article.document_id'),
      LOWER(TO_HEX(SHA256(CONCAT(
        source_id, '\n',
        COALESCE(JSON_VALUE(payload, '$.article.canonical_url'),
                 source_record_id)
      ))))
    ) AS document_id,
    source_record_id,
    source_version,
    JSON_VALUE(payload, '$.source.name') AS source_name,
    JSON_VALUE(payload, '$.article.canonical_url') AS canonical_url,
    JSON_VALUE(payload, '$.article.title') AS title,
    JSON_VALUE(payload, '$.article.body_text') AS raw_body_text,
    JSON_VALUE(payload, '$.article.body_source') AS body_source,
    SAFE_CAST(JSON_VALUE(payload, '$.dates.published_at_utc') AS TIMESTAMP)
      AS published_at,
    JSON_VALUE(payload, '$.language.declared_iso639_3') AS language_code,
    JSON_VALUE(payload, '$.language.script_iso15924') AS script_code,
    JSON_VALUE(payload, '$.language.detected_iso639_3') AS detected_language_code,
    SAFE_CAST(JSON_VALUE(payload, '$.language.detection_confidence') AS FLOAT64)
      AS language_confidence,
    JSON_VALUE(payload, '$.fetch.content_sha256') AS source_content_hash_sha256,
    COALESCE(
      JSON_VALUE(payload, '$.fetch.normalized_content_sha256'),
      source_version
    ) AS source_normalized_content_hash_sha256,
    ingested_at,
    payload
  FROM `your-gcp-project.bronze.source_record`
  -- Any source written by etl/sentiment_ingestion (payload schema 1.x), so new
  -- sources appear here without editing this view.
  WHERE JSON_VALUE(payload, '$.source.source_id') = source_id
    AND JSON_VALUE(payload, '$.schema_version') LIKE '1.%'
    AND JSON_VALUE(payload, '$.article.body_text') IS NOT NULL
    AND JSON_VALUE(payload, '$.article.body_source') != 'feed_summary'
    AND NOT (
      source_id = 'SRC_BBC_SWAHILI'
      AND JSON_VALUE(payload, '$.article.canonical_url')
          LIKE '%/bbc_swahili_radio/%'
    )
    AND NOT (
      source_id = 'SRC_HESPRESS'
      AND EXISTS (
        SELECT 1
        FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.feed_entry.tags')) AS tag
        WHERE JSON_VALUE(tag) IN ('صوت وصورة', 'كاريكاتير اليوم')
      )
    )
),
cleaned AS (
  SELECT
    * EXCEPT(raw_body_text),
    TRIM(REGEXP_REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              raw_body_text,
              'الأكثر قراءة نهاية', ''
            ),
            'شرح معمق لأبرز الأحداث والموضوعات، لمساعدتك على فهم أهم المتغيرات حولك وأثرها على حياتك الحلقات البودكاست نهاية',
            ''
          ),
          'End of Iliyosomwa zaidi', ''
        ),
        'Pata habari za kina kutoka BBC News Swahili, moja kwa moja kupitia WhatsApp. Bonyeza hapa kujiunga Mwisho wa Whatsapp',
        ''
      ),
      r'\s+', ' '
    )) AS body_text
  FROM source_rows
),
eligible AS (
  SELECT
    * EXCEPT(body_text),
    body_text,
    ARRAY_LENGTH(SPLIT(body_text, ' ')) AS word_count,
    LOWER(TO_HEX(SHA256(body_text))) AS served_content_hash_sha256,
    CAST(NULL AS STRING) AS country_iso3
  FROM cleaned
  WHERE ARRAY_LENGTH(SPLIT(body_text, ' ')) >= 80
)
SELECT * EXCEPT(revision_rank)
FROM (
  SELECT
    eligible.*,
    ROW_NUMBER() OVER (
      PARTITION BY source_id, document_id
      ORDER BY ingested_at DESC, source_version DESC
    ) AS revision_rank
  FROM eligible
)
WHERE revision_rank = 1;

