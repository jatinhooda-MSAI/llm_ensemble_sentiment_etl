-- Sentiment scoring: views (computed, nothing stored twice).
-- Rule parameters below are deliberate constants; change them here and
-- re-run this file:
--   random audit of unanimous items ............ 5%
--   second human opinion ........................ 20% of human-labeled items
--   max humans per item ......................... 2

-- 1. Every article the scorers should read, in one shape, from every family:
--      - our news ingestion (bronze.current_text_article)
--      - whatever other pipelines load into the existing silver.document table
--    New families: add a UNION ALL branch, or load into silver.document.
--    Same quality rule for all: at least 80 words of text. Empty rights mean
--    "use everything"; only an explicit PROHIBITED is left out.
--    source_weight is 1.0 for every outlet until the registry carries weights.
CREATE OR REPLACE VIEW `your-gcp-project.silver.v_document` AS
WITH family_news AS (
  SELECT
    document_id AS doc_id, source_version, source_id, canonical_url, title,
    body_text, word_count, language_code AS language_iso639_3,
    published_at, ingested_at AS fetched_at,
    CAST(NULL AS STRING) AS rights_class
  FROM `your-gcp-project.bronze.current_text_article`
),
family_silver_table AS (
  SELECT
    doc_id,
    COALESCE(content_hash, vintage_id, TO_HEX(SHA256(IFNULL(content, '')))) AS source_version,
    source_id, COALESCE(canonical_url, url) AS canonical_url, title,
    content AS body_text,
    ARRAY_LENGTH(SPLIT(content, ' ')) AS word_count,
    language_code AS language_iso639_3,
    TIMESTAMP(published_date) AS published_at, silver_loaded_at AS fetched_at,
    rights_class
  FROM `your-gcp-project.silver.document`
  WHERE content IS NOT NULL
    AND UPPER(COALESCE(deletion_status, 'ACTIVE')) = 'ACTIVE'
    AND source_id NOT IN (SELECT DISTINCT source_id FROM family_news)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY silver_loaded_at DESC) = 1
)
SELECT *, 1.0 AS source_weight
FROM (
  SELECT * FROM family_news
  UNION ALL
  SELECT * FROM family_silver_table
)
WHERE word_count >= 80
  AND UPPER(COALESCE(rights_class, '')) != 'PROHIBITED';
