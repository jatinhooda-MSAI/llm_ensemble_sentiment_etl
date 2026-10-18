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

-- 2. The final answer per (article, country), from the active models and any
--    humans. Always returns something usable:
--      UNANIMOUS            all active models tagged it with the same label
--      HUMAN                one human labeled it
--      HUMAN_PAIR_AGREED    two humans agreed
--      HUMAN_PAIR_CONTESTED two humans disagreed -> model majority, flagged
--      PROVISIONAL          models disagree, no human yet -> model majority
CREATE OR REPLACE VIEW `your-gcp-project.silver.sentiment_consensus` AS
WITH active_models AS (
  SELECT model_version FROM (
    SELECT model_version, status,
           ROW_NUMBER() OVER (PARTITION BY model_version ORDER BY recorded_at DESC) AS rn
    FROM `your-gcp-project.gold.dim_model_version`
    WHERE task = 'TAG_AND_SENTIMENT'
  ) WHERE rn = 1 AND status = 'ACTIVE'
),
consensus_meta AS (
  SELECT COUNT(*) AS n_active,
         TO_HEX(SHA256(STRING_AGG(model_version, ',' ORDER BY model_version))) AS consensus_version
  FROM active_models
),
model_rows AS (  -- latest output per (article version, model, country)
  SELECT * EXCEPT (rn) FROM (
    SELECT l.*,
           ROW_NUMBER() OVER (
             PARTITION BY doc_id, source_version, labeler_id, IFNULL(country_iso3, '')
             ORDER BY created_at DESC) AS rn
    FROM `your-gcp-project.silver.sentiment_label` AS l
    JOIN active_models AS m ON l.labeler_id = m.model_version
    WHERE l.labeler_type = 'MODEL'
  ) WHERE rn = 1
),
processed AS (  -- how many active models have answered each article version
  -- A model whose only results are errors counts once its retry has also failed
  -- (2 errors); until then the article waits instead of going to a human.
  SELECT doc_id, source_version,
         COUNT(DISTINCT IF(n_ok > 0 OR n_err >= 2, labeler_id, NULL)) AS n_models_processed,
         ANY_VALUE(published_date) AS published_date
  FROM (
    SELECT doc_id, source_version, labeler_id, ANY_VALUE(published_date) AS published_date,
           COUNTIF(tag_status != 'ERROR') AS n_ok,
           (SELECT COUNTIF(e.tag_status = 'ERROR')
            FROM `your-gcp-project.silver.sentiment_label` AS e
            WHERE e.doc_id = m.doc_id AND e.source_version = m.source_version
              AND e.labeler_id = m.labeler_id) AS n_err
    FROM model_rows AS m
    GROUP BY doc_id, source_version, labeler_id
  )
  GROUP BY doc_id, source_version
),
candidates AS (  -- every country any active model tagged
  SELECT doc_id, source_version, country_iso3,
         COUNT(DISTINCT labeler_id) AS n_models_tagged,
         COUNT(DISTINCT sentiment_label) AS n_distinct_labels,
         ARRAY_AGG(STRUCT(labeler_id AS model_version, sentiment_label AS label,
                          sentiment_score AS score) ORDER BY labeler_id) AS model_labels
  FROM model_rows
  WHERE tag_status = 'TAGGED' AND country_iso3 IS NOT NULL
  GROUP BY doc_id, source_version, country_iso3
),
model_majority AS (
  SELECT doc_id, source_version, country_iso3,
         ARRAY_AGG(STRUCT(sentiment_label AS label, n) ORDER BY n DESC LIMIT 2) AS top2
  FROM (
    SELECT doc_id, source_version, country_iso3, sentiment_label, COUNT(*) AS n
    FROM model_rows WHERE tag_status = 'TAGGED' AND country_iso3 IS NOT NULL
    GROUP BY doc_id, source_version, country_iso3, sentiment_label
  ) GROUP BY doc_id, source_version, country_iso3
),
human_rows AS (  -- latest non-skip answer per (article version, country, human)
  SELECT * EXCEPT (rn) FROM (
    SELECT l.*,
           ROW_NUMBER() OVER (
             PARTITION BY doc_id, source_version, country_iso3, labeler_id
             ORDER BY created_at DESC) AS rn
    FROM `your-gcp-project.silver.sentiment_label` AS l
    WHERE l.labeler_type = 'HUMAN' AND NOT IFNULL(l.is_skip, FALSE)
  ) WHERE rn = 1
),
humans AS (
  SELECT doc_id, source_version, country_iso3,
         COUNT(*) AS n_humans,
         ARRAY_AGG(labeler_id ORDER BY created_at) AS labeled_by,
         ARRAY_AGG(STRUCT(labeler_id, is_about_country, sentiment_label AS label)
                   ORDER BY created_at) AS human_labels,
         COUNT(DISTINCT CONCAT(CAST(is_about_country AS STRING), '|',
                               IFNULL(sentiment_label, ''))) AS n_distinct_answers
  FROM human_rows GROUP BY doc_id, source_version, country_iso3
),
joined AS (
  SELECT
    c.doc_id, c.source_version, c.country_iso3, p.published_date,
    cm.consensus_version, cm.n_active,
    p.n_models_processed, c.n_models_tagged, c.model_labels,
    (p.n_models_processed = cm.n_active) AS models_complete,
    (p.n_models_processed = cm.n_active AND c.n_models_tagged = cm.n_active
       AND c.n_distinct_labels = 1) AS unanimous,
    -- model majority: tagged by most models that answered, and a label with a strict lead
    (c.n_models_tagged * 2 > p.n_models_processed) AS majority_about,
    IF(ARRAY_LENGTH(mm.top2) = 1 OR mm.top2[OFFSET(0)].n > mm.top2[OFFSET(1)].n,
       mm.top2[OFFSET(0)].label, 'UNCERTAIN') AS majority_label,
    IFNULL(h.n_humans, 0) AS n_humans, h.labeled_by, h.human_labels,
    IFNULL(h.n_distinct_answers, 0) AS n_distinct_answers,
    MOD(ABS(FARM_FINGERPRINT(CONCAT(c.doc_id, '|', c.country_iso3, '|audit'))), 100) AS audit_bucket,
    MOD(ABS(FARM_FINGERPRINT(CONCAT(c.doc_id, '|', c.country_iso3, '|second'))), 100) AS second_bucket
  FROM candidates AS c
  JOIN processed AS p USING (doc_id, source_version)
  JOIN model_majority AS mm USING (doc_id, source_version, country_iso3)
  CROSS JOIN consensus_meta AS cm
  LEFT JOIN humans AS h USING (doc_id, source_version, country_iso3)
),
decided AS (
  SELECT
    *,
    (unanimous AND audit_bucket < 5) AS audit_selected,
    (models_complete AND (NOT unanimous OR audit_bucket < 5)) AS needs_human,
    CASE
      WHEN n_humans = 0 AND unanimous THEN 'UNANIMOUS'
      WHEN n_humans = 0 THEN 'PROVISIONAL'
      WHEN n_distinct_answers = 1 AND n_humans = 1 THEN 'HUMAN'
      WHEN n_distinct_answers = 1 THEN 'HUMAN_PAIR_AGREED'
      ELSE 'HUMAN_PAIR_CONTESTED'
    END AS method
  FROM joined
)
SELECT
  doc_id, source_version, country_iso3, published_date, consensus_version, method,
  CASE
    WHEN method IN ('HUMAN', 'HUMAN_PAIR_AGREED') THEN human_labels[OFFSET(0)].is_about_country
    WHEN method = 'UNANIMOUS' THEN TRUE
    ELSE majority_about
  END AS is_about_country,
  CASE
    WHEN method IN ('HUMAN', 'HUMAN_PAIR_AGREED') THEN human_labels[OFFSET(0)].label
    WHEN method = 'UNANIMOUS' THEN model_labels[OFFSET(0)].label
    ELSE majority_label
  END AS final_label,
  needs_human, audit_selected,
  IF(needs_human, IF(second_bucket < 20, 2, 1), 0) AS humans_needed,
  n_humans, labeled_by, human_labels,
  models_complete, n_active, n_models_processed, n_models_tagged, unanimous, model_labels
FROM decided;
