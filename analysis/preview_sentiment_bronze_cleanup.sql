-- Read-only preview for cleanup_sentiment_bronze.sql.
WITH candidates AS (
  SELECT
    CASE
      WHEN source_id = 'SRC_FINANCIAL_AFRIK'
        THEN 'source_removed_teasers'
      WHEN source_id = 'SRC_BBC_SWAHILI'
       AND JSON_VALUE(payload, '$.article.canonical_url')
           LIKE '%/bbc_swahili_radio/%'
        THEN 'radio_program'
      WHEN source_id = 'SRC_HESPRESS'
       AND EXISTS (
         SELECT 1
         FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.feed_entry.tags')) AS tag
         WHERE JSON_VALUE(tag) IN ('صوت وصورة', 'كاريكاتير اليوم')
       )
        THEN 'hespress_non_text'
      WHEN JSON_VALUE(payload, '$.article.body_source') = 'feed_summary'
        THEN 'summary_only'
      WHEN SAFE_CAST(JSON_VALUE(payload, '$.article.word_count') AS INT64) < 80
        THEN 'too_short_for_sentiment'
      ELSE NULL
    END AS deletion_reason
  FROM `your-gcp-project.bronze.source_record`
  WHERE source_id IN (
    'SRC_BBC_SWAHILI', 'SRC_BBC_ARABIC', 'SRC_HESPRESS',
    'SRC_ALLAFRICA_FR', 'SRC_AFRICANEWS_FR', 'SRC_FINANCIAL_AFRIK'
  )
)
SELECT deletion_reason, COUNT(*) AS rows_to_delete
FROM candidates
WHERE deletion_reason IS NOT NULL
GROUP BY deletion_reason
ORDER BY rows_to_delete DESC;
