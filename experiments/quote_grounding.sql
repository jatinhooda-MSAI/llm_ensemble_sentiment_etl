-- Share of each model's quotes found in the article (body only, and body or
-- title), and the share of tags that come with no quote at all. Run on
-- 2026-10-25 (before the Gemma fix); after the fix the Gemma rows were split
-- into the old and new version.
WITH latest AS (
  SELECT CASE WHEN labeler_id LIKE 'claude%' THEN 'claude' WHEN labeler_id LIKE 'gemini%' THEN 'gemini' ELSE 'gemma' END AS m,
         doc_id, source_version, country_iso3, tag_status, evidence
  FROM `your-gcp-project.silver.sentiment_label` WHERE labeler_type = 'MODEL'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY labeler_id, doc_id, source_version, IFNULL(country_iso3,'') ORDER BY created_at DESC) = 1),
docs AS (SELECT doc_id, source_version FROM latest GROUP BY 1,2
         HAVING COUNT(DISTINCT IF(tag_status != 'ERROR', m, NULL)) = 3 AND COUNTIF(tag_status='ERROR') = 0),
t AS (SELECT l.*, COUNT(*) OVER (PARTITION BY l.doc_id, l.source_version, l.country_iso3) AS n_models
      FROM latest l JOIN docs USING (doc_id, source_version) WHERE tag_status = 'TAGGED'),
q AS (SELECT t.m, t.n_models, e,
        STRPOS(LOWER(REGEXP_REPLACE(v.body_text, r'\s+', ' ')), LOWER(TRIM(REGEXP_REPLACE(e, r'\s+', ' ')))) > 0 AS in_body,
        STRPOS(LOWER(REGEXP_REPLACE(IFNULL(v.title,''), r'\s+', ' ')), LOWER(TRIM(REGEXP_REPLACE(e, r'\s+', ' ')))) > 0 AS in_title
      FROM t JOIN `your-gcp-project.silver.v_document` v USING (doc_id, source_version), UNNEST(t.evidence) e
      WHERE LENGTH(TRIM(e)) > 0)
SELECT 'quotes' AS what, m, COUNT(*) n,
       ROUND(COUNTIF(in_body)/COUNT(*),3) body_only,
       ROUND(COUNTIF(in_body OR in_title)/COUNT(*),3) body_or_title,
       ROUND(COUNTIF(n_models=1 AND (in_body OR in_title))/NULLIF(COUNTIF(n_models=1),0),3) solo_tags_grounded
FROM q GROUP BY m
UNION ALL
-- for 'no_quote' rows: body_only = share of all tags with no quote,
-- body_or_title = share of solo tags (only this model) with no quote
SELECT 'no_quote', m, COUNT(*), ROUND(COUNTIF(ARRAY_LENGTH(evidence)=0)/COUNT(*),3),
       ROUND(COUNTIF(n_models=1 AND ARRAY_LENGTH(evidence)=0)/NULLIF(COUNTIF(n_models=1),0),3), NULL
FROM t GROUP BY m
ORDER BY what, m
