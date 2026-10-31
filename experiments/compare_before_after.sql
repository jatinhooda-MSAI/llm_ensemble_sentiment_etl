-- Agreement metrics before and after the Gemma fix, from the two runs of
-- `run_sentiment_scoring.py agreement` stored in gold.model_evaluation_slice.
WITH e AS (
  SELECT evaluation_id, segment_key, metric_name, metric_value
  FROM `your-gcp-project.gold.model_evaluation_slice`
  WHERE evaluation_id IN ('model_agreement-20261025T232951-e9f17c', 'model_agreement-20261026T042826-87b93d'))
SELECT segment_key, REPLACE(REPLACE(REPLACE(metric_name, '-4-26b-a4b-it-maas', ''), '-sonnet-5-5', ''), '-3.8-flash', '') AS metric,
  ROUND(MAX(IF(evaluation_id LIKE '%e9f17c', metric_value, NULL)), 3) AS before_fix,
  ROUND(MAX(IF(evaluation_id LIKE '%87b93d', metric_value, NULL)), 3) AS after_fix
FROM e
WHERE (segment_key = 'ALL' OR (segment_key IN ('ara','fra','swa','eng') AND metric_name LIKE 'model:gemma%' AND REGEXP_CONTAINS(metric_name, 'extra_tag|grounded|odd_one')))
  AND NOT REGEXP_CONTAINS(metric_name, 'share_mixed|missed_share') OR (segment_key='ALL' AND metric_name LIKE '%gemma%missed%')
GROUP BY 1, 2 ORDER BY 1, 2
