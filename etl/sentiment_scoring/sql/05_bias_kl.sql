-- Bias check 1: does the same country get a different label mix depending on
-- the language of the coverage? For each country with at least two languages
-- that each have @min_per_language articles in the trailing @window_days, compare
-- each language's label distribution with the country's pooled distribution:
--   KL(pooled || language), add-1/2 smoothed over the 5 labels.
-- The country's value is the largest per-language KL. Threshold 0.15 (work plan).
-- Results are published; they never hide the series.
-- Parameters: @window_days INT64, @min_per_language INT64, @evaluation_id STRING

INSERT INTO `your-gcp-project.gold.model_evaluation_slice` (
  evaluation_id, evaluation_type, subject_version, eval_set_id, window_start, window_end,
  segment_type, segment_key, metric_name, metric_value, metric_json, n_items,
  threshold, passed, computed_at)
WITH labels AS (
  SELECT c.country_iso3, d.language_iso639_3 AS lang, c.final_label AS label,
         c.consensus_version
  FROM `your-gcp-project.silver.sentiment_consensus` AS c
  JOIN `your-gcp-project.silver.v_document` AS d USING (doc_id, source_version)
  WHERE c.is_about_country AND c.final_label IS NOT NULL
    AND DATE(d.published_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL @window_days DAY)
),
lang_counts AS (
  SELECT country_iso3, lang, COUNT(*) AS n FROM labels GROUP BY 1, 2
),
eligible AS (  -- languages with enough articles, in countries with 2+ such languages
  SELECT country_iso3, lang, n FROM lang_counts
  WHERE n >= @min_per_language
  QUALIFY COUNT(*) OVER (PARTITION BY country_iso3) >= 2
),
grid AS (  -- every (country, language, label) cell, including zeros
  SELECT e.country_iso3, e.lang, e.n, label
  FROM eligible AS e, UNNEST(['POSITIVE', 'NEGATIVE', 'NEUTRAL', 'MIXED', 'UNCERTAIN']) AS label
),
dist AS (
  SELECT g.country_iso3, g.lang, g.label, g.n,
         COUNT(l.label) AS k
  FROM grid AS g
  LEFT JOIN labels AS l
    ON l.country_iso3 = g.country_iso3 AND l.lang = g.lang AND l.label = g.label
  GROUP BY 1, 2, 3, 4
),
probs AS (  -- add-1/2 (Jeffreys) smoothing: small samples with empty label
             -- cells would otherwise make KL explode
  SELECT *,
         (k + 0.5) / (n + 2.5) AS p_lang,
         (SUM(k) OVER (PARTITION BY country_iso3, label) + 0.5)
           / (SUM(k) OVER (PARTITION BY country_iso3) + 2.5) AS p_ref
  FROM dist
),
kl_per_lang AS (
  SELECT country_iso3, lang, ANY_VALUE(n) AS n,
         SUM(p_ref * LN(p_ref / p_lang)) AS kl
  FROM probs GROUP BY 1, 2
)
SELECT
  @evaluation_id, 'BIAS',
  (SELECT ANY_VALUE(consensus_version) FROM labels),
  CAST(NULL AS STRING),
  DATE_SUB(CURRENT_DATE(), INTERVAL @window_days DAY), CURRENT_DATE(),
  'COUNTRY', country_iso3, 'kl_divergence',
  MAX(kl),
  TO_JSON(ARRAY_AGG(STRUCT(lang, n, ROUND(kl, 4) AS kl) ORDER BY kl DESC)),
  SUM(n),
  0.15,
  MAX(kl) < 0.15,
  CURRENT_TIMESTAMP()
FROM kl_per_lang
GROUP BY country_iso3;
