-- Rebuild the daily country sentiment series for the trailing window.
-- Derived data, so the window is replaced for this aggregation_version.
-- Parameters: @window_days INT64, @aggregation_version STRING,
--             @min_docs INT64, @min_outlets INT64
--
-- Method:
--   value per article: POSITIVE +1, NEGATIVE -1, NEUTRAL 0, MIXED 0;
--                      UNCERTAIN is counted but left out of the score.
--   weight per article: source_weight / (articles from that outlet in the cell),
--                       so every outlet gets one voice per country-day.
--   score_0_100 = 50 + 50 x weighted mean.
--   range: +/- 1.96 standard errors, with a variance floor of 0.25 so a
--          handful of agreeing articles never looks certain.
--   coverage is flagged, never hidden; bias results are flagged, never hidden.

DELETE FROM `your-gcp-project.gold.fact_sentiment_signal`
WHERE aggregation_version = @aggregation_version
  AND period_start >= DATE_SUB(CURRENT_DATE(), INTERVAL @window_days DAY);

INSERT INTO `your-gcp-project.gold.fact_sentiment_signal` (
  country_iso3, sector_code, narrative_code, language_code, period_start, period_grain,
  aggregation_version, score_0_100, score_low, score_high, coverage_confidence,
  coverage_flag, bias_flag, n_docs, n_outlets, n_languages, n_human_resolved,
  n_provisional, share_positive, share_negative, share_neutral, share_mixed,
  n_uncertain, velocity, intensity, representative_doc_ids, data_as_of, computed_at)
WITH articles AS (
  SELECT
    c.country_iso3, c.doc_id, c.final_label, c.method,
    d.source_id, d.language_iso639_3, d.source_weight, d.published_at,
    DATE(d.published_at) AS period_start,
    CASE c.final_label WHEN 'POSITIVE' THEN 1.0 WHEN 'NEGATIVE' THEN -1.0
                       WHEN 'NEUTRAL' THEN 0.0 WHEN 'MIXED' THEN 0.0 END AS x
  FROM `your-gcp-project.silver.sentiment_consensus` AS c
  JOIN `your-gcp-project.silver.v_document` AS d USING (doc_id, source_version)
  WHERE c.is_about_country
    AND c.final_label IS NOT NULL
    AND DATE(d.published_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL @window_days + 7 DAY)
),
cells AS (  -- each article counts in its own language cell and in ALL
  SELECT a.*, lang AS language_code
  FROM articles AS a, UNNEST([a.language_iso639_3, 'ALL']) AS lang
),
weighted AS (
  SELECT *,
         IF(x IS NULL, NULL,
            source_weight / COUNTIF(x IS NOT NULL) OVER (
              PARTITION BY country_iso3, language_code, period_start, source_id)) AS w
  FROM cells
),
moments AS (
  SELECT *,
         SUM(w * x) OVER cell / NULLIF(SUM(w) OVER cell, 0) AS cell_mean
  FROM weighted
  WINDOW cell AS (PARTITION BY country_iso3, language_code, period_start)
),
daily AS (
  SELECT
    country_iso3, language_code, period_start,
    SUM(w * x) / NULLIF(SUM(w), 0) AS mean_x,
    SUM(w * POW(x - cell_mean, 2)) / NULLIF(SUM(w), 0) AS var_x,
    POW(SUM(w), 2) / NULLIF(SUM(w * w), 0) AS n_eff,
    COUNT(DISTINCT doc_id) AS n_docs,
    COUNT(DISTINCT source_id) AS n_outlets,
    COUNT(DISTINCT language_iso639_3) AS n_languages,
    COUNT(DISTINCT IF(method LIKE 'HUMAN%', doc_id, NULL)) AS n_human_resolved,
    COUNT(DISTINCT IF(method = 'PROVISIONAL', doc_id, NULL)) AS n_provisional,
    COUNTIF(final_label = 'UNCERTAIN') AS n_uncertain,
    COUNTIF(x IS NOT NULL) AS n_scored,
    COUNTIF(final_label = 'POSITIVE') AS n_pos,
    COUNTIF(final_label = 'NEGATIVE') AS n_neg,
    COUNTIF(final_label = 'NEUTRAL') AS n_neu,
    COUNTIF(final_label = 'MIXED') AS n_mix,
    ARRAY_AGG(doc_id ORDER BY ABS(IFNULL(x, 0)) DESC, published_at DESC LIMIT 20) AS representative_doc_ids,
    MAX(published_at) AS data_as_of
  FROM moments
  GROUP BY country_iso3, language_code, period_start
),
scored AS (
  SELECT
    *,
    50 + 50 * mean_x AS score,
    1.96 * 50 * SQRT(GREATEST(IFNULL(var_x, 0), 0.25) / NULLIF(n_eff, 0)) AS half_range
  FROM daily
),
with_velocity AS (
  SELECT
    *,
    score - AVG(score) OVER (
      PARTITION BY country_iso3, language_code ORDER BY UNIX_DATE(period_start)
      RANGE BETWEEN 7 PRECEDING AND 1 PRECEDING) AS velocity
  FROM scored
),
bias AS (  -- latest country-level bias result, if any
  SELECT segment_key AS country_iso3, passed
  FROM `your-gcp-project.gold.model_evaluation_slice`
  WHERE evaluation_type = 'BIAS' AND segment_type = 'COUNTRY' AND metric_name = 'kl_divergence'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_key ORDER BY computed_at DESC) = 1
)
SELECT
  v.country_iso3, 'ALL', 'ALL', v.language_code, v.period_start, 'DAY',
  @aggregation_version,
  v.score,
  GREATEST(0, v.score - v.half_range),
  LEAST(100, v.score + v.half_range),
  LEAST(1, v.n_docs / 10) * LEAST(1, v.n_outlets / 2),
  IF(v.n_docs >= @min_docs AND v.n_outlets >= @min_outlets, 'OK', 'LOW'),
  CASE WHEN b.passed IS NULL THEN 'NOT_CHECKED' WHEN b.passed THEN 'PASS' ELSE 'FAIL' END,
  v.n_docs, v.n_outlets, v.n_languages, v.n_human_resolved, v.n_provisional,
  SAFE_DIVIDE(v.n_pos, v.n_scored), SAFE_DIVIDE(v.n_neg, v.n_scored),
  SAFE_DIVIDE(v.n_neu, v.n_scored), SAFE_DIVIDE(v.n_mix, v.n_scored),
  v.n_uncertain,
  v.velocity,
  SAFE_DIVIDE(v.n_pos + v.n_neg, v.n_scored),
  v.representative_doc_ids,
  v.data_as_of,
  CURRENT_TIMESTAMP()
FROM with_velocity AS v
LEFT JOIN bias AS b USING (country_iso3)
WHERE v.period_start >= DATE_SUB(CURRENT_DATE(), INTERVAL @window_days DAY);
