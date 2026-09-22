# Sentiment News Ingestion Audit — 2026-09-22

Scope: latest version of each article ingested during the prior 30 days for the six
Arabic, French, and Swahili sources in `your-gcp-project.bronze.source_record`.

## Verdict

- Operational health: PASS. Latest data was two hours old and the ten most recent
  scheduled Cloud Run executions all succeeded.
- Bronze contract integrity: PASS. No missing titles/bodies, schema-version errors,
  exact duplicate versions, recomputed content-hash failures, or character-count
  mismatches.
- Language metadata: PASS. No declared/detected mismatches or confidence below 0.80.
- Full-article requirement: PARTIAL FAIL. Financial Afrik, Hespress fallbacks, and
  BBC Swahili radio entries do not consistently contain full news articles.
- Country semantics: NEEDS CONTRACT DECISION. Current values mix subject-country
  inference with publisher-country defaulting.

## Remediation completed on 2026-09-22

- Removed Financial Afrik from production because its public text was a truncated
  opening excerpt rather than a complete article.
- Deleted 657 invalid bronze rows: 388 Financial Afrik teasers, 159 Hespress
  video/cartoon entries, 66 BBC Swahili radio programs, 43 other bodies below
  80 words, and one summary-only record.
- Removed every deterministic country assignment from ingestion. New rows have
  `country_iso3 = NULL` and no `payload.country`. The current view also exposes
  country as NULL so downstream models cannot mistake publisher origin for the
  subject of sentiment.
- Added pre-fetch tag/URL exclusions, an 80-word full-text gate, no RSS-summary
  fallback, Hespress footer removal, and BBC HTML-container exclusions for
  recommendation/podcast modules and image-credit figures.
- Added stable `document_id`, exact `source_version`, and normalized lexical
  content fingerprint. Formatting-only changes are skipped; material publisher
  revisions remain append-only in bronze.
- Created `bronze.current_text_article`, which selects one latest valid
  revision per stable document and removes fixed legacy BBC UI strings from the
  served body without mutating raw bronze revisions.
- Deployed immutable image
  `sentiment-ingestion:<BUILD_ID>` to the Cloud Run
  job running as the pipeline's service account. The six-hour UTC schedule remains enabled.
- Post-cleanup current-view counts: Africanews 539, AllAfrica 1,465, BBC Arabic
  352, BBC Swahili 199, and Hespress 1,268. All have at least 80 words and zero
  country assignments.
- A normal validation cycle succeeded, skipped 117 unchanged feed records before
  page download, rejected/excluded the configured non-articles, and inserted zero
  duplicate rows.

## 30-day scorecard (latest article versions)

| Source | Articles | Country coverage | Median words | Under 100 words | Fallbacks/errors |
|---|---:|---:|---:|---:|---:|
| Africanews FR | 544 | 74.4% | 296 | 16 | 1 |
| AllAfrica FR | 1,459 | 97.1% | 434 | 7 | 0 |
| BBC Arabic | 368 | 26.9% | 864 | 19 | 0 |
| BBC Swahili | 274 | 36.5% | 874 | 66 | 0 |
| Financial Afrik | 388 | 87.1% | 56 | 225 | 12 |
| Hespress | 1,429 | 100% | 329 | 229 | 113 |

Total latest articles: 4,462.
