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

## Findings requiring action

1. Financial Afrik does not meet the full-article requirement for most records.
   225/388 records are under 100 words, and the normal page-scrape path has a median
   of 56 words. These are generally previews/paywall teasers.
2. Hespress has 113 failed full-text extractions that fell back to 14–18 word
   WordPress summaries. In addition, all sampled/stored Hespress text includes the
   trailing `The post … appeared first on Hespress` boilerplate, including otherwise
   complete records.
3. BBC Swahili has 66 records under 100 words. Inspection shows these are recurring
   radio-program descriptions (`Amka Na BBC`, `Dira Ya Dunia`), not full news articles.
4. Subject-country semantics are inconsistent. Hespress defaults every unmatched
   story to `MAR`, including stories about US politics, France, and Italian football.
   This is correct only if `country_iso3` means publisher/source country; it is wrong
   if it means the article's primary subject country. Other sources use subject-country
   inference.
5. BBC Swahili has 35 missing publication timestamps and BBC Arabic has 2. No dates
   were unparseable, future-dated, or stale at ingestion. Filtering BBC radio entries
   may remove much of the Swahili gap.
6. Version behavior is append-only and has zero exact duplicate rows. There is notable
   revision churn: Africanews has 127 revised articles; BBC Arabic 62; BBC Swahili 32.
   One BBC Swahili live page accumulated 34 versions, which is plausible for live
   coverage but should not be treated as 34 independent articles downstream.

## Follow-up forensic findings

- Financial Afrik's common 55-word records are leading excerpts, often ending in the
  middle of a sentence or word. They are not complete editorial summaries.
- Recent Hespress extraction failures were non-text feed categories: 98 `صوت وصورة`
  (sound-and-image/video) entries and 13 cartoon entries in the inspected snapshot.
  Their pages contain embeds but no article body. The 14–18-word fallback is merely
  `The post … appeared first on Hespress`, not useful sentiment evidence.
- BBC Swahili's 18-word records are recurring audio-program landing pages under
  `/swahili/bbc_swahili_radio/`, not articles.
- The broad BBC `main p` selector includes site modules. In the latest 30-day article
  set, 323/357 BBC Arabic bodies contained the Arabic “most read” heading and 281/357
  contained podcast-module copy. A later exact-marker check found 149 BBC Swahili
  bodies containing `End of Iliyosomwa zaidi`; the earlier count of 51 based on the
  ordinary word `maarufu` was not a valid boilerplate measure.
- Full revision histories were downloaded locally: 549 rows for 215 revised articles.
  BBC revisions are usually material live-news updates (median adjacent-version word
  similarity about 0.77); Africanews revisions are usually trivial punctuation or
  append-only edits (median 0.995). The 34-version BBC Swahili record is a live page
  that grew from 3,948 to 8,318 words. Its feed timestamps were blank/unchanged, so it
  was intentionally re-fetched every run and each changed body produced a new hash.

## Integrity evidence

- Recomputed SHA-256 over `title + "\n" + body_text`: zero mismatches.
- `source_version` vs payload `content_sha256`: zero mismatches.
- Stored vs recomputed character count: zero mismatches.
- Exact duplicate `(source_id, source_record_id, source_version)` rows: zero.
- Declared vs detected ISO 639-3 language: zero mismatches.
- Language confidence below 0.80: zero.

## Acceptance rules now enforced

- Consume `bronze.current_text_article`, not raw bronze snapshots, for
  tagging and sentiment.
- Accept only full text from feed `content:encoded` or article-page extraction;
  never promote an RSS summary to an article body.
- Require at least 80 words and exclude configured non-text tags/URL patterns.
- Keep ingestion country NULL. Assign multi-country subject tags in a separate,
  model-versioned step with roles, confidence and evidence spans.
- Treat `(source_id, document_id)` as article identity and `source_version` as a
  revision. Valid older revisions remain audit history, not independent articles.

The repeatable read-only audit is in `analysis/sentiment_ingestion_audit.sql`.
