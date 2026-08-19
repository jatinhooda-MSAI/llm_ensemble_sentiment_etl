# Sentiment Ingestion (bronze layer)

Fetches **full news articles** from multilingual African sources — Arabic,
French, Swahili — and appends them to BigQuery
**`bronze.source_record`**, per an existing data dictionary. Bronze only: one
append-only row per source-native record, full payload preserved as JSON. The
bronze table exists already and its schema is owned elsewhere — this code
never creates or alters tables.

## Sources

| config key | source_id | Language / script | Notes |
|---|---|---|---|
| `bbc_swahili` | `SRC_BBC_SWAHILI` | swa / Latn | RSS + page scrape; radio-program URLs excluded |
| `bbc_arabic` | `SRC_BBC_ARABIC` | ara / Arab | RSS + page scrape |
| `hespress` | `SRC_HESPRESS` | ara / Arab | full text in feed `content:encoded`; video/cartoon tags excluded; feed paged (`?paged=N`) |
| `allafrica_fr` | `SRC_ALLAFRICA_FR` | fra / Latn | full page text; robots.txt allows story pages, crawl-delay 1s (we use 1.5s) |
| `africanews_fr` | `SRC_AFRICANEWS_FR` | fra / Latn | short brief-style articles (~150–200 words) |

Financial Afrik is deliberately absent. Its public pages returned hard-truncated
opening excerpts rather than complete articles, so it is not an ingestion source.
RSS summaries are retained only in the source payload and are never accepted as
article bodies. Every loaded article must contain at least 80 words of full text.

Source definitions (feeds, CSS selectors, delays, language codes) live in
[`config/sentiment_sources.yaml`](../../config/sentiment_sources.yaml).

**Feed paging.** Hespress publishes ~95 posts/day but its feed lists only
the newest 12, so one page per 6h run kept about half. Sources with
`feed_pagination: wordpress` are read page by page (`?paged=2`, `?paged=3`,
…) and stop after the first page that holds an already-ingested entry
(`caught_up`), when a whole page is older than `--days` (`cutoff`), at the
end of the feed, or at `max_feed_pages`. The run summary reports
`feed_pages` and `feed_page_stop`; `page_cap` means the run never caught up
and is logged as a possible coverage gap.

## Running

```bash
# offline check — writes NDJSON, no credentials and no BQ_PROJECT needed
python run_sentiment_ingestion.py --sources all --days 3 --max 5 --dry-run /tmp/bronze_dryrun

# live load to BigQuery (set BQ_PROJECT to your project)
export BQ_PROJECT=your-gcp-project
python run_sentiment_ingestion.py --sources all --days 7 --max 100

# one source
python run_sentiment_ingestion.py --sources bbc_swahili --days 2 --max 5

# intentional backfill after extraction logic changes
python run_sentiment_ingestion.py --sources all --days 2 --max 200 --force-refresh

# history backfill for a paginated feed: page past known entries back to --days
python run_sentiment_ingestion.py --sources hespress --days 56 --max 6000 --backfill-pages 450
```

Backfilled rows carry the backfill time in `ingested_at`; bucket by
`published_at` for anything date-based.

Env: `BQ_PROJECT` (**required for live loads, no default**; the live
medallion is in project `your-gcp-project`), `BQ_BRONZE_DATASET` (default
`bronze`), `BQ_BRONZE_TABLE` (default `source_record`).

**Auth (local dev):** the Python client uses *application-default
credentials*, which are a separate store from `gcloud auth login` — if the
runner 403s while `bq` works, run:

```bash
gcloud auth application-default login   # an account with BigQuery editor access
gcloud auth application-default set-quota-project your-gcp-project
```

**Auth (production):** run the job *as* the pipeline's service account
(e.g. Cloud Run Job identity) — no key files, no impersonation code needed.
The deploying account needs `iam.serviceAccounts.actAs` on that service account to
deploy resources that run as it. Local token impersonation is not needed —
for local runs just use your own ADC (editor role suffices).

Tests (offline): `pytest tests/test_sentiment_ingestion.py -q`

Spot-check loaded rows:

```sql
SELECT source_id, document_id, title, word_count, language_code, ingested_at
FROM `<project>.bronze.current_text_article`
ORDER BY ingested_at DESC
LIMIT 20;
```

## Dependencies

`requirements.sentiment.txt`: `feedparser`, `requests`, `beautifulsoup4`,
`lxml`, `langdetect`, `pyyaml`, `google-cloud-bigquery`; `pytest` for tests.

## Row contract

- `source_record_id` — RSS entry GUID (fallback: entry link).
- `payload.article.document_id` — stable SHA-256 of source plus canonical URL.
  It is the common identity across publisher revisions and across rotating RSS
  GUIDs for the same URL.
- `source_version` — sha256(title + "\n" + body_text). Re-fetching an
  unchanged article produces the same version and is skipped (lookback query
  in `bq_writer.py`); an edited article inserts a **new** row — append-only,
  never overwrite. This follows the dictionary's append-only bronze contract.
- `payload.fetch.normalized_content_sha256` — a lexical fingerprint that
  ignores Unicode presentation forms, punctuation, invisible characters and
  whitespace. A formatting-only edit is skipped even if the exact bytes differ.
- **Pre-fetch skip**: before fetching, the runner pulls each known GUID's
  stored feed stamp (`updated`/`published` string) from bronze; entries whose
  stamp is unchanged are skipped **before any article download**
  (`skipped_known` in the run summary). If a feed has no useful update stamp,
  stored HTTP `ETag`/`Last-Modified` values are sent in a conditional request;
  a `304 Not Modified` avoids downloading the article body. The two hashes are
  the final safeguards after a full response.
- `country_iso3` — always NULL in ingestion. Publisher location is not the
  subject of sentiment, and keyword counts cannot represent multi-country
  stories. A later model assigns one or more country targets with role,
  confidence, evidence spans and model version.
- `object_uri` — NULL (raw/object-storage layer out of scope).
- `payload` — full source-native record: feed entry (incl. verbatim
  source-native date strings), article text + provenance
  (`body_source`: `feed_content_encoded | page_scrape`), fetch metadata
  (HTTP status, validators and both content hashes), and declared + detected
  language (ISO 639-3 + ISO 15924). Detection is a validation signal; it does
  not replace the source-declared language.
