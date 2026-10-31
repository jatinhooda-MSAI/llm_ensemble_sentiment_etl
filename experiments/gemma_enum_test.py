"""Does the African-only country enum force Gemma onto wrong codes?

Re-runs Gemma on (1) articles where only Gemma tagged some country and
(2) control articles where all three models fully agreed, with
  ENUM : today's schema (country_iso3 restricted to African codes)
  FREE : country_iso3 any string; non-African codes dropped afterwards
Writes nothing to BigQuery. Output: JSON lines + a summary.

Usage (run 2026-10-25 with 40 and 15):
  GCLOUD_TOKEN=$(gcloud auth print-access-token) \
    python3 experiments/gemma_enum_test.py 40 15 /tmp/gemma_enum_test.jsonl
"""
import copy
import json
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("BQ_PROJECT", "your-gcp-project")

from etl.sentiment_scoring import jobs, prompt as P, providers as V, store as S  # noqa: E402
from etl.sentiment_scoring.config import load_config  # noqa: E402

N_GEMMA_ONLY, N_CONTROL = int(sys.argv[1]), int(sys.argv[2])
OUT = sys.argv[3]

cfg = load_config()
ctx = jobs.Context(cfg, S.get_client())
spec = next(m for m in cfg.models if m.slot == "gemma")
enum_schema = ctx.schema
free_schema = copy.deepcopy(enum_schema)
free_schema["properties"]["countries"]["items"]["properties"]["country_iso3"] = {"type": "string"}

SQL = """
WITH latest AS (
  SELECT CASE WHEN labeler_id LIKE 'claude%' THEN 'claude' WHEN labeler_id LIKE 'gemini%' THEN 'gemini' ELSE 'gemma' END AS m,
         doc_id, source_version, country_iso3, tag_status, sentiment_label
  FROM `your-gcp-project.silver.sentiment_label` WHERE labeler_type = 'MODEL'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY labeler_id, doc_id, source_version, IFNULL(country_iso3,'') ORDER BY created_at DESC) = 1),
docs AS (SELECT doc_id, source_version FROM latest GROUP BY 1,2
         HAVING COUNT(DISTINCT IF(tag_status != 'ERROR', m, NULL)) = 3 AND COUNTIF(tag_status='ERROR') = 0),
pairs AS (SELECT doc_id, source_version, country_iso3, ARRAY_AGG(STRUCT(m, sentiment_label)) votes
          FROM latest JOIN docs USING (doc_id, source_version) WHERE tag_status = 'TAGGED' GROUP BY 1,2,3),
per_doc AS (
  SELECT doc_id, source_version,
    LOGICAL_OR(ARRAY_LENGTH(votes) = 1 AND votes[OFFSET(0)].m = 'gemma') AS gemma_only,
    LOGICAL_AND(ARRAY_LENGTH(votes) = 3 AND (SELECT COUNT(DISTINCT sentiment_label) FROM UNNEST(votes)) = 1) AS all_agree,
    ARRAY_AGG(STRUCT(country_iso3, votes)) AS pairs
  FROM pairs GROUP BY 1,2)
SELECT p.*, d.title, d.body_text, d.language_iso639_3, d.published_at,
       IF(gemma_only, 'GEMMA_ONLY', 'CONTROL') AS grp
FROM per_doc p JOIN `your-gcp-project.silver.v_document` d USING (doc_id, source_version)
WHERE gemma_only OR all_agree
QUALIFY ROW_NUMBER() OVER (PARTITION BY gemma_only ORDER BY FARM_FINGERPRINT(CONCAT(doc_id, 'enumtest'))) <= IF(gemma_only, @a, @b)
"""
from google.cloud import bigquery  # noqa: E402

rows = list(ctx.client.query(SQL, job_config=bigquery.QueryJobConfig(query_parameters=[
    bigquery.ScalarQueryParameter("a", "INT64", N_GEMMA_ONLY),
    bigquery.ScalarQueryParameter("b", "INT64", N_CONTROL)])).result())
print(f"{len(rows)} articles", file=sys.stderr)


def raw_codes(text):
    try:
        return [str(c.get("country_iso3")) for c in P._extract_json(text).get("countries", [])]
    except Exception:
        return None


def ask(schema, doc):
    body = V.gemma_request(spec.model_id, ctx.system, P.user_message(doc), schema, spec.max_output_tokens)
    text = V.gemma_text(ctx.vertex.chat_completion(spec.location, body))
    try:
        tags = {t["country_iso3"]: t["sentiment_label"] for t in P.validate(text, ctx.codes)}
    except P.ResponseError as exc:
        tags = {"ERROR": str(exc)[:80]}
    return {"raw": raw_codes(text), "tags": tags}


def work(r):
    doc = dict(r.items())
    stored = {p["country_iso3"]: {v["m"]: v["sentiment_label"] for v in p["votes"]} for p in doc["pairs"]}
    return {"grp": doc["grp"], "doc_id": doc["doc_id"], "lang": doc["language_iso639_3"],
            "title": (doc["title"] or "")[:100], "stored": stored,
            "enum": ask(enum_schema, doc), "free": ask(free_schema, doc)}


with ThreadPoolExecutor(max_workers=2) as pool, open(OUT, "w") as f:
    results = []
    for res in pool.map(work, rows):
        results.append(res)
        f.write(json.dumps(res, ensure_ascii=False) + "\n")
        f.flush()


def others(stored):
    return {c for c, v in stored.items() if "gemini" in v or "claude" in v}


summary = {}
for grp in ("GEMMA_ONLY", "CONTROL"):
    rs = [r for r in results if r["grp"] == grp]
    s = Counter()
    for r in rs:
        ref = others(r["stored"])
        s["articles"] += 1
        s["stored_gemma_extra"] += len({c for c, v in r["stored"].items() if set(v) == {"gemma"}})
        for mode in ("enum", "free"):
            tags = set(r[mode]["tags"]) - {"ERROR"}
            s[f"{mode}_errors"] += "ERROR" in r[mode]["tags"]
            s[f"{mode}_tags"] += len(tags)
            s[f"{mode}_extra_vs_others"] += len(tags - ref)
            s[f"{mode}_missed_vs_others"] += len(ref - tags)
            s[f"{mode}_label_match_others"] += sum(
                1 for c in tags & ref
                if r[mode]["tags"][c] in {r["stored"][c].get("gemini"), r["stored"][c].get("claude")})
        s["free_non_african_raw"] += sum(1 for c in (r["free"]["raw"] or []) if c not in ctx.codes)
    summary[grp] = dict(s)
non_african = Counter(c for r in results for c in (r["free"]["raw"] or []) if c not in ctx.codes)
summary["free_non_african_codes"] = non_african.most_common(25)
print(json.dumps(summary, indent=1))
