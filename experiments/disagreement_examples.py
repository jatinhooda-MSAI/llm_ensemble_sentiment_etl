"""Print disagreement examples: each model's label and its first quote, with a
check of whether the quote appears in the article.

Usage: GCLOUD_TOKEN=$(gcloud auth print-access-token) python3 experiments/disagreement_examples.py [A B C D]
"""
import os
import sys

from google.cloud import bigquery
from google.oauth2.credentials import Credentials

client = bigquery.Client(project="your-gcp-project", credentials=Credentials(os.environ["GCLOUD_TOKEN"]))

BASE = """
WITH latest AS (
  SELECT CASE WHEN labeler_id LIKE 'claude%' THEN 'claude' WHEN labeler_id LIKE 'gemini%' THEN 'gemini' ELSE 'gemma' END AS m,
         doc_id, source_version, country_iso3, tag_status, sentiment_label, evidence
  FROM `your-gcp-project.silver.sentiment_label` WHERE labeler_type = 'MODEL'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY labeler_id, doc_id, source_version, IFNULL(country_iso3,'') ORDER BY created_at DESC) = 1),
docs AS (SELECT doc_id, source_version FROM latest GROUP BY 1,2 HAVING COUNT(DISTINCT IF(tag_status != 'ERROR', m, NULL)) = 3),
pairs AS (
  SELECT l.doc_id, l.source_version, l.country_iso3,
    MAX(IF(m='gemini', sentiment_label, NULL)) g, MAX(IF(m='gemma', sentiment_label, NULL)) e, MAX(IF(m='claude', sentiment_label, NULL)) c,
    ANY_VALUE(IF(m='gemini', evidence[SAFE_OFFSET(0)], NULL)) g_ev,
    ANY_VALUE(IF(m='gemma', evidence[SAFE_OFFSET(0)], NULL)) e_ev,
    ANY_VALUE(IF(m='claude', evidence[SAFE_OFFSET(0)], NULL)) c_ev
  FROM latest l JOIN docs USING (doc_id, source_version) WHERE l.tag_status = 'TAGGED' GROUP BY 1,2,3),
x AS (SELECT p.*, d.title, d.language_iso639_3 lang, d.source_id, d.body_text
      FROM pairs p JOIN `your-gcp-project.silver.v_document` d USING (doc_id, source_version))
"""

QUERIES = {
    "A. Only Gemma tagged the country":
        "SELECT * FROM x WHERE e IS NOT NULL AND g IS NULL AND c IS NULL "
        "ORDER BY FARM_FINGERPRINT(CONCAT(doc_id, country_iso3)) LIMIT 6",
    "B. All three tagged; one model's label differs":
        "SELECT * FROM x WHERE g IS NOT NULL AND e IS NOT NULL AND c IS NOT NULL "
        "AND ((g = c AND e != g) OR (g = e AND c != g) OR (c = e AND g != c)) "
        "ORDER BY FARM_FINGERPRINT(CONCAT(doc_id, country_iso3, 'b')) LIMIT 6",
    "C. Gemini missed a country the other two tagged":
        "SELECT * FROM x WHERE g IS NULL AND e IS NOT NULL AND c IS NOT NULL "
        "ORDER BY FARM_FINGERPRINT(CONCAT(doc_id, country_iso3, 'c')) LIMIT 3",
    "D. Gemma's quote is not in the article (country tagged by Gemma only)":
        "SELECT * FROM x WHERE e IS NOT NULL AND g IS NULL AND c IS NULL AND e_ev IS NOT NULL "
        "AND STRPOS(LOWER(REGEXP_REPLACE(body_text, r'\\s+', ' ')), LOWER(TRIM(REGEXP_REPLACE(e_ev, r'\\s+', ' ')))) = 0 "
        "ORDER BY FARM_FINGERPRINT(CONCAT(doc_id, country_iso3, 'd')) LIMIT 4",
}

which = sys.argv[1:] or list(QUERIES)
for name in QUERIES:
    if not any(name.startswith(w) for w in which):
        continue
    print(f"##### {name}")
    for r in client.query(BASE + QUERIES[name]).result():
        body = " ".join((r["body_text"] or "").split()).lower()
        print(f"--- [{r['lang']}, {r['source_id']}] {(r['title'] or '')[:90]}")
        print(f"    country {r['country_iso3']}: gemini={r['g']}  gemma={r['e']}  claude={r['c']}")
        for key, model in (("g_ev", "gemini"), ("e_ev", "gemma"), ("c_ev", "claude")):
            ev = r[key]
            if ev:
                where = "in article" if " ".join(ev.split()).lower() in body else "NOT in article"
                print(f"    {model} quote ({where}): {ev[:150]}")
    print()
