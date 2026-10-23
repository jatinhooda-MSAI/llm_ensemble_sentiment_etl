"""
Model agreement evaluation: how the scoring models compare on the articles
they have all answered. Writes long-format rows to
gold.model_evaluation_slice (evaluation_type AGREEMENT), overall and per
language, so the numbers can be re-run and compared as models change.

Metrics (share = fraction of pairs unless noted; a pair is one (article, country)
that at least one model tagged):
  articles, pairs
  share_all_agree               every model tagged it, all with the same label
  share_all_tag_label_split     every model tagged it, labels differ
  share_tagged_by_<k>           exactly k models tagged it
  model:<m>:extra_tag_share     only m tagged it
  model:<m>:missed_share        every other model tagged it, m did not
  model:<m>:odd_one_out_share   share of 1-vs-rest label splits where m is the one
  model:<m>:tags_per_article    tags per article that m answered
  model:<m>:evidence_grounded   share of m's quotes found verbatim in the article
  model:<m>:share_mixed / share_uncertain   of m's own tags
  pair:<a>|<b>:label_agreement  same label, over pairs both tagged
  rule:any_disagreement:human_share     anything short of full agreement
  rule:country_majority:human_share     country kept by majority; label splits to humans
                                        (share of kept pairs)
  rule:full_majority:human_share        country and label by majority; humans only
                                        when no label has a majority (share of kept pairs)
  rule:without:<m>:human_share          any-disagreement rule with model m dropped
"""

import itertools
import json
from collections import Counter, defaultdict

from etl.sentiment_scoring import store as S
from etl.sentiment_scoring.config import DOCUMENT_VIEW, EVAL_TABLE, LABEL_TABLE, MODEL_VERSION_TABLE, TASK, fq


def short(model_version: str) -> str:
    return model_version.split("|")[0]


def evaluated_versions(client) -> list:
    """Every TAG_AND_SENTIMENT version that is ACTIVE or STANDBY."""
    rows = S._query(client, f"""
        SELECT model_version FROM `{fq(MODEL_VERSION_TABLE)}`
        WHERE task = @task
        QUALIFY ROW_NUMBER() OVER (PARTITION BY model_version ORDER BY recorded_at DESC) = 1
          AND status IN ('ACTIVE', 'STANDBY')
    """, {"task": TASK})
    return sorted(r.model_version for r in rows)


def fetch(client, versions: list):
    """(docs, tags, grounding) for articles every version answered usably."""
    base = f"""
        WITH latest AS (
          SELECT * FROM `{fq(LABEL_TABLE)}`
          WHERE labeler_type = 'MODEL' AND labeler_id IN UNNEST(@versions)
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY labeler_id, doc_id, source_version, IFNULL(country_iso3, '')
            ORDER BY created_at DESC) = 1),
        docs AS (
          SELECT doc_id, source_version FROM latest
          GROUP BY 1, 2
          HAVING COUNT(DISTINCT IF(tag_status != 'ERROR', labeler_id, NULL)) = ARRAY_LENGTH(@versions)
             AND COUNTIF(tag_status = 'ERROR') = 0)
    """
    params = {"versions": versions}
    docs = S._query(client, base + f"""
        SELECT d.doc_id, d.source_version, v.language_iso639_3 AS lang
        FROM docs d JOIN `{fq(DOCUMENT_VIEW)}` v USING (doc_id, source_version)
    """, params)
    tags = S._query(client, base + """
        SELECT l.doc_id, l.source_version, l.country_iso3, l.labeler_id, l.sentiment_label
        FROM latest l JOIN docs USING (doc_id, source_version)
        WHERE l.tag_status = 'TAGGED'
    """, params)
    grounding = S._query(client, base + f"""
        SELECT l.labeler_id, v.language_iso639_3 AS lang, COUNT(*) AS n,
               COUNTIF(STRPOS(
                 LOWER(REGEXP_REPLACE(v.body_text, r'\\s+', ' ')),
                 LOWER(TRIM(REGEXP_REPLACE(e, r'\\s+', ' ')))) > 0) AS grounded
        FROM latest l JOIN docs USING (doc_id, source_version)
        JOIN `{fq(DOCUMENT_VIEW)}` v USING (doc_id, source_version), UNNEST(l.evidence) AS e
        WHERE l.tag_status = 'TAGGED' AND LENGTH(TRIM(e)) > 0
        GROUP BY 1, 2
    """, params)
    return ([dict(r.items()) for r in docs], [dict(r.items()) for r in tags],
            [dict(r.items()) for r in grounding])
