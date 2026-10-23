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


def compute(docs: list, tags: list, grounding: list, models: list) -> list:
    """Metric rows for the ALL segment and each language. `models` are short
    model ids; tags/grounding carry full labeler_ids (model_version)."""
    lang_of = {(d["doc_id"], d["source_version"]): d["lang"] or "unknown" for d in docs}
    pairs = defaultdict(dict)  # (doc, version, country) -> {model: label}
    for t in tags:
        key = (t["doc_id"], t["source_version"], t["country_iso3"])
        if key[:2] in lang_of:
            pairs[key][short(t["labeler_id"])] = t["sentiment_label"]

    segments = {"ALL": (list(lang_of), list(pairs))}
    for lang in sorted(set(lang_of.values())):
        segments[lang] = ([k for k, v in lang_of.items() if v == lang],
                          [p for p in pairs if lang_of[p[:2]] == lang])

    out = []
    n_models = len(models)
    for seg, (seg_docs, seg_pairs) in segments.items():
        seg_type = "ALL" if seg == "ALL" else "LANGUAGE"
        n_pairs = len(seg_pairs)

        def add(name, value, n=n_pairs, extra=None):
            out.append({"segment_type": seg_type, "segment_key": seg, "metric_name": name,
                        "metric_value": None if value is None else round(float(value), 6),
                        "n_items": n, "metric_json": json.dumps(extra) if extra else None})

        share = lambda k, n=n_pairs: (k / n) if n else None
        add("articles", len(seg_docs), n=len(seg_docs))
        add("pairs", n_pairs)
        if not n_pairs:
            continue
        votes = [pairs[p] for p in seg_pairs]
        add("share_all_agree", share(sum(len(v) == n_models and len(set(v.values())) == 1 for v in votes)))
        add("share_all_tag_label_split", share(sum(len(v) == n_models and len(set(v.values())) > 1 for v in votes)))
        for k in range(1, n_models + 1):
            add(f"share_tagged_by_{k}", share(sum(len(v) == k for v in votes)))

        splits = [v for v in votes if len(v) == n_models and len(set(v.values())) > 1]
        one_vs_rest = []
        for v in splits:
            counts = Counter(v.values())
            if n_models > 2 and len(counts) == 2 and min(counts.values()) == 1:
                lone = min(counts, key=counts.get)
                one_vs_rest.append(next(m for m, lab in v.items() if lab == lone))
        add("share_three_way_split", share(sum(len(set(v.values())) == n_models >= 3 for v in splits)))
        for m in models:
            mine = [v[m] for v in votes if m in v]
            add(f"model:{m}:extra_tag_share", share(sum(set(v) == {m} for v in votes)))
            add(f"model:{m}:missed_share", share(sum(len(v) == n_models - 1 and m not in v for v in votes)))
            add(f"model:{m}:odd_one_out_share",
                share(one_vs_rest.count(m), len(one_vs_rest)), n=len(one_vs_rest))
            add(f"model:{m}:tags_per_article", share(len(mine), len(seg_docs)), n=len(seg_docs))
            add(f"model:{m}:share_mixed", share(mine.count("MIXED"), len(mine)), n=len(mine))
            add(f"model:{m}:share_uncertain", share(mine.count("UNCERTAIN"), len(mine)), n=len(mine))
            g = [r for r in grounding if short(r["labeler_id"]) == m and (seg == "ALL" or r["lang"] == seg)]
            n_ev, n_ok = sum(r["n"] for r in g), sum(r["grounded"] for r in g)
            add(f"model:{m}:evidence_grounded", share(n_ok, n_ev), n=n_ev)
        for a, b in itertools.combinations(models, 2):
            both = [v for v in votes if a in v and b in v]
            add(f"pair:{a}|{b}:label_agreement", share(sum(v[a] == v[b] for v in both), len(both)), n=len(both))

        kept = [v for v in votes if len(v) * 2 > n_models]
        add("rule:any_disagreement:human_share",
            share(sum(not (len(v) == n_models and len(set(v.values())) == 1) for v in votes)))
        add("rule:country_majority:human_share",
            share(sum(len(set(v.values())) > 1 for v in kept), len(kept)), n=len(kept))
        add("rule:full_majority:human_share",
            share(sum(max(Counter(v.values()).values()) * 2 <= len(v) for v in kept), len(kept)),
            n=len(kept))
        for m in models:
            rest = [{k: lab for k, lab in v.items() if k != m} for v in votes]
            rest = [v for v in rest if v]
            add(f"rule:without:{m}:human_share",
                share(sum(not (len(v) == n_models - 1 and len(set(v.values())) == 1) for v in rest), len(rest)),
                n=len(rest))
    return out


def run(ctx, run_id) -> dict:
    versions = evaluated_versions(ctx.client)
    docs, tags, grounding = fetch(ctx.client, versions)
    rows = compute(docs, tags, grounding, [short(v) for v in versions])
    now = S.utcnow()
    subject = ",".join(versions)
    records = [{"evaluation_id": run_id, "evaluation_type": "AGREEMENT", "subject_version": subject,
                "eval_set_id": None, "window_start": None, "window_end": None,
                "threshold": None, "passed": None, "computed_at": now, **r} for r in rows]
    from google.cloud import bigquery

    table = ctx.client.get_table(fq(EVAL_TABLE))
    ctx.client.load_table_from_json(
        [{k: S._iso(v) for k, v in r.items()} for r in records], table,
        job_config=bigquery.LoadJobConfig(schema=table.schema,
                                          write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    ).result()
    return {"models": [short(v) for v in versions], "articles": len(docs), "rows_written": len(records)}
