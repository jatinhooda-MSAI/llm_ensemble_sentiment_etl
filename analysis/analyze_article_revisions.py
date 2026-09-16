"""Download and analyze 30 days of revised multilingual news records.

Read-only against BigQuery. Writes local evidence under analysis/revision_audit/.
"""

from __future__ import annotations

import difflib
import json
from collections import Counter, defaultdict
from pathlib import Path

from google.cloud import bigquery


PROJECT = "your-gcp-project"
OUT = Path(__file__).parent / "revision_audit"
SOURCES = (
    "SRC_BBC_SWAHILI",
    "SRC_BBC_ARABIC",
    "SRC_AFRICANEWS_FR",
)


QUERY = """
WITH candidates AS (
  SELECT source_id, source_record_id
  FROM `your-gcp-project.bronze.source_record`
  WHERE source_id IN UNNEST(@sources)
    AND ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY source_id, source_record_id
  HAVING COUNT(DISTINCT source_version) > 1
)
SELECT
  r.source_id,
  r.source_record_id,
  r.source_version,
  r.ingested_at,
  JSON_VALUE(r.payload, '$.article.title') AS title,
  JSON_VALUE(r.payload, '$.article.canonical_url') AS canonical_url,
  JSON_VALUE(r.payload, '$.article.body_text') AS body_text,
  SAFE_CAST(JSON_VALUE(r.payload, '$.article.word_count') AS INT64) AS word_count,
  JSON_VALUE(r.payload, '$.feed_entry.published_raw') AS published_raw,
  JSON_VALUE(r.payload, '$.feed_entry.updated_raw') AS updated_raw
FROM `your-gcp-project.bronze.source_record` r
JOIN candidates USING (source_id, source_record_id)
WHERE r.ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY source_id, source_record_id, ingested_at
"""


def similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(
        None, (left or "").split(), (right or "").split(), autojunk=True
    ).ratio()


def change_kind(previous: dict, current: dict) -> str:
    old = previous.get("body_text") or ""
    new = current.get("body_text") or ""
    ratio = similarity(old, new)
    if old and new.startswith(old):
        return "append_only"
    if new and old.startswith(new):
        return "truncation"
    if ratio >= 0.995:
        return "tiny_edit"
    if ratio >= 0.95:
        return "minor_edit"
    if ratio >= 0.75:
        return "material_edit"
    return "major_rewrite"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    client = bigquery.Client(project=PROJECT)
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("sources", "STRING", list(SOURCES))]
    )
    rows = [dict(row.items()) for row in client.query(QUERY, job_config=config).result()]

    with (OUT / "revised_article_versions.jsonl").open("w", encoding="utf-8") as handle:
        for row in rows:
            row["ingested_at"] = row["ingested_at"].isoformat()
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    histories: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        histories[(row["source_id"], row["source_record_id"])].append(row)

    source_changes: dict[str, Counter] = defaultdict(Counter)
    source_pairs: dict[str, list[float]] = defaultdict(list)
    article_summaries = []
    for (source_id, record_id), versions in histories.items():
        changes = Counter()
        similarities = []
        for previous, current in zip(versions, versions[1:]):
            kind = change_kind(previous, current)
            ratio = similarity(previous.get("body_text") or "", current.get("body_text") or "")
            changes[kind] += 1
            source_changes[source_id][kind] += 1
            similarities.append(ratio)
            source_pairs[source_id].append(ratio)
        article_summaries.append({
            "source_id": source_id,
            "source_record_id": record_id,
            "title": versions[-1].get("title"),
            "canonical_url": versions[-1].get("canonical_url"),
            "versions": len(versions),
            "first_words": versions[0].get("word_count"),
            "last_words": versions[-1].get("word_count"),
            "min_similarity": round(min(similarities), 4),
            "mean_similarity": round(sum(similarities) / len(similarities), 4),
            "change_kinds": dict(changes),
            "distinct_published_raw": len({v.get("published_raw") for v in versions}),
            "distinct_updated_raw": len({v.get("updated_raw") for v in versions}),
        })

    article_summaries.sort(key=lambda x: (-x["versions"], x["source_id"]))
    with (OUT / "revision_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(article_summaries, handle, ensure_ascii=False, indent=2)

    lines = [
        "# Article revision analysis",
        "",
        f"Downloaded {len(rows):,} version rows for {len(histories):,} revised articles.",
        "",
        "## Change categories by source",
        "",
        "| Source | Revised articles | Version transitions | Median similarity | Change categories |",
        "|---|---:|---:|---:|---|",
    ]
    for source_id in SOURCES:
        source_articles = [x for x in article_summaries if x["source_id"] == source_id]
        ratios = sorted(source_pairs[source_id])
        median = ratios[len(ratios) // 2] if ratios else 1.0
        categories = ", ".join(f"{k}: {v}" for k, v in source_changes[source_id].most_common())
        lines.append(
            f"| {source_id} | {len(source_articles)} | {len(ratios)} | {median:.4f} | {categories} |"
        )

    lines.extend(["", "## Most-versioned articles", ""])
    for summary in article_summaries[:20]:
        lines.extend([
            f"### {summary['source_id']} — {summary['versions']} versions",
            "",
            f"- Title: {summary['title']}",
            f"- URL: {summary['canonical_url']}",
            f"- Words: {summary['first_words']} → {summary['last_words']}",
            f"- Similarity: mean {summary['mean_similarity']}, minimum {summary['min_similarity']}",
            f"- Changes: {summary['change_kinds']}",
            f"- Distinct feed published stamps: {summary['distinct_published_raw']}",
            f"- Distinct feed updated stamps: {summary['distinct_updated_raw']}",
            "",
        ])

    (OUT / "revision_report.md").write_text("\n".join(lines), encoding="utf-8")

    # Bounded word-level change excerpts for the most-versioned article from
    # each source. Article bodies are stored as one line, so conventional line
    # diffs are both expensive and unreadable.
    for source_id in SOURCES:
        candidates = [x for x in article_summaries if x["source_id"] == source_id]
        if not candidates:
            continue
        top = candidates[0]
        versions = histories[(source_id, top["source_record_id"])]
        diff_lines = [
            f"# Revision excerpts: {source_id}",
            "",
            f"Article: {top['title']}",
            f"URL: {top['canonical_url']}",
            "",
        ]
        for index, (previous, current) in enumerate(zip(versions, versions[1:]), start=1):
            old_words = (previous.get("body_text") or "").split()
            new_words = (current.get("body_text") or "").split()
            matcher = difflib.SequenceMatcher(None, old_words, new_words, autojunk=True)
            diff_lines.extend([
                f"## Version {index} → {index + 1}",
                "",
                f"Words: {len(old_words)} → {len(new_words)}; similarity {matcher.ratio():.4f}",
                "",
            ])
            changes = 0
            for tag, i1, i2, j1, j2 in matcher.get_opcodes():
                if tag == "equal":
                    continue
                changes += 1
                if changes > 20:
                    diff_lines.append("- Additional changes omitted from this excerpt.")
                    break
                old_excerpt = " ".join(old_words[i1:i2][:80])
                new_excerpt = " ".join(new_words[j1:j2][:80])
                diff_lines.extend([
                    f"- `{tag}`",
                    f"  - old: {old_excerpt or '[empty]'}",
                    f"  - new: {new_excerpt or '[empty]'}",
                ])
            diff_lines.append("")
        safe_source = source_id.lower()
        (OUT / f"{safe_source}_top_history.md").write_text("\n".join(diff_lines), encoding="utf-8")


if __name__ == "__main__":
    main()
