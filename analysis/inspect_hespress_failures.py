"""Inspect current HTML structure for recent Hespress full-text failures."""

from collections import Counter
import json

import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery


QUERY = """
WITH x AS (
  SELECT source_record_id,
         JSON_VALUE(payload, '$.article.title') title,
         JSON_VALUE(payload, '$.article.canonical_url') url,
         JSON_VALUE(payload, '$.fetch.error') error,
         ingested_at
  FROM `your-gcp-project.bronze.source_record`
  WHERE source_id = 'SRC_HESPRESS'
    AND ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_id, source_record_id ORDER BY ingested_at DESC
  ) = 1
)
SELECT * FROM x
WHERE error IS NOT NULL
ORDER BY ingested_at DESC
LIMIT 12
"""


def signature(tag) -> str:
    parent = tag.parent
    classes = ".".join(parent.get("class", [])[:3])
    ident = parent.get("id") or ""
    return f"{parent.name}#{ident}.{classes}"


def main() -> None:
    rows = list(bigquery.Client(project="your-gcp-project").query(QUERY).result())
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (compatible; news-sentiment-ingestion/1.0)"
    )
    for row in rows:
        print(f"\nTITLE: {row.title}\nSTORED ERROR: {row.error}\nURL: {row.url}")
        try:
            response = session.get(row.url, timeout=20)
            print(f"CURRENT HTTP: {response.status_code}; bytes={len(response.content)}")
            soup = BeautifulSoup(response.text, "lxml")
            schema_types = []
            article_body_lengths = []
            for node in soup.select("script[type='application/ld+json']"):
                try:
                    data = json.loads(node.string or "{}")
                except (TypeError, json.JSONDecodeError):
                    continue
                objects = data.get("@graph", [data]) if isinstance(data, dict) else data
                if not isinstance(objects, list):
                    objects = [objects]
                for obj in objects:
                    if not isinstance(obj, dict):
                        continue
                    kind = obj.get("@type")
                    if kind:
                        schema_types.extend(kind if isinstance(kind, list) else [kind])
                    if obj.get("articleBody"):
                        article_body_lengths.append(len(obj["articleBody"]))
            print(
                f"SCHEMA TYPES: {sorted(set(schema_types))}; "
                f"articleBody lengths={article_body_lengths}; "
                f"video tags={len(soup.find_all('video'))}; iframe tags={len(soup.find_all('iframe'))}"
            )
            substantial = [p for p in soup.find_all("p") if len(p.get_text(" ", strip=True)) >= 60]
            parents = Counter(signature(p) for p in substantial)
            print(f"SUBSTANTIAL PARAGRAPHS: {len(substantial)}")
            for parent, count in parents.most_common(5):
                chars = sum(
                    len(p.get_text(" ", strip=True))
                    for p in substantial if signature(p) == parent
                )
                print(f"  {parent}: {count} paragraphs, {chars} chars")
        except Exception as exc:
            print(f"CURRENT FETCH ERROR: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
