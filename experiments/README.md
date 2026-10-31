# Experiment scripts

One-off scripts from the Gemma investigation (25–26 October 2026). They were run, not deployed, and they need read access to the original BigQuery project. Results are in the main README.

| File | What it does |
|---|---|
| `gemma_enum_test.py` | Re-runs Gemma on 40 articles where only Gemma named a country and 15 where all three models agreed, once with the African-only country list and once with a free country field. Writes nothing to BigQuery. |
| `disagreement_examples.py` | Prints example disagreements with each model's label and whether its quote appears in the article. |
| `quote_grounding.sql` | Share of each model's quotes found in the article, and of tags with no quote. |
| `compare_before_after.sql` | Agreement metrics before and after the Gemma fix. |

The main agreement evaluation is part of the pipeline: `run_sentiment_scoring.py agreement` (`etl/sentiment_scoring/agreement.py`).
