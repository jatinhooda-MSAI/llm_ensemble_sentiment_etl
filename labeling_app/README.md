# Sentiment labeling app

A small web app where human labelers answer the items the models could not settle.

- **What it asks:** each item is one (article, country) pair. The labeler answers two questions:
  1. Is this article substantially about this country?
  2. If yes, how does it portray that country? Positive, negative, neutral, mixed, or too little to judge.
- **What it hides:** the models' answers. Labelers are not told why an item is in their queue.

## Where items come from

`silver.labeling_queue`, a view over the model and human labels, decides what needs a human:

| Reason | When |
|---|---|
| Model disagreement | The active models did not all give the same countries and labels. |
| Random audit | 5% of items where all models agreed, to measure how often they agree and are wrong. |
| Second opinion | 20% of human-labeled items, so two humans' answers can be compared. |

No item is ever labeled by more than two humans. The server refuses a first answer to an item that already has its two.

## Tables

| Table | Use |
|---|---|
| `silver.labeling_queue` | The work queue (read) |
| `silver.v_document` | Article title and text (read) |
| `gold.dim_country` | Country names (read) |
| `gold.dim_labeler` | Login codes and the languages each labeler reads (read) |
| `silver.sentiment_label` | Answers, appended as `labeler_type = 'HUMAN'` rows. Nothing is updated or deleted; the latest answer per labeler and item wins. |

To add a labeler, insert a row into `gold.dim_labeler`. Leave `languages` empty to show all languages.

```sql
INSERT INTO `your-gcp-project.gold.dim_labeler`
  (labeler_id, login_code, languages, active, valid_from, valid_to, created_by)
VALUES ('lab_001', 'choose-a-code', ['ara'], TRUE, CURRENT_TIMESTAMP(), NULL, 'admin');
```

## Run locally

```bash
cd labeling_app
BQ_PROJECT=your-gcp-project .venv/bin/uvicorn main:app --port 8080
```

## Deploy (Cloud Run, runs as the pipeline's service account)

```bash
cd labeling_app
gcloud builds submit --project=your-gcp-project --config=cloudbuild.yaml .
gcloud run deploy sentiment-labeling-app --project=your-gcp-project --region=us-central1 \
  --image=us-central1-docker.pkg.dev/your-gcp-project/your-artifact-repo/sentiment-labeling-app:<BUILD_ID> \
  --service-account=your-service-account@your-gcp-project.iam.gserviceaccount.com \
  --set-env-vars=BQ_PROJECT=your-gcp-project --no-allow-unauthenticated
```

The service is private. Labelers outside the project need the project Owner to grant `roles/run.invoker` to `allUsers`, or a sign-in layer such as IAP in front of it. The deploying account could not grant that.
