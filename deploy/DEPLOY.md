# Cloud deploy (Profile C) — GCP Cloud Run + Upstash Redis, target Rp 0

## One-time prerequisites (account owner)

1. **GCP**: create account at console.cloud.google.com, activate billing
   (card required for verification; Cloud Run always-free tier: 2M
   requests/month — this demo stays far under it). Create a project, e.g.
   `streaming-fraud-fikri`.
2. **gcloud CLI**: `winget install Google.CloudSDK`, then `gcloud auth login`
   and `gcloud config set project <PROJECT_ID>`.
3. **Upstash Redis** (free, no card): console.upstash.com → create Redis
   database (region: ap-southeast-1) → copy the `rediss://...` URL.

## Deploy

```bash
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

# build + deploy straight from source (Cloud Build, free tier)
gcloud run deploy fraud-api \
  --source . \
  --region asia-southeast2 \
  --allow-unauthenticated \
  --memory 512Mi --cpu 1 --max-instances 1 \
  --set-env-vars REDIS_URL="rediss://default:<password>@<host>:6379"
```

(`--max-instances 1` + 512Mi keeps usage inside the always-free envelope.)

## Feeding the cloud online store

The local processor can write features to Upstash instead of local Redis:

```bash
set REDIS_URL=rediss://default:<password>@<host>:6379
python -m src.processor
python -m src.simulator --tps 20 --limit 2000
```

Then the Cloud Run `/score` endpoint serves with *real* online features.

## Broker in the cloud (optional, trial-based)

No always-free managed Kafka exists in 2026 (Upstash Kafka discontinued).
For a recorded full-cloud demo: Redpanda Serverless 14-day trial ($100
credit) — point `KAFKA_BROKER` + SASL env at the serverless cluster and run
processor/scorer as Cloud Run jobs. Documented as trial-grade, not always-on.
