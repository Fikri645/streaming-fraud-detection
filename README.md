# streaming-fraud-detection

[![CI](https://github.com/Fikri645/streaming-fraud-detection/actions/workflows/ci.yml/badge.svg)](https://github.com/Fikri645/streaming-fraud-detection/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)

**The same fraud model, three deployment patterns.** My
[fraud-detection](https://github.com/Fikri645/fraud-detection) project scores
batches; this project makes the identical LightGBM model (PR-AUC 0.9666)
score **a live transaction stream**: Kafka-compatible broker, stateful
streaming features, online feature store, measured p99 latency — with
pre-registered targets and the failures reported as honestly as the passes.

```
simulator ──► Redpanda topic: transactions
                  │ (Quix Streams, stateful per card)
                  ▼
            sliding-window features (1h/24h/7d) ──► Redis online store
                  │ topic: features
                  ▼
            micro-batched LightGBM scorer ──► topics: scores · alerts
                  │                               │
                  ▼                               ▼
            FastAPI /score /metrics        SSE live dashboard
```

## Measured results (pre-registered targets, [full report](reports/results.md))

| Target | Result |
|:--|:--|
| **T1** e2e latency p99 < 250 ms @ 100 TPS | ✅ **p50 102 ms · p99 172 ms** — after two real optimizations (below) |
| **T2** online/offline feature parity | ✅ window semantics + idempotence + ordering covered by unit tests |
| **T3** replay determinism | ✅ two independent replays → **875/875 cards byte-identical** |
| **T4** Redis-down degradation | ✅ Redis paused 15 s mid-burst → **1,500/1,500 scores still flowed** |

### The engineering stories (what actually went wrong)

1. **Poison pill** — the first run died instantly on a leftover smoke-test
   message at offset 0. Fix: schema-validate at the boundary, skip + count
   (`metrics:poison`), never let one bad message stop the stream.
2. **Throughput ceiling** — scoring one message at a time capped the pipeline
   at ~68 TPS; at 100 TPS input, latency grew unbounded (p50 **12.9 s**).
   Fix: micro-batch up to 256 messages into **one vectorized
   `predict_proba`** → p50 298 ms.
3. **A silently expensive default** — librdkafka's `fetch.wait.max.ms=500`
   dominated e2e latency. 500→50 ms on both consumers → **p50 102 ms**.
4. **Replay divergence** — the first T3 run failed: 505/875 cards diverged
   (at-least-once redelivery double-appended history; replayed event time
   jumped backwards). Fix: **idempotent fold by `txn_id` + sorted insertion**
   → byte-identical replays.

## Stack (2026 choices, rationale in [RESEARCH.md](RESEARCH.md))

**Redpanda** v24.3 (Kafka API-compatible, single binary — every Kafka client
works unchanged) · **Quix Streams** (pure-Python stateful stream processing;
Faust is unmaintained, PyFlink is a JVM wrapper) · **Redis 7** online feature
store · **LightGBM** (reused, frozen) · **FastAPI + SSE** · Docker Compose ·
GitHub Actions.

## Quick start

```bash
docker compose up -d            # Redpanda (:19092) + Console (:8088) + Redis
py -3.11 -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt

python -m src.processor         # terminal 1: stateful feature enrichment
python -m src.scorer            # terminal 2: micro-batched scoring
uvicorn api.main:app --port 8000   # terminal 3: API + live dashboard at /
python -m src.simulator --tps 100 --limit 5000   # terminal 4: replay traffic

python -m src.bench t1 --n 5000 --tps 100        # measure e2e latency
pytest tests/ -v                                  # window math / contracts
```

## Online vs offline features — the contract

12 of the model's 24 features are **stateful** and computed online per card
(counts/sums/means over 1h/24h/7d event-time windows, seconds-since-previous,
lifetime-mean ratios, distinct merchants); the other 12 ride on the event.
The same `features_core` math is unit-tested for window semantics,
**idempotence under redelivery**, and **out-of-order tolerance** — the three
properties that make a replayable, at-least-once stream behave like a
database.

## Cloud profile — LIVE

**[fraud-api-340979059251.asia-southeast2.run.app](https://fraud-api-340979059251.asia-southeast2.run.app)**
— FastAPI `/score` + `/health` + `/metrics` + live dashboard on **GCP Cloud
Run** (Jakarta, always-free tier), reading per-card online features from
**Upstash Redis** (Singapore, always-free). The local processor streams
features into the same Upstash instance, so the cloud `/score` serves with
*real* streaming history:

```
POST /score {"cc_num":"180046617132290","amt":95,"category":"grocery_pos"}
  -> score 0.000, safe          (normal txn for this card)
POST /score {"cc_num":"180046617132290","amt":1500,"category":"shopping_net"}
  -> score 0.136, ALERT         (same card, anomalous amount)
```

The broker has *no* always-free managed option in 2026 (Upstash Kafka was
discontinued in 2025), so the broker + processor + scorer run locally / on a
trial cluster while Cloud Run hosts the serving surface — documented in
[deploy/DEPLOY.md](deploy/DEPLOY.md) and [RESEARCH.md §2](RESEARCH.md).

### Two more war stories, from the deploy itself

5. **The blocking prompt** — the first `gcloud run deploy --source` hung
   forever: it silently waited on a *"create Artifact Registry repo? (Y/n)"*
   confirmation a detached process can't answer. Fix: `--quiet`.
6. **The missing OpenMP runtime** — the container built fine but crashed on
   boot: `python:3.11-slim` ships no `libgomp.so.1`, which **LightGBM**
   requires (`import lightgbm` → `OSError`). Fix: `apt-get install libgomp1`
   in the image.

## License

MIT
