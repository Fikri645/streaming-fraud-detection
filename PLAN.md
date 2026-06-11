# Project Plan — Real-Time Streaming Fraud Detection (Kafka + Cloud)

> Status: **EXECUTING.** See [RESEARCH.md](RESEARCH.md) for stack rationale.
> One-sentence pitch: *"I took my batch fraud model and made it score
> transactions in real time — Kafka-compatible broker, stateful streaming
> features, online feature store, measured p99 latency, deployed to cloud —
> same model, three deployment patterns (batch / in-process / streaming)."*

## 1. Phases

### Phase 1 — Scaffold + model handoff (~half day)
Repo, venv 3.11, copy `lgbm_fraud.joblib` + meta from `fraud-detection`,
docker-compose with **Redpanda + Redpanda Console + Redis**, smoke produce/consume.

### Phase 2 — Stream core (~1-2 days)
- `producer/simulator.py`: replays Sparkov test split as JSON events;
  `--tps`, `--burst-fraud` knobs; embeds `produced_at` for latency tracking.
- `processor/features.py` (Quix Streams): per-card stateful sliding windows
  (count/sum/avg 1m/5m/1h, seconds-since-last, geo-velocity) → writes Redis +
  emits enriched `features` topic. **T2 parity test** vs offline pandas calc.
- `scorer/service.py`: consumes `features`, joins model features, LightGBM
  score, threshold 0.0187 → `scores` + `alerts` topics, latency histogram.

### Phase 3 — Serving + observability (~1 day)
FastAPI `/score` (on-demand, reads Redis), `/health`, `/metrics`;
SSE dashboard (live TPS, alert feed, p50/p99, score distribution + PSI drift
vs training distribution). **T1 latency bench** + **T3 replay test** +
**T4 degradation test** scripted, results into `reports/results.md`.

### Phase 4 — Tests + CI + ship hygiene (~half day)
Unit tests (window math, schema, scorer joins — no broker needed via Quix
test harness/fakes), integration test behind a marker (needs compose up),
flake8, GitHub Actions, README with measured numbers.

### Phase 5 — Cloud deploy (Profile C) (~1 day)
Cloud Run: scoring API + dashboard (always-free) · Upstash Redis (always-free)
· broker: Redpanda Serverless trial (recorded demo) or always-free VM — decide
with Fikri based on accounts. Document costs honestly (Rp 0 target).

### Phase 6 — Ship
GitHub push + CI green · wiki ingest · Career Profile + MOC · portfolio
website card · GitHub profile README · gap-analysis page updated (3 Tier-1
gaps → closed).

## 2. Repo layout

```
streaming-fraud-detection/
├── RESEARCH.md · PLAN.md · README.md
├── docker-compose.yml            ← redpanda, console, redis, 4 services
├── src/
│   ├── config.py                 ← topics, windows, paths, thresholds
│   ├── schema.py                 ← transaction/feature/score event models (pydantic)
│   ├── simulator.py              ← producer
│   ├── processor.py              ← Quix Streams windowed features → Redis
│   ├── scorer.py                 ← consumer + LightGBM + alerts
│   └── bench.py                  ← T1/T3/T4 measurement scripts
├── api/main.py                   ← FastAPI + SSE dashboard
├── models/                       ← copied lgbm_fraud.joblib + meta (committed)
├── deploy/                       ← Cloud Run configs + notes
├── tests/  ·  .github/workflows/ci.yml  ·  reports/
```

## 3. Risks

| Risk | Mitigation |
|:--|:--|
| Docker Desktop not running/installed on the Windows box | check first; Redpanda also runs via WSL2 |
| Quix Streams Windows quirks (RocksDB state dir) | state dir pinned to project path; fallback: tumbling windows via plain dict state |
| 24-feature model expects offline-engineered inputs the stream can't derive | map what's derivable; document the online/offline feature contract explicitly (this *is* the real-world lesson) |
| Cloud broker has no always-free option | two-profile strategy (RESEARCH §2); recorded demo acceptable |
| Laptop perf vs T1 target | targets pre-registered; report honestly whatever falls |

## 4. Definition of Done

- [ ] Full local stack via one `docker compose up`
- [ ] T1–T4 verdicts with numbers in `reports/results.md`
- [ ] Tests + flake8 + CI green
- [ ] Cloud profile deployed (or recorded + documented if trial-based)
- [ ] Wiki + portfolio + GitHub profile updated
