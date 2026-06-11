# Real-Time Streaming Fraud Detection — 2026 Stack Research

> Compiled 2026-06-11, before any code. Driven by the job-market gap analysis:
> **Cloud + Kafka/streaming + real-time** appeared in every Indonesian
> engineer-level JD scanned (Glints/Jobstreet/LinkedIn — SPE Solution, Kopi
> Janji Jiwa, Flip, PrimaVista), and none of the 22 portfolio projects covers
> them. Budget constraint: **free**.

---

## 1. The Stack Decisions

### Broker: Redpanda (Kafka-compatible), not Apache Kafka

- Single binary, no ZooKeeper/JVM, starts in ms, fraction of Kafka's memory —
  the consensus 2026 choice for Docker-based dev
  ([OneUptime guide](https://oneuptime.com/blog/post/2026-02-08-how-to-run-redpanda-kafka-compatible-in-docker/view),
  [The New Stack](https://thenewstack.io/data-streaming-when-is-redpanda-better-than-apache-kafka/)).
- **100% Kafka API compatible** — every JD says "Kafka"; any Kafka client
  works unchanged, so the skill transfers 1:1
  ([Redpanda vs Kafka](https://www.redpanda.com/compare/redpanda-vs-kafka)).
- Built-in console UI + schema registry + HTTP proxy in one container.

### Stream processor: Quix Streams (pure-Python), Kafka client: confluent-kafka

- **Faust is dead** (unmaintained); PyFlink/PySpark are JVM wrappers — heavy
  for a laptop and awkward to debug
  ([Quix vs Faust](https://quix.io/blog/quix-streams-faust-alternative),
  [RisingWave 2026 guide](https://risingwave.com/blog/stream-processing-python-practical-guide/)).
- **Quix Streams**: pure Python, pandas-like streaming DataFrame API, native
  stateful windowing (tumbling/hopping/sliding) over Kafka topics, RocksDB
  state, exactly-the-ecosystem ML libs need
  ([Gang Tao's series](https://taogang.medium.com/the-past-and-present-of-stream-processing-part-23-python-native-ultra-fast-streaming-with-quix-8fdc83946ab8)).
- Bytewax (Rust-bridged) is the main alternative (TCO ~4.6× lower than Flink
  per [their bench](https://bytewax.io/blog/going-head-to-head-against-flink/));
  Quix wins here for the pandas-style API + simpler deploy.

### Online feature store: Redis (plain), Feast out of scope

- Every JD names **Redis** directly (SPE: "Redis"; AI Engineer roles: "Redis").
  None name Feast. Plain Redis hash/sorted-set windowed aggregates show the
  *mechanism*; Feast adds infra without adding interview signal.

### Serving: FastAPI + SSE live dashboard

- Consistent with the rest of the portfolio; the dashboard consumes the scored
  stream and shows TPS, fraud alerts, and p50/p99 latency live.

## 2. Free Cloud Reality Check (the hard part)

| Option | Status 2026 | Verdict |
|:--|:--|:--|
| **Upstash Kafka** | **DISCONTINUED** (deprecated Sep 2024, dead Mar 2025 — [official](https://upstash.com/blog/workflow-kafka)) | ❌ do not plan on it |
| **Redpanda Serverless** | $100 credit / 14-day trial ([pricing](https://www.redpanda.com/product/serverless)) | ⚠️ good for a recorded cloud demo, not always-on |
| **Confluent Cloud** | $50/mo off × 3 months ([pricing](https://www.confluent.io/confluent-cloud/pricing/)) | ⚠️ same: trial-grade |
| **GCP Cloud Run** | **Always-free**: 2M req/mo, 360K vCPU-sec/mo, renews monthly ([pricing](https://cloud.google.com/run/pricing)) | ✅ scoring API + dashboard |
| **Upstash Redis** | **Always-free**: 256MB, 500K commands/mo, no card ([pricing](https://upstash.com/pricing/redis)) | ✅ online feature store |
| **GCP e2-micro VM** | Always-free (1 vCPU shared, 1GB) | ⚠️ Redpanda dev-mode *might* fit (`--smp 1 --memory 400M`) — tight |
| **Oracle Cloud Always Free** | 4 ARM OCPU + 24GB RAM, free forever | ✅ can run the whole compose 24/7 — best always-on option, but signup friction (card verification) |

**Deploy strategy (two profiles, both honest):**
- **Profile L (local-prod)**: full stack via docker-compose — the engineering
  artifact reviewers actually read.
- **Profile C (cloud)**: Cloud Run (scoring API + dashboard, always-free) +
  Upstash Redis (always-free) + broker on either Redpanda Serverless trial
  (recorded demo + screenshots) or an always-free VM. Decided at deploy time
  based on which accounts Fikri has/wants.

## 3. Architecture

```
simulator (Sparkov test stream, controllable TPS + fraud bursts)
   │  produce JSON
   ▼
Redpanda topic: transactions
   │
   ▼
Quix Streams processor  ──────────────►  Redis online store
   stateful sliding windows per card:      (latest features per card)
   txn count / amount sum-avg (1m,5m,1h),
   seconds-since-last, geo-velocity
   │  enriched event
   ▼
Redpanda topic: features ──► scorer consumer (LightGBM, reused
   │                          fraud-detection model, threshold 0.0187)
   ▼
Redpanda topics: scores, alerts (score ≥ threshold)
   │
   ▼
FastAPI: /score (on-demand w/ Redis features), /health, /metrics
   + SSE live dashboard (TPS, alert feed, p50/p99 end-to-end latency)
```

**Reused asset:** `fraud-detection`'s LightGBM (PR-AUC 0.9666, 24 features,
cost-optimal threshold 0.0187). The old project's `simulate_stream.py` was an
*in-process* loop (117 TPS, p50 8ms, no broker) — that becomes the explicit
baseline this project beats on *architecture*, not speed: durable topics,
replayable streams, independent scaling, exactly-once-ish semantics.

## 4. Pre-Registered Targets (state before building)

- **T1**: end-to-end p99 latency (produce → score) **< 250 ms** at 100 TPS
  sustained on the laptop compose stack.
- **T2**: feature parity — online (streamed) features match offline (batch)
  computation on the same data within float tolerance, proven by a test.
- **T3**: replay — wipe Redis, replay the topic from offset 0, end state
  identical (the Kafka "log as source of truth" story).
- **T4**: graceful degradation — scorer keeps serving (model-only features)
  when Redis is down; measured accuracy delta documented.

## 5. Sources

- [Redpanda in Docker (OneUptime, Feb 2026)](https://oneuptime.com/blog/post/2026-02-08-how-to-run-redpanda-kafka-compatible-in-docker/view) · [Redpanda vs Kafka](https://www.redpanda.com/compare/redpanda-vs-kafka) · [The New Stack comparison](https://thenewstack.io/data-streaming-when-is-redpanda-better-than-apache-kafka/)
- [Quix vs Faust](https://quix.io/blog/quix-streams-faust-alternative) · [Python stream processing 2026 (RisingWave)](https://risingwave.com/blog/stream-processing-python-practical-guide/) · [Bytewax vs Flink TCO](https://bytewax.io/blog/going-head-to-head-against-flink/)
- [Upstash Kafka deprecation (official)](https://upstash.com/blog/workflow-kafka) · [Redpanda Serverless](https://www.redpanda.com/product/serverless) · [Confluent pricing](https://www.confluent.io/confluent-cloud/pricing/)
- [Cloud Run pricing/free tier](https://cloud.google.com/run/pricing) · [Upstash Redis pricing](https://upstash.com/pricing/redis)
