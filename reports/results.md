# Results — Pre-Registered Targets (measured 2026-06-11, local Profile L)

Stack under test: Redpanda v24.3 (dev mode, 1 vCPU/1GB) + Quix Streams
processor + micro-batched LightGBM scorer + Redis 7, all on one Windows
laptop via docker-compose. Simulator: Sparkov test split, keyed by card.

## T1 — End-to-end latency (produce → score), 5,000 events @ 100 TPS

| iteration | p50 | p95 | p99 | max | verdict |
|:--|--:|--:|--:|--:|:--|
| v1 naive scorer (1 msg / predict) | 12,915 ms | 22,599 | 23,087 | 23,238 | ❌ unbounded growth — pipeline capped at ~68 TPS |
| v2 **micro-batching** (≤256 msgs / one vectorized `predict_proba`) | 298 ms | 534 | 559 | 582 | ❌ flat but above target |
| v3 + **`fetch.wait.max.ms` 500→50** on both consumers | **102 ms** | **160** | **172** | 219 | ✅ **PASS** (target p99 < 250 ms) |

Lessons: (1) per-message pandas+predict is a throughput killer — batch the
model; (2) librdkafka's default 500 ms fetch wait silently dominates e2e
latency at low/medium TPS.

## T3 — Replay determinism (the log as source of truth)

First attempt **FAILED honestly**: 505/875 cards diverged between live state
and a clean replay. Root causes: (a) at-least-once redelivery after hard
kills double-appended events into per-card history; (b) dataset replays made
event time jump backwards, corrupting append-assumes-ordered windows
(negative `secs_since_prev_txn`).

Fix in `features_core`: **idempotent fold by `txn_id`** + **sorted insertion**
of history. After the fix: two independent replays from offset 0
(fresh consumer groups + state dirs) → **875/875 cards byte-identical** ✅.

## T4 — Graceful degradation (Redis down)

Redis container paused for the middle 15 s of a 1,500-event burst @ 100 TPS.
**1,500/1,500 scores still produced** to the `scores` topic ✅ — Redis is
observability + online store only, never the scoring path. The online store
self-heals on the next event per card; `/score` API falls back to neutral
features and flags `degraded_mode: true`.

## T2 — Online/offline feature parity

Covered by unit tests in `tests/test_features.py` (Phase 4) — same
`features_core` math validated against hand-computed window values and the
batch project's semantics (windows inclusive of current txn, lifetime mean
computed pre-fold).

## Bonus war story — the poison pill

The very first pipeline run died instantly: a leftover smoke-test message at
offset 0 failed schema validation and crashed the consumer group. Fix: a
validation guard that skips malformed events and counts them
(`metrics:poison`) instead of dying. One bad message must never stop the
stream.
