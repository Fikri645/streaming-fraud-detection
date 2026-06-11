"""Scorer — consumes enriched ``features`` events, scores with the reused
LightGBM model, emits ``scores`` (all) and ``alerts`` (score >= threshold).

Latency accounting: every ScoreEvent carries produced_at / processed_at /
scored_at, and a rolling latency histogram is pushed to Redis for the
dashboard (key ``metrics:latency``, a capped list).

Run:  python -m src.scorer
"""
from __future__ import annotations

import json
import time

import joblib
import pandas as pd
import redis as redis_lib
from confluent_kafka import Consumer, Producer

from src import config
from src.schema import FeatureEvent, ScoreEvent

_redis = redis_lib.Redis.from_url(config.REDIS_URL, decode_responses=True)

MODEL = joblib.load(config.MODEL_PATH)
FEATURE_ORDER = list(MODEL.feature_name_)


def event_to_dict(ev: FeatureEvent) -> dict:
    """One model-feature dict from event + online features."""
    import math

    txn = ev.txn
    dt = pd.Timestamp(txn.ts, unit="s")
    base = {
        "amt": txn.amt,
        "amt_log": math.log1p(txn.amt),
        "hour": dt.hour,
        "day_of_week": dt.dayofweek,
        "is_night": int(dt.hour < 6 or dt.hour >= 22),
        "is_weekend": int(dt.dayofweek >= 5),
        "age": txn.age,
        "city_pop_log": txn.city_pop_log,
        "dist_home_merchant_km": txn.dist_home_merchant_km,
        "dist_from_prev_txn_km": txn.dist_from_prev_txn_km,
        "category": txn.category,
        "gender": txn.gender,
        "state": txn.state,
    }
    online = {k: getattr(ev, k) for k in config.ONLINE_FEATURES
              if k != "dist_from_prev_txn_km"}
    return {**base, **online}


def build_rows(events: list[FeatureEvent]) -> pd.DataFrame:
    """Vectorized: one DataFrame + one predict for the whole micro-batch."""
    rows = pd.DataFrame([event_to_dict(ev) for ev in events])
    for c in config.CATEGORICAL_FEATURES:
        rows[c] = rows[c].astype("category")
    return rows[FEATURE_ORDER]


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BROKER,
        "group.id": config.CONSUMER_GROUP_SCORER,
        "auto.offset.reset": "earliest",
        "fetch.wait.max.ms": 50,      # default 500ms dominates e2e latency
    })
    producer = Producer({"bootstrap.servers": config.KAFKA_BROKER,
                         "linger.ms": 5})
    consumer.subscribe([config.TOPIC_FEATURES])
    print(f"[scorer] consuming {config.TOPIC_FEATURES} @ {config.KAFKA_BROKER}")

    # MICRO-BATCHING: the first bench (T1) failed hard — one-message-at-a-time
    # pandas + predict_proba capped throughput at ~68 TPS, so at 100 TPS input
    # the consumer lag (and thus e2e latency) grew without bound. Consuming up
    # to BATCH messages and scoring them with ONE vectorized predict_proba
    # call moves the per-event model cost from ~15 ms to well under 1 ms.
    BATCH = 256
    n = 0
    while True:
        msgs = consumer.consume(num_messages=BATCH, timeout=0.1)
        msgs = [m for m in msgs if m is not None and not m.error()]
        if not msgs:
            continue
        events = [FeatureEvent(**json.loads(m.value())) for m in msgs]
        scores = MODEL.predict_proba(build_rows(events))[:, 1]
        now = time.time()

        latencies, n_alerts = [], 0
        for ev, score in zip(events, scores):
            out = ScoreEvent(
                txn_id=ev.txn.txn_id, cc_num=ev.txn.cc_num, amt=ev.txn.amt,
                score=float(score),
                is_alert=float(score) >= config.ALERT_THRESHOLD,
                is_fraud=ev.txn.is_fraud,
                produced_at=ev.txn.produced_at, processed_at=ev.processed_at,
                scored_at=now,
            )
            payload = out.model_dump_json().encode()
            producer.produce(config.TOPIC_SCORES, key=out.cc_num.encode(),
                             value=payload)
            if out.is_alert:
                n_alerts += 1
                producer.produce(config.TOPIC_ALERTS, key=out.cc_num.encode(),
                                 value=payload)
            latencies.append(round(out.e2e_latency_ms, 2))
        producer.poll(0)

        # Redis here is OBSERVABILITY, not the scoring path — if it's down
        # the scorer must keep producing scores (degradation target T4).
        try:
            pipe = _redis.pipeline()
            pipe.lpush("metrics:latency", *latencies)
            pipe.ltrim("metrics:latency", 0, 9999)
            pipe.incrby("metrics:scored", len(events))
            if n_alerts:
                pipe.incrby("metrics:alerts", n_alerts)
            pipe.execute()
        except redis_lib.exceptions.RedisError:
            pass

        n += len(events)
        if n % 2000 < BATCH:
            print(f"[scorer] scored={n} batch={len(events)} "
                  f"last_latency={latencies[-1]:.0f}ms", flush=True)


if __name__ == "__main__":
    main()
