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


def build_row(ev: FeatureEvent) -> pd.DataFrame:
    """Assemble the model's 24-feature row from event + online features."""
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
    row = pd.DataFrame([{**base, **online}])
    for c in config.CATEGORICAL_FEATURES:
        row[c] = row[c].astype("category")
    return row[FEATURE_ORDER]


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BROKER,
        "group.id": config.CONSUMER_GROUP_SCORER,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": config.KAFKA_BROKER,
                         "linger.ms": 5})
    consumer.subscribe([config.TOPIC_FEATURES])
    print(f"[scorer] consuming {config.TOPIC_FEATURES} @ {config.KAFKA_BROKER}")

    n = 0
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        ev = FeatureEvent(**json.loads(msg.value()))
        score = float(MODEL.predict_proba(build_row(ev))[0, 1])
        out = ScoreEvent(
            txn_id=ev.txn.txn_id, cc_num=ev.txn.cc_num, amt=ev.txn.amt,
            score=score, is_alert=score >= config.ALERT_THRESHOLD,
            is_fraud=ev.txn.is_fraud,
            produced_at=ev.txn.produced_at, processed_at=ev.processed_at,
            scored_at=time.time(),
        )
        payload = out.model_dump_json().encode()
        producer.produce(config.TOPIC_SCORES, key=out.cc_num.encode(),
                         value=payload)
        if out.is_alert:
            producer.produce(config.TOPIC_ALERTS, key=out.cc_num.encode(),
                             value=payload)

        # rolling latency + counters for the dashboard
        pipe = _redis.pipeline()
        pipe.lpush("metrics:latency", round(out.e2e_latency_ms, 2))
        pipe.ltrim("metrics:latency", 0, 9999)
        pipe.incr("metrics:scored")
        if out.is_alert:
            pipe.incr("metrics:alerts")
        pipe.execute()

        n += 1
        if n % 500 == 0:
            producer.poll(0)
            print(f"[scorer] scored={n} last_latency={out.e2e_latency_ms:.0f}ms")


if __name__ == "__main__":
    main()
