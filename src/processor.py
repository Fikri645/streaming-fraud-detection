"""Stream processor — Quix Streams stateful enrichment.

``transactions`` topic → per-card online features (event-time windows via
``features_core``) → ``features`` topic + Redis online store (latest feature
vector per card, TTL'd). State lives in Quix's per-key RocksDB store, keyed by
the Kafka message key (cc_num) — so partition reassignment moves state
correctly and a topic replay rebuilds it from scratch (target T3).

Run:  python -m src.processor
"""
from __future__ import annotations

import time

import redis as redis_lib
from quixstreams import Application

from src import config
from src.features_core import compute_online_features, new_state
from src.schema import FeatureEvent, Transaction

_redis = redis_lib.Redis.from_url(config.REDIS_URL, decode_responses=True)


def enrich(value: dict, state) -> dict | None:
    """Enrich one event; malformed events are skipped (poison-pill guard).

    A single bad message must never kill the consumer group — the very first
    run of this pipeline died on a leftover smoke-test message at offset 0.
    Skipped events are counted in Redis (``metrics:poison``) for visibility.
    """
    try:
        txn = Transaction(**value)
    except Exception:
        _redis.incr("metrics:poison")
        return None

    card_state = state.get("s") or new_state()
    feats = compute_online_features(card_state, txn.ts, txn.amt,
                                    txn.merchant, txn.txn_id)
    state.set("s", card_state)

    event = FeatureEvent(txn=txn, processed_at=time.time(), **feats)

    # online store: latest features per card (what /score reads on demand).
    # Redis being down must not stop the stream (T4): features still flow to
    # the topic; the online store is simply stale until Redis returns.
    try:
        _redis.hset(config.REDIS_KEY_PREFIX + txn.cc_num,
                    mapping={k: str(v) for k, v in feats.items()})
        _redis.expire(config.REDIS_KEY_PREFIX + txn.cc_num,
                      config.REDIS_TTL_SECONDS)
    except redis_lib.exceptions.RedisError:
        pass

    return event.model_dump()


def main() -> None:
    app = Application(
        broker_address=config.KAFKA_BROKER,
        consumer_group=config.CONSUMER_GROUP_PROCESSOR,
        auto_offset_reset="earliest",
        state_dir=str(config.STATE_DIR),
        # librdkafka defaults fetch.wait.max.ms to 500ms — at low/medium TPS
        # that wait dominates e2e latency (T1 tuning finding).
        consumer_extra_config={"fetch.wait.max.ms": 50},
        producer_extra_config={"linger.ms": 5},
    )
    topic_in = app.topic(config.TOPIC_TRANSACTIONS, value_deserializer="json")
    topic_out = app.topic(config.TOPIC_FEATURES, value_serializer="json")

    sdf = app.dataframe(topic_in)
    sdf = sdf.apply(enrich, stateful=True)
    sdf = sdf.filter(lambda v: v is not None)
    sdf.to_topic(topic_out)

    print(f"[processor] consuming {config.TOPIC_TRANSACTIONS} "
          f"-> {config.TOPIC_FEATURES} @ {config.KAFKA_BROKER}")
    app.run()


if __name__ == "__main__":
    main()
