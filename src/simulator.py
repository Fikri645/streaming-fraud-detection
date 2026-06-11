"""Transaction simulator — replays the batch project's held-out test split
into the ``transactions`` topic as raw swipe-time events.

Event time comes from the dataset; wall-clock ``produced_at`` is stamped for
end-to-end latency measurement. Keyed by ``cc_num`` so a card's events always
land on the same partition (ordering guarantee the processor's state needs).

Usage:
    python -m src.simulator --tps 50 --limit 20000
    python -m src.simulator --tps 200 --limit 50000 --burst-fraud
"""
from __future__ import annotations

import argparse
import json
import time
import uuid

import pandas as pd
from confluent_kafka import Producer

from src import config
from src.schema import Transaction


def load_events(limit: int, burst_fraud: bool) -> pd.DataFrame:
    df = pd.read_parquet(config.SOURCE_TEST_PARQUET)
    df = df.sort_values("trans_date_trans_time").head(limit).copy()
    if burst_fraud:                      # oversample fraud rows into a burst
        fraud = df[df["is_fraud"] == 1]
        df = pd.concat([df, fraud, fraud]).sort_values("trans_date_trans_time")
    df["ts"] = pd.to_datetime(df["trans_date_trans_time"]).astype("int64") / 1e9
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tps", type=float, default=config.SIMULATOR_DEFAULT_TPS)
    ap.add_argument("--limit", type=int, default=20_000)
    ap.add_argument("--burst-fraud", action="store_true")
    args = ap.parse_args()

    df = load_events(args.limit, args.burst_fraud)
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BROKER,
        "linger.ms": 5,
        "compression.type": "lz4",
    })

    interval = 1.0 / args.tps
    sent = 0
    t_start = time.perf_counter()
    for row in df.itertuples(index=False):
        txn = Transaction(
            txn_id=uuid.uuid4().hex[:16],
            cc_num=str(row.cc_num),
            merchant=str(row.merchant),
            ts=float(row.ts),
            amt=float(row.amt),
            category=str(row.category),
            gender=str(row.gender),
            state=str(row.state),
            age=float(row.age),
            city_pop_log=float(row.city_pop_log),
            dist_home_merchant_km=float(row.dist_home_merchant_km),
            dist_from_prev_txn_km=float(row.dist_from_prev_txn_km),
            is_fraud=int(row.is_fraud),
            produced_at=time.time(),
        )
        producer.produce(config.TOPIC_TRANSACTIONS, key=txn.cc_num.encode(),
                         value=txn.model_dump_json().encode())
        sent += 1
        if sent % 500 == 0:
            producer.poll(0)
            elapsed = time.perf_counter() - t_start
            print(f"[sim] sent={sent}  actual_tps={sent / elapsed:.1f}")
        # pace to target tps
        target = t_start + sent * interval
        delay = target - time.perf_counter()
        if delay > 0:
            time.sleep(delay)

    producer.flush(30)
    elapsed = time.perf_counter() - t_start
    print(json.dumps({"sent": sent, "seconds": round(elapsed, 1),
                      "tps": round(sent / elapsed, 1)}))


if __name__ == "__main__":
    main()
