"""Benchmark + verification helpers for the pre-registered targets T1/T3/T4.

Services (processor + scorer) are expected to be RUNNING and drained;
orchestration (starting services, pausing Redis, replay groups) happens in
the shell — these subcommands do the measuring.

  python -m src.bench t1 --n 5000 --tps 100     # steady-state e2e latency
  python -m src.bench snapshot --out a.json     # dump online store (T3)
  python -m src.bench compare a.json b.json     # diff two snapshots (T3)
  python -m src.bench count-scores --seconds 30 # scores produced in window (T4)
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

import redis as redis_lib
from confluent_kafka import Consumer, TopicPartition

from src import config

_redis = redis_lib.Redis.from_url(config.REDIS_URL, decode_responses=True)


def t1(n: int, tps: float) -> None:
    _redis.delete("metrics:latency", "metrics:scored", "metrics:alerts")
    print(f"[t1] producing {n} events @ {tps} TPS ...")
    subprocess.run([sys.executable, "-m", "src.simulator",
                    "--tps", str(tps), "--limit", str(n)],
                   check=True, cwd=config.ROOT)
    deadline = time.time() + 120
    while time.time() < deadline:
        scored = int(_redis.get("metrics:scored") or 0)
        if scored >= n:
            break
        time.sleep(1)
    scored = int(_redis.get("metrics:scored") or 0)
    lat = sorted(float(x) for x in _redis.lrange("metrics:latency", 0, n - 1))
    res = {
        "n_produced": n, "n_scored": scored, "tps_target": tps,
        "p50_ms": round(lat[len(lat) // 2], 1),
        "p95_ms": round(lat[int(len(lat) * 0.95)], 1),
        "p99_ms": round(lat[min(int(len(lat) * 0.99), len(lat) - 1)], 1),
        "max_ms": round(lat[-1], 1),
        "alerts": int(_redis.get("metrics:alerts") or 0),
        "t1_target_p99_ms": 250,
        "t1_pass": lat[min(int(len(lat) * 0.99), len(lat) - 1)] < 250,
    }
    (config.REPORTS_DIR / "t1_latency.json").write_text(json.dumps(res, indent=1))
    print(json.dumps(res, indent=1))


def snapshot(out: str) -> None:
    data = {}
    for key in sorted(_redis.scan_iter(config.REDIS_KEY_PREFIX + "*", count=1000)):
        h = _redis.hgetall(key)
        data[key] = {k: round(float(v), 6) for k, v in sorted(h.items())}
    with open(out, "w") as f:
        json.dump(data, f, indent=1, sort_keys=True)
    print(f"[snapshot] {len(data)} cards -> {out}")


def compare(a: str, b: str) -> None:
    da, db = json.load(open(a)), json.load(open(b))
    if da == db:
        print(f"T3 PASS: snapshots identical ({len(da)} cards)")
        return
    only_a = set(da) - set(db)
    only_b = set(db) - set(da)
    diff = [k for k in set(da) & set(db) if da[k] != db[k]]
    print(f"T3 FAIL: only_a={len(only_a)} only_b={len(only_b)} "
          f"value_diffs={len(diff)}")
    for k in diff[:3]:
        print(" ", k, {f: (da[k][f], db[k][f]) for f in da[k]
                       if da[k].get(f) != db[k].get(f)})
    sys.exit(1)


def count_scores(seconds: int) -> None:
    """Count messages arriving on ``scores`` during the next N seconds."""
    c = Consumer({"bootstrap.servers": config.KAFKA_BROKER,
                  "group.id": f"bench-count-{int(time.time())}",
                  "auto.offset.reset": "latest"})
    parts = [TopicPartition(config.TOPIC_SCORES, p)
             for p in c.list_topics(config.TOPIC_SCORES, timeout=10)
             .topics[config.TOPIC_SCORES].partitions]
    for tp in parts:
        lo, hi = c.get_watermark_offsets(tp, timeout=10)
        tp.offset = hi
    c.assign(parts)
    n = 0
    end = time.time() + seconds
    while time.time() < end:
        msg = c.poll(0.5)
        if msg is not None and not msg.error():
            n += 1
    c.close()
    print(json.dumps({"window_seconds": seconds, "scores_received": n}))


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p1 = sub.add_parser("t1")
    p1.add_argument("--n", type=int, default=5000)
    p1.add_argument("--tps", type=float, default=100)
    ps = sub.add_parser("snapshot")
    ps.add_argument("--out", required=True)
    pc = sub.add_parser("compare")
    pc.add_argument("a")
    pc.add_argument("b")
    p4 = sub.add_parser("count-scores")
    p4.add_argument("--seconds", type=int, default=30)
    args = ap.parse_args()

    if args.cmd == "t1":
        t1(args.n, args.tps)
    elif args.cmd == "snapshot":
        snapshot(args.out)
    elif args.cmd == "compare":
        compare(args.a, args.b)
    elif args.cmd == "count-scores":
        count_scores(args.seconds)


if __name__ == "__main__":
    main()
