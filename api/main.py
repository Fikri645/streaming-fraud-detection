"""FastAPI serving layer + live SSE dashboard.

* ``GET /``        — live dashboard (TPS, alerts, latency percentiles)
* ``GET /events``  — SSE stream of metrics (1s cadence, from Redis)
* ``POST /score``  — on-demand scoring: online features for the card are read
                     from Redis; if Redis is down or the card is unseen, the
                     model scores with neutral defaults (degraded mode, T4)
* ``GET /health``  — liveness + dependency status
* ``GET /metrics`` — one-shot JSON of the same numbers the dashboard shows

Run:  uvicorn api.main:app --port 8000
"""
from __future__ import annotations

import asyncio
import json
import math
import time

import joblib
import pandas as pd
import redis as redis_lib
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from src import config

app = FastAPI(title="Streaming Fraud Detection", version="1.0")
_redis = redis_lib.Redis.from_url(config.REDIS_URL, decode_responses=True)
MODEL = joblib.load(config.MODEL_PATH)
FEATURE_ORDER = list(MODEL.feature_name_)

NEUTRAL_ONLINE = {
    "txn_count_1h": 1.0, "txn_count_24h": 1.0, "txn_count_7d": 1.0,
    "amt_sum_1h": 0.0, "amt_sum_24h": 0.0, "amt_sum_7d": 0.0,
    "amt_mean_24h": 0.0, "secs_since_prev_txn": -1.0,
    "amt_dev_from_card_mean": 0.0, "amt_ratio_to_card_mean": 1.0,
    "distinct_merchants_24h": 1.0,
}


class ScoreRequest(BaseModel):
    cc_num: str
    amt: float = Field(ge=0)
    category: str = "misc_net"
    gender: str = "M"
    state: str = "NY"
    age: float = 35.0
    city_pop_log: float = 10.0
    dist_home_merchant_km: float = 5.0
    dist_from_prev_txn_km: float = 0.0


def _metrics_snapshot() -> dict:
    try:
        lat = sorted(float(x) for x in _redis.lrange("metrics:latency", 0, 9999))
        n = len(lat)
        return {
            "scored": int(_redis.get("metrics:scored") or 0),
            "alerts": int(_redis.get("metrics:alerts") or 0),
            "poison": int(_redis.get("metrics:poison") or 0),
            "cards": sum(1 for _ in _redis.scan_iter(config.REDIS_KEY_PREFIX + "*",
                                                     count=1000)),
            "latency_p50_ms": round(lat[n // 2], 1) if n else None,
            "latency_p99_ms": round(lat[min(int(n * 0.99), n - 1)], 1) if n else None,
            "redis": "up",
        }
    except redis_lib.exceptions.RedisError:
        return {"redis": "down"}


@app.get("/health")
def health():
    out = {"status": "ok", "model": config.MODEL_PATH.name}
    try:
        _redis.ping()
        out["redis"] = "up"
    except redis_lib.exceptions.RedisError:
        out["redis"] = "down"
    return out


@app.get("/metrics")
def metrics():
    return _metrics_snapshot()


@app.post("/score")
def score(req: ScoreRequest):
    degraded = False
    try:
        feats = _redis.hgetall(config.REDIS_KEY_PREFIX + req.cc_num)
    except redis_lib.exceptions.RedisError:
        feats = {}
    if feats:
        online = {k: float(v) for k, v in feats.items()}
    else:
        online, degraded = dict(NEUTRAL_ONLINE), True

    now = pd.Timestamp.now()
    row = pd.DataFrame([{
        "amt": req.amt, "amt_log": math.log1p(req.amt),
        "hour": now.hour, "day_of_week": now.dayofweek,
        "is_night": int(now.hour < 6 or now.hour >= 22),
        "is_weekend": int(now.dayofweek >= 5),
        "age": req.age, "city_pop_log": req.city_pop_log,
        "dist_home_merchant_km": req.dist_home_merchant_km,
        "dist_from_prev_txn_km": req.dist_from_prev_txn_km,
        "category": req.category, "gender": req.gender, "state": req.state,
        **online,
    }])
    for c in config.CATEGORICAL_FEATURES:
        row[c] = row[c].astype("category")
    prob = float(MODEL.predict_proba(row[FEATURE_ORDER])[0, 1])
    return {"cc_num": req.cc_num, "score": round(prob, 6),
            "is_alert": prob >= config.ALERT_THRESHOLD,
            "threshold": config.ALERT_THRESHOLD,
            "degraded_mode": degraded, "scored_at": time.time()}


@app.get("/events")
async def events():
    async def gen():
        prev_scored = 0
        while True:
            snap = _metrics_snapshot()
            scored = snap.get("scored", 0) or 0
            snap["tps"] = max(0, scored - prev_scored)
            prev_scored = scored
            yield {"data": json.dumps(snap)}
            await asyncio.sleep(1)
    return EventSourceResponse(gen())


DASH = """<!doctype html><html><head><title>Streaming Fraud Detection</title>
<style>
 body{font-family:system-ui;background:#0f0f14;color:#e5e5e5;margin:2rem}
 h1{color:#a78bfa} .grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1rem}
 .card{background:#1b1b24;border-radius:10px;padding:1.2rem}
 .v{font-size:2.2rem;font-weight:700;color:#a78bfa} .l{color:#9ca3af;font-size:.85rem}
 .alert .v{color:#f87171} .ok .v{color:#34d399}
</style></head><body>
<h1>🛡️ Streaming Fraud Detection — live</h1>
<div class=grid>
 <div class=card><div class=v id=tps>—</div><div class=l>scored / sec</div></div>
 <div class=card><div class=v id=scored>—</div><div class=l>total scored</div></div>
 <div class=card class=alert><div class=v id=alerts>—</div><div class=l>fraud alerts</div></div>
 <div class=card><div class=v id=p50>—</div><div class=l>p50 latency (ms)</div></div>
 <div class=card><div class=v id=p99>—</div><div class=l>p99 latency (ms)</div></div>
 <div class=card><div class=v id=cards>—</div><div class=l>cards in online store</div></div>
</div>
<p class=l>Redis: <span id=redis>—</span> · poison skipped: <span id=poison>—</span>
 · Redpanda console: <a href="http://localhost:8088" style="color:#a78bfa">:8088</a></p>
<script>
 const es = new EventSource('/events');
 es.onmessage = e => { const d = JSON.parse(e.data);
  for (const k of ['tps','scored','alerts','p50','p99','cards','redis','poison']) {
    const m = {p50:'latency_p50_ms', p99:'latency_p99_ms'}[k] || k;
    const el = document.getElementById(k);
    if (el && d[m] !== undefined && d[m] !== null) el.textContent = d[m];
  }};
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASH
