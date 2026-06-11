"""Pure online-feature math — no broker, no Redis, fully unit-testable.

The processor keeps, per card, a bounded history of past transactions
(timestamp, amount, merchant) plus lifetime aggregates, and calls
:func:`compute_online_features` for every new event. The same function is
used by the T2 parity test against the batch project's offline values.
"""
from __future__ import annotations

from typing import Any

from src import config

# per-card state shape kept in the processor's state store:
# {"hist": [[ts, amt, merchant], ...] (asc, trimmed to 7d),
#  "life_n": int, "life_sum": float}


def new_state() -> dict[str, Any]:
    return {"hist": [], "life_n": 0, "life_sum": 0.0}


def compute_online_features(state: dict[str, Any], ts: float, amt: float,
                            merchant: str, txn_id: str = "") -> dict[str, float]:
    """Return the 11 stateful features for the txn at ``ts``, then fold it in.

    Windows are event-time and **inclusive** of the current transaction,
    matching the batch project's rolling computation.

    Two hard-won robustness properties (added after the first T3 replay
    failed with 505/875 cards diverging):

    * **Idempotent by txn_id** — under at-least-once delivery a crash between
      state flush and offset commit redelivers events; append-only state
      double-counts them. Re-seen txn_ids fold in as a no-op.
    * **Out-of-order tolerant** — history is kept sorted by event time
      (insertion sort), so a txn arriving late (or a dataset replayed from
      the start) cannot corrupt window math or yield negative
      ``secs_since_prev_txn``.
    """
    import bisect

    hist = state["hist"]

    # idempotence: the same txn_id must not change state twice
    if txn_id and any(r[3] == txn_id for r in hist if len(r) > 3):
        return _aggregate(hist, ts, amt, state)

    # lifetime (card mean) is computed *before* this txn, like the batch code
    life_n, life_sum = state["life_n"], state["life_sum"]
    card_mean = (life_sum / life_n) if life_n else amt
    amt_dev = amt - card_mean
    amt_ratio = amt / card_mean if card_mean else 1.0

    bisect.insort(hist, [ts, amt, merchant, txn_id], key=lambda r: r[0])
    # trim anything older than the widest window, relative to the NEWEST event
    horizon = hist[-1][0] - config.WINDOWS_SECONDS["7d"]
    while hist and hist[0][0] < horizon:
        hist.pop(0)

    state["life_n"] = life_n + 1
    state["life_sum"] = life_sum + amt

    feats = _aggregate(hist, ts, amt, state)
    feats["amt_dev_from_card_mean"] = float(amt_dev)
    feats["amt_ratio_to_card_mean"] = float(amt_ratio)
    return feats


def _aggregate(hist: list, ts: float, amt: float,
               state: dict[str, Any]) -> dict[str, float]:
    """Window aggregates over the (sorted) history, inclusive of ``ts``."""
    feats: dict[str, float] = {}
    for name, secs in config.WINDOWS_SECONDS.items():
        lo = ts - secs
        rows = [r for r in hist if lo <= r[0] <= ts]
        feats[f"txn_count_{name}"] = float(len(rows))
        feats[f"amt_sum_{name}"] = float(sum(r[1] for r in rows))

    rows_24h = [r for r in hist
                if ts - config.WINDOWS_SECONDS["24h"] <= r[0] <= ts]
    feats["amt_mean_24h"] = feats["amt_sum_24h"] / max(len(rows_24h), 1)
    feats["distinct_merchants_24h"] = float(len({r[2] for r in rows_24h}))

    # previous event strictly before this txn's event time
    prev = [r[0] for r in hist if r[0] < ts]
    feats["secs_since_prev_txn"] = float(ts - prev[-1]) if prev else -1.0

    # duplicate-delivery path: lifetime mean already includes this txn
    life_n, life_sum = state["life_n"], state["life_sum"]
    card_mean = (life_sum / life_n) if life_n else amt
    feats["amt_dev_from_card_mean"] = float(amt - card_mean)
    feats["amt_ratio_to_card_mean"] = float(amt / card_mean) if card_mean else 1.0
    return feats
