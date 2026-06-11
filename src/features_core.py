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
                            merchant: str) -> dict[str, float]:
    """Return the 11 stateful features for the txn at ``ts``, then fold it in.

    Windows are event-time, *exclusive of the current txn* for counts/sums of
    "previous activity" semantics? — No: the batch project computed windows
    **inclusive** of the current transaction (rolling on the row itself), so
    we match that: append first, then aggregate.
    """
    hist = state["hist"]
    prev_ts = hist[-1][0] if hist else None

    # lifetime (card mean) is computed *before* this txn, like the batch code
    life_n, life_sum = state["life_n"], state["life_sum"]
    card_mean = (life_sum / life_n) if life_n else amt
    amt_dev = amt - card_mean
    amt_ratio = amt / card_mean if card_mean else 1.0

    hist.append([ts, amt, merchant])
    # trim anything older than the widest window
    horizon = ts - config.WINDOWS_SECONDS["7d"]
    while hist and hist[0][0] < horizon:
        hist.pop(0)

    feats: dict[str, float] = {}
    for name, secs in config.WINDOWS_SECONDS.items():
        lo = ts - secs
        rows = [r for r in hist if r[0] >= lo]
        feats[f"txn_count_{name}"] = float(len(rows))
        feats[f"amt_sum_{name}"] = float(sum(r[1] for r in rows))

    rows_24h = [r for r in hist if r[0] >= ts - config.WINDOWS_SECONDS["24h"]]
    feats["amt_mean_24h"] = feats["amt_sum_24h"] / max(len(rows_24h), 1)
    feats["distinct_merchants_24h"] = float(len({r[2] for r in rows_24h}))
    feats["secs_since_prev_txn"] = float(ts - prev_ts) if prev_ts else -1.0
    feats["amt_dev_from_card_mean"] = float(amt_dev)
    feats["amt_ratio_to_card_mean"] = float(amt_ratio)

    state["life_n"] = life_n + 1
    state["life_sum"] = life_sum + amt
    return feats
