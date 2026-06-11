"""Event contracts for every topic — pydantic models, one per stream stage.

The schema *is* the interface between services; events validate at the
boundary so malformed data fails loudly instead of corrupting state
downstream.

Design note — what travels on the raw event vs what the processor computes:
the simulator emits only what a payment edge could know at swipe time
(amount, card, merchant, timestamps, geo, static card attributes). All
**stateful** features — rolling counts/sums/means per card, seconds since
previous transaction, lifetime-mean ratios, distinct merchants — are computed
*online* by the stream processor. The batch project computed those same
features offline, so online-vs-offline parity is directly testable (target T2).
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class Transaction(BaseModel):
    """Raw event on the ``transactions`` topic (simulator output)."""

    txn_id: str
    cc_num: str                      # card identifier (partition key)
    merchant: str
    ts: float                        # EVENT time (unix seconds, from dataset)
    amt: float = Field(ge=0)

    # static / edge-computable attributes carried with the event
    category: str
    gender: str
    state: str
    age: float
    city_pop_log: float
    dist_home_merchant_km: float
    dist_from_prev_txn_km: float     # terminal-side geo displacement

    is_fraud: int = Field(ge=0, le=1)    # ground truth, kept for evaluation
    produced_at: float               # wall-clock at produce (latency tracking)


class FeatureEvent(BaseModel):
    """Enriched event on the ``features`` topic: txn + the online features."""

    txn: Transaction

    txn_count_1h: float = 0
    txn_count_24h: float = 0
    txn_count_7d: float = 0
    amt_sum_1h: float = 0.0
    amt_sum_24h: float = 0.0
    amt_sum_7d: float = 0.0
    amt_mean_24h: float = 0.0
    secs_since_prev_txn: float = -1.0    # -1 = first txn seen for this card
    amt_dev_from_card_mean: float = 0.0
    amt_ratio_to_card_mean: float = 1.0
    distinct_merchants_24h: float = 0

    processed_at: float = 0.0


class ScoreEvent(BaseModel):
    """Scored event on the ``scores`` (and ``alerts``) topics."""

    txn_id: str
    cc_num: str
    amt: float
    score: float
    is_alert: bool
    is_fraud: int                        # ground truth, for live evaluation
    produced_at: float
    processed_at: float
    scored_at: float

    @property
    def e2e_latency_ms(self) -> float:
        return (self.scored_at - self.produced_at) * 1000.0
