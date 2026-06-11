"""T2 — online feature math: window semantics, idempotence, ordering.

These are the properties the replay test (T3) depends on; they run with no
broker, no Redis, no model.
"""
from src.features_core import compute_online_features, new_state

H = 3600.0
T0 = 1_700_000_000.0


def _fold(state, ts, amt, merchant="m1", txn_id=""):
    return compute_online_features(state, ts, amt, merchant,
                                   txn_id or f"id-{ts}-{amt}")


def test_windows_inclusive_of_current_txn():
    s = new_state()
    f = _fold(s, T0, 100.0)
    assert f["txn_count_1h"] == 1
    assert f["amt_sum_1h"] == 100.0
    assert f["secs_since_prev_txn"] == -1.0     # first txn for the card


def test_window_boundaries_1h_24h():
    s = new_state()
    _fold(s, T0, 10.0)                  # 25h before t2 -> only in 7d window
    _fold(s, T0 + H, 20.0)              # 24h before t2 -> in 24h window
    f = _fold(s, T0 + 25 * H, 30.0)
    assert f["txn_count_1h"] == 1       # just itself
    assert f["txn_count_24h"] == 2      # itself + the one exactly 24h ago
    assert f["txn_count_7d"] == 3
    assert f["amt_sum_24h"] == 50.0
    assert f["amt_sum_7d"] == 60.0


def test_lifetime_mean_computed_before_fold():
    s = new_state()
    _fold(s, T0, 100.0)
    f = _fold(s, T0 + 60, 300.0)        # card mean BEFORE this txn = 100
    assert f["amt_dev_from_card_mean"] == 200.0
    assert f["amt_ratio_to_card_mean"] == 3.0


def test_distinct_merchants_24h():
    s = new_state()
    _fold(s, T0, 10.0, merchant="a")
    _fold(s, T0 + 60, 10.0, merchant="b")
    f = _fold(s, T0 + 120, 10.0, merchant="a")
    assert f["distinct_merchants_24h"] == 2


def test_idempotent_by_txn_id():
    """Redelivered events (at-least-once) must not change state."""
    s = new_state()
    _fold(s, T0, 100.0, txn_id="tx1")
    f1 = _fold(s, T0 + 60, 50.0, txn_id="tx2")
    f2 = _fold(s, T0 + 60, 50.0, txn_id="tx2")      # redelivery
    assert f1["txn_count_1h"] == f2["txn_count_1h"] == 2
    assert s["life_n"] == 2                          # not 3
    f3 = _fold(s, T0 + 120, 10.0, txn_id="tx3")
    assert f3["txn_count_1h"] == 3                   # tx2 counted once


def test_out_of_order_event_time():
    """A late event must not produce negative secs or corrupt windows."""
    s = new_state()
    _fold(s, T0 + 600, 10.0, txn_id="late-arrives-first")
    f = _fold(s, T0, 20.0, txn_id="early-arrives-late")
    assert f["secs_since_prev_txn"] == -1.0          # nothing BEFORE T0
    assert f["txn_count_1h"] == 1                    # window is [T0-1h, T0]
    f2 = _fold(s, T0 + 1200, 30.0, txn_id="tx3")
    assert f2["txn_count_1h"] == 3                   # all three within 1h
    assert f2["secs_since_prev_txn"] == 600.0        # vs T0+600, sorted


def test_trim_keeps_only_7d():
    s = new_state()
    _fold(s, T0, 10.0)
    _fold(s, T0 + 8 * 24 * H, 20.0)                  # 8 days later
    assert len(s["hist"]) == 1                       # old row trimmed
