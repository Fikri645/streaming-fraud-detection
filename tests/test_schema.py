"""Event-contract tests: valid events pass, poison pills fail loudly."""
import pytest
from pydantic import ValidationError

from src.schema import ScoreEvent, Transaction

VALID = dict(
    txn_id="abc123", cc_num="4111", merchant="shop_x", ts=1_700_000_000.0,
    amt=42.5, category="grocery_pos", gender="F", state="CA", age=30.0,
    city_pop_log=9.2, dist_home_merchant_km=3.1, dist_from_prev_txn_km=0.5,
    is_fraud=0, produced_at=1_700_000_000.5,
)


def test_valid_transaction_roundtrip():
    txn = Transaction(**VALID)
    assert Transaction.model_validate_json(txn.model_dump_json()) == txn


def test_poison_pill_rejected():
    """The exact message that killed the first pipeline run."""
    with pytest.raises(ValidationError):
        Transaction(**{"hello": "stream", "ts": 1781184864.29})


def test_negative_amount_rejected():
    with pytest.raises(ValidationError):
        Transaction(**{**VALID, "amt": -1.0})


def test_score_event_latency_property():
    ev = ScoreEvent(txn_id="t", cc_num="c", amt=1.0, score=0.5, is_alert=True,
                    is_fraud=0, produced_at=100.0, processed_at=100.05,
                    scored_at=100.125)
    assert ev.e2e_latency_ms == pytest.approx(125.0)
