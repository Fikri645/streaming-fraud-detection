"""Central configuration for the streaming fraud-detection stack."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = ROOT / "models"
REPORTS_DIR = ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# --------------------------------------------------------------------------- #
# Kafka / Redpanda
# --------------------------------------------------------------------------- #
# Host-side default; inside compose the services get KAFKA_BROKER=redpanda:9092
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:19092")

TOPIC_TRANSACTIONS = "transactions"
TOPIC_FEATURES = "features"
TOPIC_SCORES = "scores"
TOPIC_ALERTS = "alerts"

# GROUP_SUFFIX lets the T3 replay test spin up a fresh consumer group +
# state dir that rebuilds everything from offset 0.
_SUFFIX = os.environ.get("GROUP_SUFFIX", "")
CONSUMER_GROUP_PROCESSOR = "feature-processor" + _SUFFIX
CONSUMER_GROUP_SCORER = "fraud-scorer" + _SUFFIX
STATE_DIR = Path(__file__).resolve().parents[1] / ("state" + _SUFFIX)

# --------------------------------------------------------------------------- #
# Redis online feature store
# --------------------------------------------------------------------------- #
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY_PREFIX = "card:"           # card:<cc_num> -> hash of online features
REDIS_TTL_SECONDS = 2 * 3600         # online features expire after 2h idle

# --------------------------------------------------------------------------- #
# Streaming feature windows (per card) — must match the model's contract
# --------------------------------------------------------------------------- #
WINDOWS_SECONDS = {"1h": 3600, "24h": 86400, "7d": 604800}

# Features the processor computes ONLINE (stateful, per card, event-time):
ONLINE_FEATURES = [
    "txn_count_1h", "txn_count_24h", "txn_count_7d",
    "amt_sum_1h", "amt_sum_24h", "amt_sum_7d",
    "amt_mean_24h", "secs_since_prev_txn",
    "amt_dev_from_card_mean", "amt_ratio_to_card_mean",
    "distinct_merchants_24h", "dist_from_prev_txn_km",
]
# Features carried on the event itself (computed at the edge / stateless):
EVENT_FEATURES = [
    "amt", "amt_log", "hour", "day_of_week", "is_night", "is_weekend",
    "age", "city_pop_log", "dist_home_merchant_km",
    "category", "gender", "state",
]
CATEGORICAL_FEATURES = ["category", "gender", "state"]

# --------------------------------------------------------------------------- #
# Model (reused from the fraud-detection project)
# --------------------------------------------------------------------------- #
MODEL_PATH = MODELS_DIR / "lgbm_fraud.joblib"
MODEL_META_PATH = MODELS_DIR / "model_meta.json"
ALERT_THRESHOLD = 0.0187             # cost-optimal threshold from batch project

# --------------------------------------------------------------------------- #
# Source data for the simulator (the batch project's held-out test split)
# --------------------------------------------------------------------------- #
SOURCE_TEST_PARQUET = Path(
    os.environ.get(
        "SOURCE_TEST_PARQUET",
        r"E:/!!!Project/fraud-detection/data/processed/features_test.parquet",
    )
)
SIMULATOR_DEFAULT_TPS = 50
