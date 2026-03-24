"""
producer.py — Kafka transaction producer.

Supports two modes:
  live   — generates synthetic transactions (Phase 1)
  kaggle — replays real Kaggle creditcard.csv rows as live transactions
            (strips the Class label so consumer must predict it)
            This is Phase 2: the model scores transactions that actually
            came from the same distribution it was trained on.
"""

import os
import csv
import json
import time
import signal
import logging
import random
import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "creditcard.transactions")
INTERVAL_MS      = int(os.getenv("STREAM_INTERVAL_MS", "2000"))
DATA_DIR         = Path(os.getenv("DATA_DIR", "/app/data"))
MODE             = os.getenv("PRODUCER_MODE", "live")

PRODUCER_CONFIG  = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks":              "all",
    "retries":           5,
    "linger.ms":         10,
    "compression.type":  "lz4",
    "enable.idempotence": True,
}

running = True

# Synthetic merchant names for display (Kaggle has no merchant info)
MERCHANTS = [
    "Amazon", "Walmart", "Shell Gas", "Delta Airlines", "Netflix",
    "Starbucks", "Apple Store", "Uber", "McDonald's", "Target",
    "Crypto Exchange Pro", "Unknown Merchant", "PayQuick Transfer",
    "Luxury Watches Ltd", "International Wire Co",
]
CATEGORIES = [
    "shopping_net", "grocery_pos", "gas_transport", "travel",
    "entertainment", "food_dining", "shopping_pos", "misc_net",
]


def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


# ─── Mode 1: Kaggle replay ────────────────────────────────────────────────────

def load_kaggle_rows(path: Path) -> list[dict]:
    """Load all rows from creditcard.csv into memory, strip Class label."""
    log.info("Loading Kaggle dataset from %s ...", path)
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Strip the label — consumer must predict it
            # Keep it internally only for ground-truth logging
            rows.append(row)
    log.info("Loaded %d Kaggle transactions", len(rows))
    return rows


def kaggle_stream(producer: Producer, rows: list[dict]):
    """
    Replay Kaggle rows as live transactions.
    Adds a fresh timestamp and transaction ID each time.
    Shuffles so fraud is distributed throughout the stream.
    """
    log.info("Starting Kaggle replay stream → topic=%s interval=%dms",
             KAFKA_TOPIC, INTERVAL_MS)

    # Shuffle once so fraud cases are spread out, not all at end
    shuffled = rows.copy()
    random.shuffle(shuffled)

    sent = fraud_sent = 0

    while running:
        # Loop back to start when exhausted
        if sent >= len(shuffled):
            random.shuffle(shuffled)
            sent = 0
            log.info("Replay loop complete — reshuffling and restarting")

        row = shuffled[sent]

        # Build the message — V1-V28 + Amount + Time + metadata
        # Strip Class label (ground truth) from the Kafka message
        ground_truth = int(float(row.get("Class", 0)))

        msg = {
            "trans_num":  uuid.uuid4().hex[:20].upper(),
            "trans_time": datetime.now(timezone.utc).isoformat(),
            # Kaggle PCA features
            **{f"V{i}": float(row[f"V{i}"]) for i in range(1, 29)},
            "Amount":     float(row["Amount"]),
            "Time":       float(row["Time"]),
            # Synthetic display fields (not used by model, just for dashboard UI)
            "cc_num":     str(random.randint(1000000000000000, 9999999999999999)),
            "merchant":   random.choice(MERCHANTS),
            "category":   random.choice(CATEGORIES),
            "amt":        float(row["Amount"]),   # alias for dashboard display
            "cust_lat":   round(random.uniform(35.0, 52.0), 6),
            "cust_long":  round(random.uniform(-120.0, -70.0), 6),
            "merch_lat":  round(random.uniform(35.0, 52.0), 6),
            "merch_long": round(random.uniform(-120.0, -70.0), 6),
            "dob":        "1985-06-15",
            # Include ground truth ONLY for accuracy tracking (consumer logs it but doesn't use it for prediction)
            "_ground_truth": ground_truth,
        }

        producer.produce(
            topic=KAFKA_TOPIC,
            key=msg["cc_num"].encode(),
            value=json.dumps(msg).encode(),
            callback=_delivery_report,
        )
        producer.poll(0)

        sent += 1
        if ground_truth == 1:
            fraud_sent += 1

        if sent % 100 == 0:
            log.info("Streamed %d transactions (%d fraud, %.2f%%)",
                     sent, fraud_sent, fraud_sent / sent * 100)

        time.sleep(INTERVAL_MS / 1000)

    producer.flush(timeout=10)
    log.info("Kaggle stream ended after %d messages", sent)


# ─── Mode 2: Synthetic live generation ───────────────────────────────────────

def live_stream(producer: Producer):
    """Original synthetic transaction generator (Phase 1 fallback)."""
    sys.path.insert(0, str(Path(__file__).parent))
    from generator import generate_customers, generate_transaction, to_kafka_message

    customers = generate_customers(100)
    log.info("Starting live synthetic stream → topic=%s interval=%dms",
             KAFKA_TOPIC, INTERVAL_MS)

    sent = 0
    while running:
        customer = random.choice(customers)
        tx       = generate_transaction(customer)
        msg      = to_kafka_message(tx, include_label=False)

        producer.produce(
            topic=KAFKA_TOPIC,
            key=tx.cc_num.encode(),
            value=msg.encode(),
            callback=_delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % 100 == 0:
            log.info("Produced %d synthetic transactions", sent)

        time.sleep(INTERVAL_MS / 1000)

    producer.flush(timeout=10)
    log.info("Producer shut down after %d messages", sent)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    global running

    def _shutdown(sig, frame):
        global running
        log.info("Shutting down producer...")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    producer = Producer(PRODUCER_CONFIG)
    log.info("Kafka producer connected → %s | mode=%s", KAFKA_BOOTSTRAP, MODE)

    if MODE == "kaggle":
        kaggle_path = DATA_DIR / "creditcard.csv"
        if not kaggle_path.exists():
            log.warning("creditcard.csv not found — falling back to synthetic mode")
            live_stream(producer)
        else:
            rows = load_kaggle_rows(kaggle_path)
            kaggle_stream(producer, rows)
    else:
        live_stream(producer)


if __name__ == "__main__":
    main()