"""
producer.py — Kafka credit card transaction producer.

Replaces the original Scala Kafka producer. Streams realistic synthetic
transactions into the `creditcard.transactions` topic at configurable rate.
Supports replay from CSV (for training) and live generation.
"""

import os
import csv
import json
import time
import signal
import logging
import random
import sys
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from generator import Customer, generate_customers, generate_transaction, to_kafka_message

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "creditcard.transactions")
INTERVAL_MS      = int(os.getenv("STREAM_INTERVAL_MS", "2000"))
DATA_DIR         = Path(os.getenv("DATA_DIR", "/app/data"))
MODE             = os.getenv("PRODUCER_MODE", "live")   # live | replay

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("producer")

# ─── Kafka producer config ────────────────────────────────────────────────────
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",                     # wait for all replicas
    "retries": 5,
    "retry.backoff.ms": 500,
    "linger.ms": 10,                   # micro-batching for throughput
    "compression.type": "lz4",
    "enable.idempotence": True,        # exactly-once production
}

running = True

def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed | topic=%s partition=%d offset=%d err=%s",
                  msg.topic(), msg.partition(), msg.offset(), err)
    else:
        log.debug("Delivered | partition=%d offset=%d key=%s",
                  msg.partition(), msg.offset(), msg.key())


def _partition_key(tx_dict: dict) -> bytes:
    """Partition by cc_num so all transactions for a card go to the same partition."""
    return tx_dict["cc_num"].encode()


def load_customers_from_csv() -> list[Customer]:
    path = DATA_DIR / "customers.csv"
    if not path.exists():
        log.warning("customers.csv not found — generating synthetic customers")
        customers = generate_customers(100)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["cc_num","first","last","gender","street","city",
                             "state","zip","lat","long","job","dob"])
            for c in customers:
                writer.writerow([c.cc_num, c.first, c.last, c.gender, c.street,
                                 c.city, c.state, c.zip, c.lat, c.long, c.job, c.dob])
        return customers

    customers = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            customers.append(Customer(
                cc_num=row["cc_num"], first=row["first"], last=row["last"],
                gender=row["gender"], street=row["street"], city=row["city"],
                state=row["state"], zip=row["zip"],
                lat=float(row["lat"]), long=float(row["long"]),
                job=row["job"], dob=row["dob"],
            ))
    log.info("Loaded %d customers from CSV", len(customers))
    return customers


def replay_from_csv(producer: Producer, customers: list[Customer]):
    """Replay transactions from the testing CSV (mirrors original Kafka producer behaviour)."""
    path = DATA_DIR / "transactions_testing.csv"
    if not path.exists():
        log.error("transactions_testing.csv not found — falling back to live mode")
        return live_stream(producer, customers)

    with open(path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    log.info("Replaying %d transactions from CSV", len(rows))
    for row in rows:
        if not running:
            break
        # Build a message matching live schema (without is_fraud label for inference)
        msg = {k: v for k, v in row.items() if k != "is_fraud"}
        producer.produce(
            topic=KAFKA_TOPIC,
            key=row["cc_num"].encode(),
            value=json.dumps(msg).encode(),
            callback=_delivery_report,
        )
        producer.poll(0)
        time.sleep(INTERVAL_MS / 1000)

    producer.flush()
    log.info("Replay complete")


def live_stream(producer: Producer, customers: list[Customer]):
    """Continuously generate and produce live synthetic transactions."""
    log.info("Starting live transaction stream → topic=%s interval=%dms",
             KAFKA_TOPIC, INTERVAL_MS)
    sent = 0
    while running:
        customer = random.choice(customers)
        tx = generate_transaction(customer)
        msg = to_kafka_message(tx, include_label=False)

        producer.produce(
            topic=KAFKA_TOPIC,
            key=_partition_key(json.loads(msg)),
            value=msg.encode(),
            callback=_delivery_report,
        )
        producer.poll(0)   # trigger delivery callbacks
        sent += 1

        if sent % 100 == 0:
            log.info("Produced %d transactions", sent)

        time.sleep(INTERVAL_MS / 1000)

    producer.flush(timeout=10)
    log.info("Producer shut down after %d messages", sent)


def main():
    global running

    def _shutdown(sig, frame):
        global running
        log.info("Received signal %s — shutting down", sig)
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    customers = load_customers_from_csv()
    producer = Producer(PRODUCER_CONFIG)

    log.info("Kafka producer connected → %s", KAFKA_BOOTSTRAP)

    if MODE == "replay":
        replay_from_csv(producer, customers)
    else:
        live_stream(producer, customers)


if __name__ == "__main__":
    main()
