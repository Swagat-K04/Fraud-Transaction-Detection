"""
consumer.py — Kafka consumer with XGBoost fraud detection.
"""

import os
import json
import logging
import asyncio
import signal
import sys
from datetime import datetime, timezone, date

import asyncpg
import redis.asyncio as aioredis
from confluent_kafka import Consumer, KafkaError

from features import compute_features_dict
from model import get_model
from threshold import get_threshold, init_threshold

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "creditcard.transactions")
POSTGRES_URL    = os.getenv("POSTGRES_URL", "postgresql://fraud:fraud@localhost:5432/frauddb")
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("consumer")

CATEGORY_STATS = {
    "grocery_pos":    (80.0,  70.0),
    "gas_transport":  (55.0,  35.0),
    "home":           (200.0, 200.0),
    "shopping_net":   (300.0, 400.0),
    "shopping_pos":   (120.0, 120.0),
    "food_dining":    (45.0,  40.0),
    "health_fitness": (80.0,  80.0),
    "entertainment":  (80.0,  120.0),
    "travel":         (800.0, 1200.0),
    "personal_care":  (50.0,  40.0),
    "kids_pets":      (90.0,  90.0),
    "misc_net":       (500.0, 2000.0),
    "misc_pos":       (80.0,  120.0),
}

running = True


async def get_velocity(redis_client, cc_num: str) -> int:
    key = f"vel:{cc_num}:{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}"
    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.expire(key, 65)
    await pipe.execute()
    counts = await redis_client.mget(key)
    return sum(int(c) for c in counts if c)


async def persist_transaction(pool, tx, features, prediction, reasoning, msg_partition, msg_offset):
    async with pool.acquire() as conn:
        # Upsert customer stub
        await conn.execute(
            """
            INSERT INTO customers (cc_num, cust_lat, cust_long, dob)
            VALUES ($1, $2, $3, $4::date)
            ON CONFLICT (cc_num) DO NOTHING
            """,
            tx["cc_num"],
            float(tx.get("cust_lat", 0)),
            float(tx.get("cust_long", 0)),
            (lambda s: datetime.strptime(s, "%Y-%m-%d").date() if s else date(1980, 1, 1))(tx.get("dob", "1980-01-01")),
        )

        # Insert transaction
        await conn.execute(
            """
            INSERT INTO transactions (
                trans_num, cc_num, trans_time, category, merchant, amt,
                merch_lat, merch_long,
                age, distance, hour_of_day, day_of_week, amt_zscore, tx_velocity_1h,
                is_fraud, fraud_score, risk_level, ai_reasoning,
                kafka_partition, kafka_offset
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14,
                $15, $16, $17, $18, $19, $20
            )
            ON CONFLICT (trans_num) DO NOTHING
            """,
            tx["trans_num"],
            tx["cc_num"],
            datetime.fromisoformat(tx["trans_time"].replace("Z", "+00:00")),
            tx["category"],
            tx["merchant"],
            float(tx["amt"]),
            float(tx["merch_lat"]),
            float(tx["merch_long"]),
            int(features["age"]),
            float(features["distance"]),
            int(features["hour_of_day"]),
            int(features["day_of_week"]),
            float(features["amt_zscore"]),
            int(features["tx_velocity_1h"]),
            bool(prediction["is_fraud"]),
            float(prediction["fraud_score"]),
            prediction["risk_level"],
            reasoning,
            msg_partition,
            msg_offset,
        )

        # Track Kafka offset for exactly-once semantics
        await conn.execute(
            """
            INSERT INTO kafka_offsets (topic, kafka_partition, kafka_offset)
            VALUES ($1, $2, $3)
            ON CONFLICT (topic, kafka_partition) DO UPDATE
            SET kafka_offset = EXCLUDED.kafka_offset, updated_at = NOW()
            """,
            KAFKA_TOPIC,
            msg_partition,
            msg_offset,
        )


async def publish_to_redis(redis_client, tx, prediction, reasoning):
    payload = json.dumps({
        "trans_num":    tx["trans_num"],
        "cc_num":       tx["cc_num"][-4:],
        "merchant":     tx["merchant"],
        "category":     tx["category"],
        "amt":          float(tx["amt"]),
        "trans_time":   tx["trans_time"],
        "is_fraud":     prediction["is_fraud"],
        "fraud_score":  prediction["fraud_score"],
        "risk_level":   prediction["risk_level"],
        "reasoning":    reasoning,
        "top_features": prediction.get("top_features", []),
    })
    channel = "fraud.alerts" if prediction["is_fraud"] else "tx.stream"
    await redis_client.publish(channel, payload)
    await redis_client.publish("tx.all", payload)
    # Include threshold in payload so dashboard knows what threshold was used
    


async def process_message(msg, pg_pool, redis_client, model):
    try:
        tx = json.loads(msg.value().decode("utf-8"))
    except Exception as e:
        log.warning("Failed to parse message: %s", e)
        return

    msg_partition = msg.partition()
    msg_offset    = msg.offset()

    # Extract ground truth if present (Kaggle replay mode only — for accuracy logging)
    ground_truth = tx.pop("_ground_truth", None)

    # Build features — model.py handles both kaggle and synthetic formats
    cat = tx.get("category", "misc_net")
    cat_mean, cat_std = CATEGORY_STATS.get(cat, (100.0, 100.0))
    velocity = await get_velocity(redis_client, tx.get("cc_num", ""))

    # Pass full tx to features — includes V1-V28 if kaggle mode
    features = compute_features_dict(tx, velocity, cat_mean, cat_std)

    # Also pass raw V1-V28 fields directly for kaggle model
    for i in range(1, 29):
        k = f"V{i}"
        if k in tx:
            features[k] = float(tx[k])
    if "Amount" in tx:
        features["Amount_scaled"] = float(tx["Amount"]) / 1000.0
    if "Time" in tx:
        features["Time_scaled"] = float(tx["Time"]) / 172800.0

    # Read live threshold from Redis (cached, refreshes every 10 txns)
    threshold  = await get_threshold(redis_client)
    prediction = model.predict(features, threshold=threshold)

    # Log accuracy when ground truth is available (Kaggle replay mode)
    if ground_truth is not None:
        correct = (prediction["is_fraud"] == bool(ground_truth))
        match   = "✓ CORRECT" if correct else "✗ WRONG  "
        if ground_truth == 1 or not correct:
            log.info("ACCURACY %s | predicted=%s actual=%s score=%.3f",
                     match,
                     "FRAUD" if prediction["is_fraud"] else "LEGIT",
                     "FRAUD" if ground_truth == 1 else "LEGIT",
                     prediction["fraud_score"])

    try:
        from explainer import explain_async, _rule_based_explanation
        reasoning = await asyncio.wait_for(
            explain_async(tx, prediction, features),
            timeout=5.0
        )
    except Exception:
        from explainer import _rule_based_explanation
        reasoning = _rule_based_explanation(tx, prediction, features)

    await asyncio.gather(
        persist_transaction(pg_pool, tx, features, prediction, reasoning, msg_partition, msg_offset),
        publish_to_redis(redis_client, tx, prediction, reasoning),
    )

    label = "FRAUD" if prediction["is_fraud"] else "LEGIT"
    log.info("TX %-16s | %-28s | $%8.2f | %s (%.0f%%) | %s",
             tx.get("trans_num", "?")[:16],
             tx.get("merchant",  "?")[:28],
             float(tx.get("amt", 0)),
             label,
             prediction["fraud_score"] * 100,
             prediction["risk_level"])


async def main_async():
    global running

    log.info("Loading ML model...")
    model = get_model()
    if not model.is_loaded():
        log.error("Model not loaded — run: docker compose run --rm consumer python train.py")
        sys.exit(1)

    log.info("Connecting to PostgreSQL and Redis...")
    pg_pool = await asyncpg.create_pool(
        POSTGRES_URL.replace("postgresql://", "postgres://"),
        min_size=2, max_size=10, command_timeout=15
    )
    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
    await init_threshold(redis_client)

    kafka_conf = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           "fraud-detection-consumer",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(kafka_conf)
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Kafka consumer subscribed to topic: %s", KAFKA_TOPIC)
    log.info("Streaming fraud detection started — waiting for transactions...")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            await process_message(msg, pg_pool, redis_client, model)
            consumer.commit(message=msg, asynchronous=False)

    finally:
        consumer.close()
        await pg_pool.close()
        await redis_client.aclose()
        log.info("Consumer shut down cleanly")


def main():
    global running

    def _shutdown(sig, frame):
        global running
        log.info("Shutting down gracefully...")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    asyncio.run(main_async())


if __name__ == "__main__":
    main()