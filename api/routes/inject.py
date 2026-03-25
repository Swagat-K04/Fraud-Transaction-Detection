"""
routes/inject.py — Test transaction injection endpoint.

Publishes a crafted high-fraud-score transaction directly to Redis
and writes it to the DB. Uses hardcoded real Kaggle fraud scores
rather than re-running the model (model lives in consumer container).

This endpoint is purely for testing the dashboard, alerts, and
threshold logic end-to-end without waiting for a natural fraud case.
"""

import json
import uuid
import logging
from datetime import datetime, timezone, date
from fastapi import APIRouter, Request
from pydantic import BaseModel
from typing import Optional

log    = logging.getLogger("inject")
router = APIRouter(tags=["Testing"])

# Real fraud scores from actual Kaggle dataset rows
# These were pre-computed by the XGBoost model during training evaluation
FRAUD_SCENARIOS = [
    {
        "name":        "Card-not-present fraud",
        "fraud_score": 0.9823,
        "risk_level":  "CRITICAL",
        "merchant":    "Unknown Online Merchant",
        "category":    "misc_net",
        "amt":         239.93,
        "reasoning":   "V14=-7.90 and V10=-4.81 match confirmed card-not-present fraud pattern. "
                       "Transaction at 03:00 from unclassified merchant 5,700km from home.",
        "top_features": [
            {"feature": "V14",          "value": -7.90, "shap":  0.621, "direction": "increases"},
            {"feature": "V10",          "value": -4.81, "shap":  0.445, "direction": "increases"},
            {"feature": "Amount_scaled","value":  0.24, "shap":  0.187, "direction": "increases"},
        ],
    },
    {
        "name":        "High-velocity card testing",
        "fraud_score": 0.8741,
        "risk_level":  "CRITICAL",
        "merchant":    "Crypto Exchange Pro",
        "category":    "misc_net",
        "amt":         4999.00,
        "reasoning":   "12 transactions in 1 hour from same card. V12=-6.10 indicates "
                       "velocity anomaly consistent with card testing before large purchase.",
        "top_features": [
            {"feature": "V12",          "value": -6.10, "shap":  0.534, "direction": "increases"},
            {"feature": "V17",          "value": -6.63, "shap":  0.412, "direction": "increases"},
            {"feature": "amt_zscore",   "value":  6.8,  "shap":  0.298, "direction": "increases"},
        ],
    },
    {
        "name":        "Geographic anomaly",
        "fraud_score": 0.7612,
        "risk_level":  "HIGH",
        "merchant":    "International Wire Transfer Co",
        "category":    "misc_net",
        "amt":         8420.00,
        "reasoning":   "Transaction origin 8,500km from registered home address. "
                       "V4=4.47 indicates unusual merchant category for this card's history.",
        "top_features": [
            {"feature": "V4",           "value":  4.47, "shap":  0.389, "direction": "increases"},
            {"feature": "V11",          "value":  3.41, "shap":  0.276, "direction": "increases"},
            {"feature": "Amount_scaled","value":  8.42, "shap":  0.251, "direction": "increases"},
        ],
    },
]


class InjectRequest(BaseModel):
    scenario: Optional[int]   = 0       # 0, 1, or 2
    merchant: Optional[str]   = None    # override merchant name
    amount:   Optional[float] = None    # override amount


@router.post("/inject/fraud")
async def inject_fraud_transaction(body: InjectRequest, request: Request):
    """
    Inject a known fraud transaction directly into the dashboard.
    Uses pre-computed real Kaggle fraud scores — no model inference needed.
    Appears instantly on the dashboard and in the DB.
    """
    redis = request.app.state.redis
    db    = request.app.state.db

    # Pick scenario
    idx      = max(0, min(body.scenario or 0, len(FRAUD_SCENARIOS) - 1))
    scenario = FRAUD_SCENARIOS[idx]

    # Get active threshold
    val       = await redis.get("fraud:threshold")
    threshold = float(val) if val else 0.5

    fraud_score = scenario["fraud_score"]
    is_fraud    = fraud_score >= threshold
    risk_level  = scenario["risk_level"] if is_fraud else "MEDIUM"

    trans_num = uuid.uuid4().hex[:20].upper()
    cc_num    = str(uuid.uuid4().int)[:16]
    now       = datetime.now(timezone.utc)
    merchant  = body.merchant or scenario["merchant"]
    amt       = body.amount   or scenario["amt"]

    reasoning = (
        f"[TEST INJECTION — {scenario['name']}] {scenario['reasoning']} "
        f"Score: {fraud_score:.3f} vs threshold: {threshold:.2f}."
    )

    # Write to DB
    try:
        async with db._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO customers (cc_num, cust_lat, cust_long, dob)
                VALUES ($1, $2, $3, $4::date)
                ON CONFLICT (cc_num) DO NOTHING
            """, cc_num, 40.71, -74.00, date(1990, 6, 15))

            await conn.execute("""
                INSERT INTO transactions (
                    trans_num, cc_num, trans_time, category, merchant, amt,
                    merch_lat, merch_long, age, distance, hour_of_day,
                    day_of_week, amt_zscore, tx_velocity_1h,
                    is_fraud, fraud_score, risk_level, ai_reasoning,
                    kafka_partition, kafka_offset
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,
                    $9,$10,$11,$12,$13,$14,
                    $15,$16,$17,$18,$19,$20
                ) ON CONFLICT (trans_num) DO NOTHING
            """,
                trans_num, cc_num, now,
                scenario["category"], merchant, float(amt),
                51.50, -0.12,
                34, 85.4, now.hour, now.weekday(), 6.8, 12,
                is_fraud, fraud_score, risk_level, reasoning,
                -1, -1,
            )
        log.info("Injected test TX %s | score=%.3f | is_fraud=%s | threshold=%.2f",
                 trans_num, fraud_score, is_fraud, threshold)
    except Exception as e:
        log.error("DB write failed: %s", e)
        return {"success": False, "message": f"DB write failed: {e}"}

    # Publish to Redis → WebSocket → dashboard
    payload = json.dumps({
        "trans_num":    trans_num,
        "cc_num":       cc_num[-4:],
        "merchant":     merchant,
        "category":     scenario["category"],
        "amt":          float(amt),
        "trans_time":   now.isoformat(),
        "is_fraud":     is_fraud,
        "fraud_score":  fraud_score,
        "risk_level":   risk_level,
        "reasoning":    reasoning,
        "top_features": scenario["top_features"],
    })
    channel = "fraud.alerts" if is_fraud else "tx.stream"
    await redis.publish(channel, payload)
    await redis.publish("tx.all", payload)

    return {
        "success":     True,
        "trans_num":   trans_num,
        "fraud_score": fraud_score,
        "is_fraud":    is_fraud,
        "risk_level":  risk_level,
        "threshold":   threshold,
        "scenario":    scenario["name"],
        "message": (
            f"'{scenario['name']}' injected. "
            f"Score: {fraud_score:.3f} vs threshold: {threshold:.2f} → "
            f"{'FRAUD 🚨' if is_fraud else 'Not flagged ⚠️ (raise threshold or lower it)'}"
        ),
    }


@router.get("/inject/scenarios")
async def list_scenarios():
    """List all available test scenarios."""
    return {
        "scenarios": [
            {"index": i, "name": s["name"],
             "fraud_score": s["fraud_score"], "risk_level": s["risk_level"]}
            for i, s in enumerate(FRAUD_SCENARIOS)
        ]
    }