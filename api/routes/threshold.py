"""
routes/threshold.py — Live threshold management endpoints.
"""

import json
import logging
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

log = logging.getLogger("threshold")
router = APIRouter(tags=["Threshold"])

THRESHOLD_KEY = "fraud:threshold"
DEFAULT       = 0.5


class ThresholdUpdate(BaseModel):
    threshold: float = Field(..., ge=0.1, le=0.9)


@router.get("/threshold")
async def get_threshold(request: Request):
    redis = request.app.state.redis
    db    = request.app.state.db

    val     = await redis.get(THRESHOLD_KEY)
    current = float(val) if val else DEFAULT

    # Use ALL data (not just 1h) so stats show immediately
    # Re-evaluate every transaction at the current threshold using fraud_score
    # This means stats update instantly when you change the threshold
    stats = await db.fetchrow("""
        SELECT
            COUNT(*)                                              AS total,
            COUNT(*) FILTER (WHERE fraud_score >= $1)            AS would_flag,
            COUNT(*) FILTER (WHERE fraud_score >= $1
                             AND fraud_score >= 0
                             AND (
                               -- In kaggle mode ground truth comes from _ground_truth
                               -- In synthetic mode we use the stored is_fraud
                               is_fraud = TRUE
                             ))                                   AS true_positives,
            COUNT(*) FILTER (WHERE fraud_score >= $1
                             AND is_fraud = FALSE)                AS false_positives,
            COUNT(*) FILTER (WHERE fraud_score < $1
                             AND is_fraud = TRUE)                 AS false_negatives,
            COUNT(*) FILTER (WHERE is_fraud = TRUE)               AS actual_fraud,
            ROUND(AVG(fraud_score)::numeric, 4)                   AS avg_score,
            ROUND(MIN(fraud_score)::numeric, 4)                   AS min_score,
            ROUND(MAX(fraud_score)::numeric, 4)                   AS max_score
        FROM transactions
    """, current)

    r       = dict(stats)
    total   = max(r["total"] or 0, 1)
    flagged = r["would_flag"] or 0
    tp      = r["true_positives"] or 0
    fp      = r["false_positives"] or 0
    fn      = r["false_negatives"] or 0
    actual  = r["actual_fraud"] or 0

    precision = tp / flagged     if flagged > 0 else 0.0
    recall    = tp / actual      if actual  > 0 else 0.0
    f1        = (2 * precision * recall / (precision + recall)
                 if (precision + recall) > 0 else 0.0)

    return {
        "threshold":       round(current, 2),
        "total":           r["total"] or 0,
        "would_flag":      flagged,
        "flag_rate":       round(flagged / total * 100, 2),
        "true_positives":  tp,
        "false_positives": fp,
        "false_negatives": fn,
        "actual_fraud":    actual,
        "precision":       round(precision, 4),
        "recall":          round(recall,    4),
        "f1":              round(f1,        4),
        "avg_score":       float(r["avg_score"] or 0),
        "min_score":       float(r["min_score"] or 0),
        "max_score":       float(r["max_score"] or 0),
    }


@router.post("/threshold")
async def set_threshold(body: ThresholdUpdate, request: Request):
    redis = request.app.state.redis
    t     = round(body.threshold, 2)
    await redis.set(THRESHOLD_KEY, str(t))
    await redis.publish("threshold.changed", json.dumps({"threshold": t}))
    log.info("Threshold updated to %.2f via API", t)
    return {"threshold": t, "message": f"Threshold set to {t}. Active within 10 transactions."}


@router.get("/threshold/curve")
async def get_precision_recall_curve(request: Request):
    """Precision-recall curve using ALL stored transactions."""
    db = request.app.state.db

    rows = await db.fetch("""
        SELECT fraud_score, is_fraud
        FROM transactions
        ORDER BY processed_at DESC
        LIMIT 10000
    """)

    if not rows:
        return {"curve": [], "total_transactions": 0, "total_fraud": 0,
                "message": "No transactions yet — run the pipeline first"}

    scores  = [float(r["fraud_score"]) for r in rows]
    labels  = [bool(r["is_fraud"])     for r in rows]
    total_p = sum(labels) or 1   # avoid division by zero

    curve = []
    for t in [round(i * 0.05, 2) for i in range(2, 19)]:  # 0.10 to 0.90
        flagged = [(s, l) for s, l in zip(scores, labels) if s >= t]
        tp = sum(1 for _, l in flagged if l)
        fp = sum(1 for _, l in flagged if not l)
        fn = total_p - tp

        precision = tp / len(flagged)  if flagged   else 0.0
        recall    = tp / total_p
        f1        = (2 * precision * recall / (precision + recall)
                     if (precision + recall) > 0 else 0.0)
        fpr       = fp / max(len(rows) - total_p, 1)

        curve.append({
            "threshold": t,
            "precision": round(precision, 4),
            "recall":    round(recall,    4),
            "f1":        round(f1,        4),
            "fpr":       round(fpr,       4),
            "flagged":   len(flagged),
        })

    return {
        "curve":              curve,
        "total_transactions": len(rows),
        "total_fraud":        sum(labels),
    }