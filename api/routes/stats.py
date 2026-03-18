"""routes/stats.py — Aggregated stats for the dashboard."""
from fastapi import APIRouter, Request

router = APIRouter(tags=["Stats"])

@router.get("/stats/summary")
async def summary(request: Request):
    db = request.app.state.db
    row = await db.fetchrow("""
        SELECT
            COUNT(*)                                    AS total,
            COUNT(*) FILTER (WHERE is_fraud)            AS fraud_count,
            COALESCE(SUM(amt), 0)                       AS total_volume,
            COALESCE(SUM(amt) FILTER (WHERE is_fraud),0) AS fraud_volume,
            ROUND(AVG(fraud_score)::numeric, 4)         AS avg_fraud_score,
            COUNT(*) FILTER (WHERE risk_level='CRITICAL') AS critical_count
        FROM transactions
        WHERE trans_time > NOW() - INTERVAL '24 hours'
    """)
    r = dict(row)
    total = r["total"] or 1
    r["fraud_rate"] = round(float(r["fraud_count"]) / total * 100, 2)
    return r


@router.get("/stats/hourly")
async def hourly(request: Request):
    """Hourly fraud rate for the last 24 hours — feeds the timeline chart."""
    db = request.app.state.db
    rows = await db.fetch("""
        SELECT
            time_bucket('1 hour', trans_time) AS hour,
            COUNT(*)                           AS total,
            COUNT(*) FILTER (WHERE is_fraud)   AS fraud_count,
            COALESCE(SUM(amt), 0)              AS volume
        FROM transactions
        WHERE trans_time > NOW() - INTERVAL '24 hours'
        GROUP BY hour
        ORDER BY hour ASC
    """)
    return [dict(r) for r in rows]


@router.get("/stats/by-category")
async def by_category(request: Request):
    db = request.app.state.db
    rows = await db.fetch("""
        SELECT
            category,
            COUNT(*)                          AS total,
            COUNT(*) FILTER (WHERE is_fraud)  AS fraud_count,
            ROUND(AVG(fraud_score)::numeric,3) AS avg_score
        FROM transactions
        WHERE trans_time > NOW() - INTERVAL '24 hours'
        GROUP BY category
        ORDER BY fraud_count DESC
    """)
    return [dict(r) for r in rows]


@router.get("/stats/risk-distribution")
async def risk_distribution(request: Request):
    db = request.app.state.db
    rows = await db.fetch("""
        SELECT risk_level, COUNT(*) AS count
        FROM transactions
        WHERE trans_time > NOW() - INTERVAL '24 hours'
        GROUP BY risk_level
    """)
    return [dict(r) for r in rows]
