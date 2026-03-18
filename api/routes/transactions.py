"""routes/transactions.py — Transaction query endpoints."""
from fastapi import APIRouter, Request, Query
from typing import Optional

router = APIRouter(tags=["Transactions"])

@router.get("/transactions")
async def list_transactions(
    request: Request,
    limit: int = Query(50, le=500),
    fraud_only: bool = Query(False),
    cc_num: Optional[str] = Query(None),
    risk_level: Optional[str] = Query(None),
):
    """
    Retrieve recent transactions. Mirrors the original Flask
    /api/statement/<cc_num> and dashboard select queries.
    """
    db = request.app.state.db
    where = ["1=1"]
    params = []

    if fraud_only:
        where.append("is_fraud = TRUE")
    if cc_num:
        params.append(cc_num)
        where.append(f"cc_num = ${len(params)}")
    if risk_level:
        params.append(risk_level.upper())
        where.append(f"risk_level = ${len(params)}")

    params.append(limit)
    q = f"""
        SELECT trans_num, cc_num, trans_time, category, merchant, amt,
               is_fraud, fraud_score, risk_level, ai_reasoning,
               age, distance, hour_of_day, tx_velocity_1h
        FROM transactions
        WHERE {' AND '.join(where)}
        ORDER BY trans_time DESC
        LIMIT ${len(params)}
    """
    rows = await db.fetch(q, *params)
    return [dict(r) for r in rows]


@router.get("/transactions/{trans_num}")
async def get_transaction(trans_num: str, request: Request):
    """Get a single transaction by ID."""
    db = request.app.state.db
    row = await db.fetchrow(
        "SELECT * FROM transactions WHERE trans_num = $1", trans_num
    )
    if not row:
        from fastapi import HTTPException
        raise HTTPException(404, "Transaction not found")
    return dict(row)
