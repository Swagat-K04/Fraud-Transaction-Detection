"""routes/customers.py — Customer info endpoint (mirrors original Flask /api/customer/<cc_num>)."""
from fastapi import APIRouter, Request, HTTPException

router = APIRouter(tags=["Customers"])

@router.get("/customer/{cc_num}")
async def get_customer(cc_num: str, request: Request):
    """Return customer info + recent transactions. Mirrors original Flask API."""
    db = request.app.state.db
    customer = await db.fetchrow(
        "SELECT * FROM customers WHERE cc_num = $1", cc_num
    )
    if not customer:
        raise HTTPException(404, "Customer not found")

    transactions = await db.fetch("""
        SELECT trans_num, trans_time, category, merchant, amt,
               is_fraud, fraud_score, risk_level
        FROM transactions WHERE cc_num = $1
        ORDER BY trans_time DESC LIMIT 20
    """, cc_num)

    return {
        "customer": dict(customer),
        "transactions": [dict(r) for r in transactions],
    }


@router.get("/statement/{cc_num}")
async def get_statement(cc_num: str, request: Request):
    """Transaction statement for a card (mirrors original /api/statement/<cc_num>)."""
    db = request.app.state.db
    rows = await db.fetch("""
        SELECT trans_num, trans_time, merchant, category, amt, is_fraud, risk_level
        FROM transactions WHERE cc_num = $1 AND is_fraud = FALSE
        ORDER BY trans_time DESC
    """, cc_num)
    return [dict(r) for r in rows]
