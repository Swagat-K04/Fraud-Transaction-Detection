"""
explainer.py — Claude AI integration for explainable fraud reasoning.

For each flagged transaction, calls claude-sonnet-4-20250514 with the SHAP
feature attributions and transaction context to generate a concise, human-
readable explanation. This is the key AI enhancement over the original repo.
"""

import os
import json
import logging
import asyncio
import httpx
from typing import Optional

log = logging.getLogger("explainer")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_URL     = "https://api.anthropic.com/v1/messages"
MODEL             = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = """\
You are a fraud analyst AI at a financial institution. Your job is to explain
fraud detection decisions to bank operations staff clearly and concisely.

Given a transaction and the top contributing ML features (with SHAP values
showing how much each feature increased or decreased fraud probability), write:
1. One sentence: the primary fraud signal (< 20 words)
2. One sentence: the recommended action (< 15 words)

Be specific — mention amounts, categories, distances where relevant.
Never use hedging language. Respond in JSON only.
"""

def _build_prompt(tx: dict, prediction: dict, features: dict) -> str:
    top = prediction.get("top_features", [])
    feature_lines = "\n".join(
        f"  - {f['feature']}: {f['value']:.2f} ({f['direction']} fraud risk, SHAP={f['shap']:+.3f})"
        for f in top
    )
    return f"""Transaction to explain:
- Card: {tx.get('cc_num', 'unknown')[-4:]} (last 4 digits)
- Amount: ${tx.get('amt', 0):.2f}
- Category: {tx.get('category', 'unknown')}
- Merchant: {tx.get('merchant', 'unknown')}
- Hour of day: {features.get('hour_of_day', 0)}:00
- Customer age: {features.get('age', 0)} years
- Distance from home: {features.get('distance', 0):.2f}°
- Velocity (txns last hour): {features.get('tx_velocity_1h', 0)}
- Amount z-score vs category: {features.get('amt_zscore', 0):.2f}

ML verdict: {'FRAUD' if prediction['is_fraud'] else 'LEGITIMATE'} ({prediction['fraud_score']:.1%} probability)
Risk level: {prediction['risk_level']}

Top 3 contributing features (SHAP):
{feature_lines}

Respond ONLY with this JSON (no markdown):
{{
  "signal": "<primary fraud signal sentence>",
  "action": "<recommended action sentence>"
}}"""


async def explain_async(tx: dict, prediction: dict, features: dict) -> Optional[str]:
    """
    Async call to Claude to generate fraud explanation.
    Returns a formatted string or None on failure.
    """
    if not ANTHROPIC_API_KEY:
        return _rule_based_explanation(tx, prediction, features)

    prompt = _build_prompt(tx, prediction, features)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model": MODEL,
        "max_tokens": 200,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(ANTHROPIC_URL, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()
            text = data["content"][0]["text"].strip()
            parsed = json.loads(text)
            return f"{parsed['signal']} → {parsed['action']}"
    except Exception as e:
        log.warning("Claude explain failed (%s) — using rule-based fallback", e)
        return _rule_based_explanation(tx, prediction, features)


def explain_sync(tx: dict, prediction: dict, features: dict) -> Optional[str]:
    """Synchronous wrapper for use in non-async contexts."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # In a streaming context, run in thread pool
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, explain_async(tx, prediction, features))
                return future.result(timeout=10)
        else:
            return loop.run_until_complete(explain_async(tx, prediction, features))
    except Exception as e:
        log.warning("explain_sync error: %s", e)
        return _rule_based_explanation(tx, prediction, features)


def _rule_based_explanation(tx: dict, prediction: dict, features: dict) -> str:
    """Fast fallback explanation when Claude is unavailable."""
    signals = []
    if features.get("amt_zscore", 0) > 2.5:
        signals.append(f"${tx.get('amt', 0):.0f} is unusually high for {tx.get('category', 'this category')}")
    if features.get("distance", 0) > 3.0:
        signals.append("merchant is far from customer's home address")
    if features.get("hour_of_day", 12) < 5:
        signals.append("transaction at unusual hour (late night)")
    if features.get("tx_velocity_1h", 0) > 5:
        signals.append(f"high velocity: {features['tx_velocity_1h']} transactions in 1 hour")
    if tx.get("category") in ("misc_net", "misc_pos"):
        signals.append("unclassified merchant category is high risk")

    if not signals:
        signals = ["ML model detected anomalous transaction pattern"]

    signal = signals[0].capitalize()
    action = "Block and notify cardholder" if prediction["risk_level"] == "CRITICAL" else "Flag for review"
    return f"{signal} → {action}"
