"""
threshold.py — Live threshold management via Redis.

The fraud decision threshold is stored in Redis so it can be changed
at runtime without restarting the consumer. The consumer reads it
before every prediction batch. The FastAPI backend exposes endpoints
to read and update it. The dashboard slider writes to those endpoints.

Default: 0.5
Range:   0.1 - 0.9 (step 0.01)
"""

import logging
import os

log = logging.getLogger("threshold")

THRESHOLD_KEY     = "fraud:threshold"
DEFAULT_THRESHOLD = 0.5
MIN_THRESHOLD     = 0.10
MAX_THRESHOLD     = 0.90

# In-memory cache so we don't hit Redis on every single message
# Refreshed every N transactions (see consumer.py)
_cached_threshold = DEFAULT_THRESHOLD
_cache_counter    = 0
CACHE_REFRESH_N   = 10  # re-read Redis every 10 transactions


async def get_threshold(redis_client) -> float:
    """Read current threshold from Redis, falling back to default."""
    global _cached_threshold, _cache_counter

    _cache_counter += 1
    if _cache_counter % CACHE_REFRESH_N != 0:
        return _cached_threshold

    try:
        val = await redis_client.get(THRESHOLD_KEY)
        if val is not None:
            t = float(val)
            if MIN_THRESHOLD <= t <= MAX_THRESHOLD:
                if t != _cached_threshold:
                    log.info("Threshold updated: %.2f → %.2f", _cached_threshold, t)
                _cached_threshold = t
    except Exception as e:
        log.warning("Could not read threshold from Redis: %s", e)

    return _cached_threshold


async def set_threshold(redis_client, value: float) -> float:
    """Set threshold in Redis. Returns the clamped value actually set."""
    value = round(max(MIN_THRESHOLD, min(MAX_THRESHOLD, value)), 2)
    await redis_client.set(THRESHOLD_KEY, str(value))
    # Publish change event so dashboard gets instant update
    import json
    await redis_client.publish("threshold.changed", json.dumps({"threshold": value}))
    log.info("Threshold set to %.2f", value)
    return value


async def init_threshold(redis_client):
    """Set default threshold if not already set."""
    existing = await redis_client.get(THRESHOLD_KEY)
    if existing is None:
        await redis_client.set(THRESHOLD_KEY, str(DEFAULT_THRESHOLD))
        log.info("Threshold initialised to %.2f", DEFAULT_THRESHOLD)