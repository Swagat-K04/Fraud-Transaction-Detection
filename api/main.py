"""
main.py — FastAPI backend for the Fraud Detection Dashboard.

Replaces the original dual-framework setup (Spring Boot for dashboard,
Flask for REST APIs) with a single modern async FastAPI app that provides:
  • REST endpoints: /api/transactions, /api/customers, /api/stats
  • WebSocket endpoint: /ws — real-time push from Redis Pub/Sub
  • Auto-generated OpenAPI docs at /docs
"""

import os
import json
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware

from routes.transactions import router as tx_router
from routes.customers import router as cust_router
from routes.stats import router as stats_router
from routes.threshold import router as threshold_router
from routes.inject import router as inject_router
from db import Database

log = logging.getLogger("api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://fraud:fraud@localhost:5432/frauddb")

# ─── WebSocket connection manager ─────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self._connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.append(ws)
        log.info("WS connected — total: %d", len(self._connections))

    def disconnect(self, ws: WebSocket):
        self._connections.remove(ws)
        log.info("WS disconnected — total: %d", len(self._connections))

    async def broadcast(self, message: str):
        dead = []
        for ws in self._connections:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# ─── Redis subscriber (pushes to all WebSocket clients) ───────────────────────

async def redis_subscriber(redis_client: aioredis.Redis):
    """Subscribe to Redis channels and broadcast to all connected WebSocket clients."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("tx.all", "fraud.alerts", "threshold.changed")
    log.info("Redis subscriber started — listening on tx.all, fraud.alerts")

    async for message in pubsub.listen():
        if message["type"] == "message":
            data = message["data"]
            if isinstance(data, bytes):
                data = data.decode()
            await manager.broadcast(data)


# ─── App lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    # Startup
    app.state.db = Database(POSTGRES_URL)
    await app.state.db.connect()

    app.state.redis = await aioredis.from_url(REDIS_URL, decode_responses=True)

    # Start Redis subscriber as background task
    app.state.subscriber_task = asyncio.create_task(
        redis_subscriber(app.state.redis)
    )

    log.info("API started — DB and Redis connected")
    yield

    # Shutdown
    app.state.subscriber_task.cancel()
    await app.state.db.disconnect()
    await app.state.redis.aclose()
    log.info("API shut down cleanly")


# ─── FastAPI app ──────────────────────────────────────────────────────────────

app = FastAPI(
    title="Fraud Detection API",
    description="Real-time credit card fraud detection — REST + WebSocket",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(tx_router,   prefix="/api")
app.include_router(cust_router, prefix="/api")
app.include_router(stats_router, prefix="/api")
app.include_router(threshold_router, prefix="/api")
app.include_router(inject_router, prefix="/api")


@app.get("/health")
async def health():
    return {"status": "ok"}


# ─── WebSocket endpoint ───────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Real-time transaction stream.
    Clients receive JSON messages for every transaction (fraud and non-fraud).
    Filter client-side by `is_fraud` field.

    Original dashboard polled DB every 5 seconds for new records — this
    approach pushes events sub-second after the Spark batch completes.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive; all sends happen via broadcast()
            await asyncio.sleep(30)
            await websocket.send_text('{"type":"ping"}')
    except WebSocketDisconnect:
        manager.disconnect(websocket)