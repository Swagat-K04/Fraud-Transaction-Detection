"""db.py — Async PostgreSQL connection pool wrapper."""
import asyncpg

class Database:
    def __init__(self, url: str):
        self._url = url.replace("postgresql://", "postgres://")
        self._pool: asyncpg.Pool | None = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(
            self._url, min_size=2, max_size=20, command_timeout=15
        )

    async def disconnect(self):
        if self._pool:
            await self._pool.close()

    async def fetch(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)
