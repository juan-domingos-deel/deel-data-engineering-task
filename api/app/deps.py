from typing import AsyncIterator

import asyncpg
from fastapi import Request


async def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


async def get_conn(pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    async with pool.acquire() as conn:
        yield conn
