from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Query, Request

from ..deps import get_pool
from ..queries.customers import TOP_CUSTOMERS_LITERAL, TOP_CUSTOMERS_OPEN
from . import envelope

router = APIRouter(prefix="/analytics/orders/customers", tags=["customers"])


@router.get(
    "",
    summary="Top N customers by pending-order count",
    description="Default status='open' (≡ NOT COMPLETED). Override with any literal status.",
)
async def top_customers(
    request: Request,
    status: Annotated[str, Query(min_length=1, max_length=50)] = "open",
    limit: Annotated[int, Query(ge=1, le=100)] = 3,
    pool: asyncpg.Pool = Depends(get_pool),
):
    async with pool.acquire() as conn:
        if status == "open":
            rows = await conn.fetch(TOP_CUSTOMERS_OPEN, limit)
        else:
            rows = await conn.fetch(TOP_CUSTOMERS_LITERAL, status, limit)
    return envelope([dict(r) for r in rows])
