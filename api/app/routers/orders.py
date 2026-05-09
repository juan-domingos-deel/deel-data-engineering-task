from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Query, Request

from ..deps import get_pool
from ..queries.orders import (
    ORDERS_BY_DATE_ALL,
    ORDERS_BY_DATE_STATUS_LITERAL,
    ORDERS_BY_DATE_STATUS_OPEN,
    TOP_DELIVERY_DATES_OPEN,
)
from . import envelope

router = APIRouter(prefix="/analytics/orders", tags=["orders"])


@router.get(
    "",
    summary="Number of orders by delivery_date and status",
    description=(
        "Defaults to status='open' (everything except COMPLETED). "
        "Pass status= to filter to any literal status, or 'all' for no filter."
    ),
)
async def orders_by_delivery_status(
    request: Request,
    status: Annotated[str, Query(min_length=1, max_length=50)] = "open",
    pool: asyncpg.Pool = Depends(get_pool),
):
    async with pool.acquire() as conn:
        if status == "open":
            rows = await conn.fetch(ORDERS_BY_DATE_STATUS_OPEN)
        elif status == "all":
            rows = await conn.fetch(ORDERS_BY_DATE_ALL)
        else:
            rows = await conn.fetch(ORDERS_BY_DATE_STATUS_LITERAL, status)
    return envelope([dict(r) for r in rows])


@router.get(
    "/top",
    summary="Top N delivery dates by open-order count",
)
async def top_delivery_dates(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 3,
    pool: asyncpg.Pool = Depends(get_pool),
):
    async with pool.acquire() as conn:
        rows = await conn.fetch(TOP_DELIVERY_DATES_OPEN, limit)
    return envelope([dict(r) for r in rows])
