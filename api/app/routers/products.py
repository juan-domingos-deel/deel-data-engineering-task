from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Query, Request

from ..deps import get_pool
from ..queries.products import OPEN_ITEMS_BY_PRODUCT
from . import envelope

router = APIRouter(prefix="/analytics/orders/products", tags=["products"])


@router.get(
    "",
    summary="Open pending items by product_id",
    description=(
        "Counts items whose parent order is in any non-COMPLETED status, "
        "grouped by product_id."
    ),
)
async def open_items_by_product(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    pool: asyncpg.Pool = Depends(get_pool),
):
    async with pool.acquire() as conn:
        rows = await conn.fetch(OPEN_ITEMS_BY_PRODUCT, limit, offset)
    return envelope([dict(r) for r in rows])
