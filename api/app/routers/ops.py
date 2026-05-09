import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request

from ..deps import get_pool

router = APIRouter(tags=["ops"])


@router.get("/health", summary="Liveness probe")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/ready", summary="Readiness probe - DB reachable")
async def ready(request: Request, pool: asyncpg.Pool = Depends(get_pool)) -> dict:
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"db unreachable: {exc!s}")
    return {"status": "ready"}
