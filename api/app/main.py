"""FastAPI app entrypoint. ADR-0004."""

from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import settings
from .logging import configure_logging, get_logger
from .middleware.request_id import RequestIdMiddleware
from .routers import customers, ops, orders, products

configure_logging(settings.log_level)
log = get_logger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("creating asyncpg pool", dsn_host=settings.pg_host)
    app.state.pool = await asyncpg.create_pool(
        dsn=settings.pg_dsn,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
    )
    try:
        yield
    finally:
        log.info("closing asyncpg pool")
        await app.state.pool.close()


app = FastAPI(
    title="Deel Streaming Analytics API",
    version="0.1.0",
    description=(
        "Reporting endpoints for the ACME Delivery analytics platform.\n\n"
        "Backed by a Kimball star schema (`marts.*`) populated from a "
        "Debezium → Kafka → consumer pipeline."
    ),
    lifespan=lifespan,
    openapi_url="/v1/openapi.json",
    docs_url="/docs",
    redoc_url=None,
)

app.add_middleware(RequestIdMiddleware)
Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


def _error_response(request: Request, *, status_code: int, type_: str, message: str) -> JSONResponse:
    rid = getattr(request.state, "request_id", "")
    return JSONResponse(
        status_code=status_code,
        content={"error": {"type": type_, "message": message, "request_id": rid}},
    )


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    return _error_response(
        request, status_code=400, type_="validation_error", message=str(exc.errors()[0]) if exc.errors() else "invalid request"
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    type_ = "not_found" if exc.status_code == 404 else "service_unavailable" if exc.status_code == 503 else "http_error"
    return _error_response(request, status_code=exc.status_code, type_=type_, message=str(exc.detail))


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    rid = getattr(request.state, "request_id", "")
    log.exception("unhandled error", request_id=rid, error=str(exc))
    return _error_response(request, status_code=500, type_="internal_error", message="internal error")


# v1 routers
app.include_router(orders.router, prefix="/v1")
app.include_router(products.router, prefix="/v1")
app.include_router(customers.router, prefix="/v1")
app.include_router(ops.router, prefix="/v1")
