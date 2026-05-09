# ADR-0004: API Serving Layer - FastAPI with asyncpg, Raw SQL, No Cache

## Context and Problem Statement

The brief requires four reporting endpoints exposed over HTTP. The endpoints are aggregations against the dimensional model defined in ADR-0003 and must return current results from a continuously-updated analytical store.

Four sub-decisions are resolved here:

- **A. Framework** - what serves HTTP, validates requests, generates docs.
- **B. DB access** - driver, connection pool, ORM vs raw SQL.
- **C. Contracts** - endpoint paths, query params, response shapes, error envelope, pagination semantics.
- **D. Caching** - whether to interpose a cache and at what layer.

The brief explicitly says the suggested endpoint paths "are suggestions and you adapt it, but ensure that all the metrics will be available through an API." This ADR keeps paths close to the brief's wording for read-the-prompt signal, while normalizing them for consistency.

## Decision Drivers


| ID  | Driver                                                                                                                                                   |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D1  | **Performance** - endpoints must return in p99 < 200 ms (consistent with ADR-0003 budget).                                                               |
| D2  | **Contract clarity** - request/response types must be machine-readable (OpenAPI) and validated on both sides.                                            |
| D3  | **Freshness** - must-have #3 from the brief: "data must be available and ready to consume as soon as possible." Caching layers must not compromise this. |
| D4  | **Consistency with the rest of the stack** - Python end-to-end (consumer, API, tests).                                                                   |
| D5  | **Time-to-deliver** - framework and tooling must be familiar enough to ship in scope.                                                                    |                                    |
| D6  | **Reviewer reproducibility** - runs in compose, single API container, deterministic startup.                                                             |


## Sub-decision A - Framework

### Considered options

- **A.1 - FastAPI.** Async-first, Pydantic-validated, auto-generated OpenAPI/Swagger UI, dependency injection.
- **A.2 - Flask.** Synchronous by default, minimal core, ecosystem fragmented for async + types + OpenAPI.
- **A.3 - Litestar.** FastAPI-style ergonomics, slightly faster, smaller ecosystem and less community familiarity.
- **A.4 - Starlette directly.** Lower-level than FastAPI; we'd be rebuilding what FastAPI provides.

### Comparison


| #   | Async (D1)       | Auto-OpenAPI (D2) | Type validation (D2) | Familiarity (D5)    | Outcome                                           |
| --- | ---------------- | ----------------- | -------------------- | ------------------- | ------------------------------------------------- |
| A.1 | ✓                | ✓ built-in        | ✓ Pydantic           | ✓ industry standard | **Chosen**                                        |
| A.2 | ~ via extensions | ~ via extensions  | ~ via extensions     | ✓                   | Effort to reach parity outweighs simplicity       |
| A.3 | ✓                | ✓                 | ✓                    | ~ less common       | Defensible alt; loses on familiarity for reviewer |
| A.4 | ✓                | ✗ DIY             | ✗ DIY                | ~                   | Re-implementing FastAPI                           |


**Chosen: FastAPI.**

**Rationale.** FastAPI's auto-generated OpenAPI is itself a deliverable artifact for the take-home - the reviewer gets a `/docs` page that documents every endpoint, parameter, and response shape derived directly from the code. That's documentation guaranteed to match the implementation, which is more valuable than any hand-written API doc. Pydantic-validated request/response models give us contract enforcement on both sides for free.

## Sub-decision B - Database access

### Considered options

- **B.1 - asyncpg + raw parameterized SQL** behind a thin query module.
- **B.2 - psycopg3 (async mode) + raw parameterized SQL.**
- **B.3 - SQLAlchemy 2.0 Core (async)** - query-builder style, no ORM.
- **B.4 - SQLAlchemy 2.0 ORM (async)** - full ORM with mapped classes.

### Comparison


| #   | Latency overhead (D1) | Query transparency (D6)    | Learning curve (D5) | Fits aggregation workload       | Outcome                                 |
| --- | --------------------- | -------------------------- | ------------------- | ------------------------------- | --------------------------------------- |
| B.1 | Lowest                | Total - query is what runs | Low                 | ✓                               | **Chosen**                              |
| B.2 | Low                   | Total                      | Low                 | ✓                               | Defensible alt; asyncpg slightly faster |
| B.3 | Moderate              | Partial - generated SQL    | Medium              | ~ overkill                      | Adds layer for no gain                  |
| B.4 | Higher                | Hidden behind ORM          | Medium              | ✗ analytical queries fight ORMs | Wrong tool                              |


**Chosen: asyncpg + raw parameterized SQL.**

**Rationale.** The four endpoints are pure aggregations on a dimensional model. ORMs exist to manage entity lifecycle (create, update, delete, relationship navigation) - none of which the API does. Using an ORM here adds a layer that hides what Postgres is actually executing, which is exactly what we don't want when the goal is sub-200-ms p99 on aggregations.

asyncpg over psycopg3 is a tiebreaker on raw throughput; either is defensible. Documented choice.

### Connection pool

`asyncpg.create_pool(min_size=2, max_size=10)`. The API container is single-replica in compose; sizing handles bursty concurrency without over-allocating Postgres connections. Pool is created on app startup and closed on shutdown via FastAPI's lifespan hook.

## Sub-decision C - Endpoint contracts

### Path conventions

The brief's paths are kept verbatim where they're already consistent and normalized where they aren't. Specifically the trailing-slash on `/analytics/orders/customers/` is removed to match the others, and the `status` query param is treated as a filter on the result set (not the grouping dimension).

### Response envelope

Every endpoint returns a JSON object with two top-level keys:

```json
{
  "data": [ ... ],
  "meta": { "count": N, "generated_at": "2026-05-08T14:30:00Z" }
}
```

`meta.generated_at` is the API's wall-clock time at response generation, not the analytical store's freshness watermark - the latter is exposed separately via the `/health` endpoint (see ADR-0005). Wrapping in `data`

- `meta` gives room for future fields (pagination cursor, freshness watermark, request ID) without breaking the contract.

### Error envelope

Standardized via FastAPI exception handlers:

```json
{
  "error": {
    "type": "validation_error" | "not_found" | "internal_error",
    "message": "Human-readable description",
    "request_id": "uuid"
  }
}
```

`request_id` is generated by middleware on every request and added to both the response body and the response header (`X-Request-ID`) so the caller can correlate with logs (ADR-0005).

### HTTP semantics

- `200 OK` - success with body
- `400 Bad Request` - Pydantic validation failure on query params
- `404 Not Found` - only for path-parameterized endpoints (none of the four currently)
- `500 Internal Server Error` - unhandled exception; logged with full traceback and `request_id`
- `503 Service Unavailable` - DB pool exhausted or DB unreachable

No `5xx` body should leak stack traces; error envelope only.

### Pagination

Two of the four endpoints have implicit `limit` semantics already (`top`, `customers` with `limit`). The other two return aggregations whose cardinality is bounded by:

- Endpoint #1 - bounded by distinct `(delivery_date, status)` pairs × filtered statuses → small.
- Endpoint #3 - bounded by distinct products with open items → can be large.

Endpoint #3 gets an optional `limit` (default 100, max 1000) and an `offset` (default 0). Cursor pagination is overkill for this scope and documented as a future improvement.

### Versioning

API is mounted under `/v1`. No versioning machinery beyond the path prefix; documented as the upgrade hook if breaking changes are ever needed.

## Sub-decision D - Caching

### Considered options

- **D.1 - No cache.** Every request hits Postgres. Indexes (ADR-0003) carry the latency budget.
- **D.2 - In-process LRU** with sub-second TTL.
- **D.3 - Redis** sidecar with short TTL.
- **D.4 - HTTP `Cache-Control` headers only**, leaving caching to intermediaries.

### Comparison


| #   | Freshness (D3) | Latency (D1)             | Compose footprint (D7) | Operational surface            | Outcome                             |
| --- | -------------- | ------------------------ | ---------------------- | ------------------------------ | ----------------------------------- |
| D.1 | ✓ real-time    | Bounded by DB index perf | +0                     | Lowest                         | **Chosen for v1**                   |
| D.2 | ~ stale by TTL | Sub-ms after first hit   | +0                     | Low; cache invalidation hazard | Reserved                            |
| D.3 | ~ stale by TTL | Sub-ms                   | +1 service             | Medium                         | Reserved                            |
| D.4 | Caller-decided | N/A                      | +0                     | None                           | Compatible with D.1, low value here |


**Chosen: D.1 (no cache).**

**Rationale.** The brief's freshness requirement is the dominant driver: "data must be available and ready to consume as soon as possible." Any cache TTL is a freshness regression. ADR-0003's indexed base tables hit the latency budget without help, so caching solves a problem we don't have while creating a new one (invalidation correctness, stale-data explanations to consumers).

Caching is the right answer at higher read volumes or when the analytical store is shared with other workloads - both noted in the README's "scaling considerations" section.

## Endpoint specifications

All endpoints are mounted under `/v1`. All return the standard envelope. All filter parameters are validated as Pydantic models.

### `GET /v1/analytics/orders`

**Purpose.** Number of orders grouped by `delivery_date` and `status`.

**Query parameters**

- `status: str | None` - filter to a single status. If omitted, returns all statuses. Defaults to `open` per the brief's example.

**Response shape**

```json
{
  "data": [
    { "delivery_date": "2026-05-10", "status": "open", "order_count": 42 },
    { "delivery_date": "2026-05-11", "status": "open", "order_count": 17 }
  ],
  "meta": { "count": 2, "generated_at": "..." }
}
```

**Backing query** (against `marts.fact_orders`)

```sql
SELECT delivery_date_key, current_status, COUNT(*) AS order_count
FROM marts.fact_orders
WHERE current_status = $1
GROUP BY delivery_date_key, current_status
ORDER BY delivery_date_key;
```

Index used: `(current_status, delivery_date_key)`.

### `GET /v1/analytics/orders/top`

**Purpose.** Top N delivery dates by open-order count.

**Query parameters**

- `limit: int = 3` - bounded `1 ≤ limit ≤ 100`.

**Response shape**

```json
{
  "data": [
    { "delivery_date": "2026-05-10", "order_count": 42 },
    { "delivery_date": "2026-05-11", "order_count": 17 },
    { "delivery_date": "2026-05-12", "order_count": 9 }
  ],
  "meta": { "count": 3, "generated_at": "..." }
}
```

**Backing query**

```sql
SELECT delivery_date_key, COUNT(*) AS order_count
FROM marts.fact_orders
WHERE current_status = 'open'
GROUP BY delivery_date_key
ORDER BY order_count DESC
LIMIT $1;
```

### `GET /v1/analytics/orders/products`

**Purpose.** Number of open pending items grouped by `product_id`.

**Query parameters**

- `limit: int = 100` - bounded `1 ≤ limit ≤ 1000`.
- `offset: int = 0`.

**Response shape**

```json
{
  "data": [
    { "product_id": 4711, "product_name": "Widget A", "open_item_count": 230 },
    { "product_id": 4712, "product_name": "Widget B", "open_item_count": 88 }
  ],
  "meta": { "count": 2, "generated_at": "..." }
}
```

**Backing query**

```sql
SELECT p.product_id, p.product_name, COUNT(*) AS open_item_count
FROM marts.fact_order_items i
JOIN marts.dim_product p
  ON p.product_sk = i.product_sk
WHERE i.current_order_status = 'open'
GROUP BY p.product_id, p.product_name
ORDER BY open_item_count DESC
LIMIT $1 OFFSET $2;
```

Index used: `fact_order_items (current_order_status, product_sk)`.

Note: joins to the **current** SCD2 row of `dim_product`. Open question documented as out-of-scope: should "product name" reflect the name at order time (point-in-time SCD2 lookup via `valid_from`/`valid_to` and the order's `created_at`) or the current name? For "what's pending right now" the current name is the operationally useful answer, so we join on the current version. Flagged in README.

### `GET /v1/analytics/orders/customers`

**Purpose.** Top N customers by pending-order count.

**Query parameters**

- `status: str = "open"` - filter status; explicit per the brief's path.
- `limit: int = 3` - bounded `1 ≤ limit ≤ 100`.

**Response shape**

```json
{
  "data": [
    { "customer_id": 9001, "customer_name": "Acme Corp",  "order_count": 12 },
    { "customer_id": 9002, "customer_name": "Wayne Inc.", "order_count":  9 }
  ],
  "meta": { "count": 2, "generated_at": "..." }
}
```

**Backing query**

```sql
SELECT c.customer_id, c.customer_name, COUNT(*) AS order_count
FROM marts.fact_orders f
JOIN marts.dim_customer c
  ON c.customer_sk = f.customer_sk
WHERE f.current_status = $1
GROUP BY c.customer_id, c.customer_name
ORDER BY order_count DESC
LIMIT $2;
```

Same SCD2 caveat as endpoint #3.

### Operational endpoints

- `GET /v1/health` - liveness. Returns 200 if process is up.
- `GET /v1/ready` - readiness. Returns 200 only if the DB pool can serve a `SELECT 1`; details deferred to ADR-0005.
- `GET /docs` - Swagger UI (FastAPI default).
- `GET /openapi.json` - OpenAPI schema.

## Application structure

```
app/
├── main.py            # FastAPI app, lifespan, middleware wiring
├── config.py          # Pydantic Settings (env-driven)
├── deps.py            # DB pool dependency
├── routers/
│   ├── orders.py      # /analytics/orders, /top
│   ├── products.py    # /analytics/orders/products
│   ├── customers.py   # /analytics/orders/customers
│   └── ops.py         # /health, /ready
├── schemas/           # Pydantic request/response models
├── queries/           # Raw SQL files or query functions
└── middleware/
    ├── request_id.py
    └── timing.py      # for ADR-0005's metrics
```

Queries live in their own module (one file per endpoint or one file total). Putting SQL out-of-line keeps Python files clean and lets the queries be reviewed and explained directly.

## Consequences

### Positive

- Auto-generated OpenAPI documentation matches implementation by construction (D2, D6).
- Pydantic validates query params on the way in and response shapes on the way out - contract violations fail loudly in development (D2).
- asyncpg raw SQL keeps the API → DB path transparent and fast (D1, D6).
- No caching layer means no freshness regression and no invalidation bugs (D3).
- Single API container in compose; identical container shape to a real deployment (D7).

### Negative

- **No cache means DB index sizing is load-bearing.** If aggregation performance regresses, the API has no fallback. Mitigation: load-test in validation step #3 below; document MV/cache upgrade paths.
- **Raw SQL trades type safety inside queries** - a typo in a column name fails at runtime, not at static-check time. Mitigation: integration tests that exercise every endpoint against a seeded DB.
- **SCD2 join semantics are "current version only"** - historical point-in-time joins (e.g., "name at the time of the order") are not served by these endpoints. Documented as a follow-up.
- **No auth or rate limiting.** Acceptable for take-home scope; called out in README.

### Neutral

- API is single-replica. Stateless design means horizontal scaling works as soon as needed.
- Pagination on endpoint #3 uses offset, not cursor. Acceptable for product cardinality in scope; cursor pagination noted as the next step.

## Validation

The decision is validated when:

1. **Contract.** Every endpoint round-trips Pydantic validation: invalid `limit` returns 400 with the standard error envelope; valid request returns 200 with the standard `data + meta` envelope.
2. **OpenAPI.** `/docs` renders all four endpoints with parameters, types, and example responses - without any hand-written API documentation.
3. **Performance.** Each endpoint responds in p99 < 200 ms under a concurrent load test (e.g. `wrk -c 50 -d 30s`) on the seeded dataset. Documented in README with the actual measured numbers.
4. **Numerical correctness.** Each endpoint's result matches a reference query computed directly against `staging` (the source-state mirror). Same check as ADR-0003 validation #1, but exercised through the HTTP layer.
5. **Error path.** Killing the DB mid-flight yields 503 with a clean error envelope (no stack trace leak) and a logged traceback with `request_id`.

