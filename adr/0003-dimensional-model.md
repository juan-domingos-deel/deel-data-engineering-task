# ADR-0003: Dimensional Model - Star Schema, SCD2 Dimensions, Hybrid Fact Strategy

## Context and Problem Statement

ADR-0002 commits to a Kimball-style `marts` schema fed from `staging`. This ADR specifies the actual model: which dimensions, which facts, what grain, how to track history, and how the four required reporting endpoints will be served performantly.

Three sub-decisions are entangled and resolved together:

- **A. Dimensions** - what dimensions, with which slowly-changing strategy.
- **B. Facts** - what facts, at what grain, and how state transitions are captured given that the brief requires *both* current state *and* historical querying.
- **C. Aggregation strategy** - whether the API endpoints query base tables with indexes or precomputed materialized views.

The brief's reporting requirements are the design forcing function:

1. Number of open orders by `DELIVERY_DATE` and `STATUS`
2. Top 3 delivery dates with most open orders
3. Open pending items by `PRODUCT_ID` (joining order status with order items)
4. Top 3 customers with most pending orders

All four are aggregations filtered by current order status, three of them join to a dimension (customer or product), and one joins fact-to-fact (order_items to orders).

## Decision Drivers


| ID  | Driver                                                                                                                                                                   |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| D1  | **Current-state queryability** - all four endpoints filter on current order status.                                                                                      |
| D2  | **Historical fidelity** - must support "as-of" analysis and full state-transition history (must-have from brief).                                                        |
| D3  | **Conformed dimensions** - customers and products are reused across facts and must be modeled consistently.                                                              |
| D4  | **Query performance** - sub-second p99 on the four endpoints at the implied data volume (millions of orders, tens of millions of items).                                 |
| D5  | **Write performance** - consumer applies CDC batches transactionally; modeling choices must not bottleneck the apply loop.                                               ||
| D6  | **Time-to-deliver** - model complexity must serve the brief, not gold-plate it.                                                                                          |


## Sub-decision A - Dimensions and SCD strategy

### Considered options

- **A.1** - `dim_customer`, `dim_product`, `dim_date`, `dim_status`. All business dimensions with surrogate keys; SCD2 for customer/product; `dim_status` as a small reference dimension.
- **A.2** - Same dimensions but **status as a degenerate dimension** (kept as a `VARCHAR` on the fact) rather than its own table.
- **A.3** - SCD1 (overwrite) on customer and product instead of SCD2. Simpler, smaller, but loses history.
- **A.4** - SCD3 (one prior-value column) on customer and product. Tracks one previous value but not full history.

### Comparison


| #   | Conformed dims | History on customer/product (D2) | Status modeling | Effort (D6) | Outcome                           |
| --- | -------------- | -------------------------------- | --------------- | ----------- | --------------------------------- |
| A.1 | ✓              | SCD2                             | Reference dim   | High        | Over-engineered for 5–10 statuses |
| A.2 | ✓              | SCD2                             | **Degenerate**  | Medium      | **Chosen**                        |
| A.3 | ✓              | ✗ overwritten                    | Degenerate      | Low         | Fails D2                          |
| A.4 | ✓              | ~ one prior version only         | Degenerate      | Low         | Insufficient for D2               |


**Chosen: A.2.** SCD2 on `dim_customer` and `dim_product`, generated `dim_date`, status as a degenerate dimension on the fact tables.

**Rationale.** Order status is low-cardinality (handful of values), purely attributional, and never joined for navigation - exactly the textbook case for a degenerate dimension. Promoting it to a full dim adds a join to every endpoint query without enabling any analysis we couldn't do with a `WHERE status = 'open'` clause. SCD2 on customer and product is required because "address at the time of the order" and "unit price at the time of the order" are legitimate analytical questions that the must-have history requirement implies.

## Sub-decision B - Fact tables and state-transition strategy

The tension is that the brief requires *current state* (all four endpoints) and *full history* (must-have #1) simultaneously. Pure Kimball offers three patterns; none of them solves both cleanly on its own.

### Considered options

- **B.1 - Transaction fact only.** One row per state-change event from CDC. Append-only.
- **B.2 - Periodic snapshot only.** Materialize state at fixed intervals (e.g., end of day).
- **B.3 - Accumulating snapshot only.** One row per order with milestone date columns (order_date, in_progress_date, delivered_date, …) updated in place.
- **B.4 - Hybrid: current-state fact + transaction fact for status history.** `fact_orders` holds one row per order, UPSERTed on every change. `fact_order_status_events` is append-only, INSERTed on every status change with the new status and timestamp.

### Comparison


| #   | Current-state queries (D1)                   | Full history (D2)                     | Write cost (D5)          | Reasoning load on API queries (D4)                    | Outcome                                      |
| --- | -------------------------------------------- | ------------------------------------- | ------------------------ | ----------------------------------------------------- | -------------------------------------------- |
| B.1 | ✗ requires `argMax`-style query at read time | ✓ perfect                             | Low (append-only)        | High - every endpoint needs latest-per-order subquery | Slow APIs                                    |
| B.2 | ~ stale up to one period                     | ~ one row per period                  | Medium                   | Low                                                   | Stale data, fails D1's "current" requirement |
| B.3 | ✓ if lifecycle is known and bounded          | ✗ can't model unknown future statuses | Medium                   | Low                                                   | Risky - brief doesn't specify lifecycle      |
| B.4 | **✓ direct read**                            | **✓ from event fact**                 | Medium (UPSERT + INSERT) | Low                                                   | **Chosen**                                   |


**Chosen: B.4.** Two fact tables:

- `**fact_orders`** - order-header grain, UPSERTed by the consumer on every CDC event for the order. Holds the *current* status, dates, and customer FK. This is what all four reporting endpoints read.
- `**fact_order_status_events*`* - append-only transaction fact. One row per status transition. This is the historical-fidelity record and satisfies must-have #1 on its own.
- `**fact_order_items`** - line-item grain, UPSERTed by the consumer on CDC events for `order_items`. Denormalizes parent-order status onto the item to make endpoint #3 (open pending items by product) a single-table scan.

**Rationale.** B.4 is technically a hybrid pattern (sometimes called "current-state fact" in modern Kimball treatments) and it directly maps the two requirements onto two fact tables, each optimized for one access pattern. The transaction fact for status events is also the natural landing zone for Debezium CDC events on the orders table - every UPDATE that changes the status field becomes one INSERT.

The cost is mild write amplification on status changes (UPSERT into `fact_orders` plus INSERT into `fact_order_status_events`), but both happen inside the same transaction the consumer is already using (ADR-0002 apply loop), so there is no consistency hazard.

### Status denormalization on `fact_order_items`

Endpoint #3 ("open pending items by PRODUCT_ID") requires filtering items by their parent order's status. The two ways to do this:

- **Join at query time** - `fact_order_items JOIN fact_orders ON order_id`, filter on `fact_orders.current_status`. Always correct, no duplication, but every read pays the join.
- **Denormalize parent-order status onto items** - `fact_order_items` carries `current_order_status`, updated by the consumer when the parent order's status changes.

Chosen: **denormalize**. The status is small, low-cardinality, and the endpoint is a hot path. The write-side cost is a `UPDATE fact_order_items SET current_order_status = ? WHERE order_id = ?` whenever an order's status changes - one extra statement in the apply transaction. The read-side benefit is an indexed single-table scan on the largest fact table.

This is a deliberate denormalization for query performance and is documented as such; the consumer maintains the invariant.

## Sub-decision C - Aggregation strategy for the four endpoints

### Considered options

- **C.1 - Indexed base tables only.** Each endpoint runs an aggregation query against `fact_orders` or `fact_order_items` directly. Indexes on the filter and group-by columns.
- **C.2 - Materialized views per endpoint.** One MV per endpoint, refreshed with `REFRESH MATERIALIZED VIEW CONCURRENTLY` on a timer or after each consumer batch.
- **C.3 - Incrementally maintained summary tables.** Consumer maintains endpoint-specific aggregate tables transactionally as part of the apply loop.
- **C.4 - In-memory caching layer** (Redis) in front of the API.

### Comparison


| #   | Read latency at 10M orders (D4) | Freshness                 | Write-side cost (D5)              | Implementation effort (D6) | Outcome                   |
| --- | ------------------------------- | ------------------------- | --------------------------------- | -------------------------- | ------------------------- |
| C.1 | ~50–200 ms with proper indexes  | Real-time                 | None                              | Low                        | **Chosen for v1**         |
| C.2 | < 10 ms                         | Stale by refresh interval | Refresh cost on every commit      | Medium                     | Reserved for scale        |
| C.3 | < 5 ms                          | Real-time                 | Higher per apply (4 extra writes) | High                       | Over-engineered for scope |
| C.4 | < 1 ms                          | Stale by TTL              | None on apply path                | Medium (extra service)     | Out of scope              |


**Chosen: C.1 with C.2 documented as the next step.**

**Rationale.** At the data volumes implied by the brief (low millions of orders), Postgres with composite B-tree indexes on `(current_status, delivery_date)`, `(current_status, customer_sk)`, and `(current_order_status, product_sk)` will hit p99 < 200 ms for all four endpoints - comfortably under the implicit "API-fast" expectation. Adding MVs increases compose complexity and consumer-loop work for performance headroom we don't yet need.

If a benchmark during implementation shows any endpoint missing the budget, the fallback is C.2 with `REFRESH MATERIALIZED VIEW CONCURRENTLY` invoked at the end of each apply batch - additive to the apply loop, no schema change needed. README will document this as the explicit scaling step.

## Schema layout

```
marts schema (Postgres)

dim_customer                     dim_product                      dim_date
─────────────                    ────────────                     ─────────
customer_sk PK (BIGSERIAL)       product_sk PK (BIGSERIAL)        date_key PK (INT YYYYMMDD)
customer_id NK                   product_id NK                    date
customer_name                    product_name                     year
customer_address                 barcode                          quarter
is_active                        unity_price                      month
valid_from                       is_active                        day
valid_to                         valid_from                       day_of_week
is_current                       valid_to                         day_name
                                 is_current                       is_weekend

fact_orders                                fact_order_items
────────────                               ─────────────────
order_id PK (NK from source)               order_item_id PK (NK from source)
customer_sk FK → dim_customer              order_id FK → fact_orders
order_date_key FK → dim_date               product_sk FK → dim_product
delivery_date_key FK → dim_date            customer_sk FK → dim_customer  (denorm)
current_status (degenerate)                quantity
current_status_at                          current_order_status (denorm from fact_orders)
last_updated_at                            order_date_key FK → dim_date
                                           delivery_date_key FK → dim_date
                                           last_updated_at

fact_order_status_events
────────────────────────
event_id PK (BIGSERIAL)
order_id (NK)
status
status_changed_at
event_lsn  (Kafka offset / source LSN, for ordering)
```

### Key design notes on the schema

- **Surrogate keys on dimensions, natural keys on facts.** Dimensions get `BIGSERIAL` surrogate keys to support SCD2 (multiple rows per natural key). Facts use the source natural key as their PK because the source key is stable and this simplifies upserts and traceability.
- `**fact_order_items.customer_sk` is denormalized** from the parent order. Endpoint #4 (top customers by pending orders) reads from `fact_orders`, but having `customer_sk` available on items also helps any future customer-by-product analysis.
- `**event_lsn`** on the status-events fact preserves a deterministic ordering of events even when wall-clock timestamps tie. This matters for replay correctness.
- **No `dim_status` table.** Status is a degenerate dimension; if the set of statuses ever needs descriptions or grouping, a reference table can be added without schema migration on the facts.

### Required indexes (B-tree unless noted)

- `dim_customer (customer_id, is_current)` - for SCD2 lookup during apply
- `dim_product (product_id, is_current)` - same
- `fact_orders (current_status, delivery_date_key)` - endpoints 1 and 2
- `fact_orders (current_status, customer_sk)` - endpoint 4
- `fact_order_items (current_order_status, product_sk)` - endpoint 3
- `fact_order_status_events (order_id, status_changed_at)` - history queries

## How the apply loop populates this model

Extending the loop from ADR-0002:

```
1. consume batch from Kafka
2. BEGIN transaction in postgres-analytics
3.   UPSERT staging.stg_*  (1:1 mirror of source rows)
4.   for each changed customer or product row:
        close out current SCD2 row (set valid_to, is_current=false)
        INSERT new SCD2 row (is_current=true)
5.   for each changed orders row:
        UPSERT fact_orders (current_status, dates, customer_sk via SCD2 lookup)
        if status changed:
            INSERT fact_order_status_events
            UPDATE fact_order_items SET current_order_status WHERE order_id = ?
6.   for each changed order_items row:
        UPSERT fact_order_items (with denormalized current_order_status)
7. COMMIT
8. commit Kafka offsets
```

Step 4 uses a hash diff on the SCD2-tracked attributes (`customer_name + customer_address + is_active` for customers; the analogous set for products) so unchanged updates from CDC don't generate spurious SCD2 versions.

## Consequences

### Positive

- Direct one-statement queries for all four endpoints - no `argMax`-style history reduction at read time (D1, D4).
- Full state-transition history preserved in `fact_order_status_events`, satisfying must-have #1 (D2).
- Conformed `dim_customer` and `dim_product` reusable across both fact tables (D3).
- Hybrid-fact pattern is recognizable Kimball even though it composes two patterns.
- Aggregation strategy starts simple (indexes only) with a documented upgrade path (MVs) - no premature optimization (D6).

### Negative

- **Write amplification on status changes.** A single CDC event on `orders` triggers an UPSERT (`fact_orders`) + INSERT (`fact_order_status_events`)
  - UPDATE (`fact_order_items.current_order_status` for all items in the order). Documented as a deliberate read/write tradeoff.
- **Denormalization invariants** must be maintained by the consumer. `fact_order_items.current_order_status` will drift from `fact_orders` if the consumer's order-update path is bypassed. A DQ check (ADR-0005) will assert their consistency on a schedule.
- **SCD2 hash-diff complexity.** The consumer must compute hashes over exactly the right attributes. Mistakes manifest as missing or spurious history rows. Needs unit tests.
- `**fact_order_items.customer_sk` is a denormalization too** - if a customer's SCD2 version changes between order creation and item creation, the item gets a different `customer_sk` than the parent order. Mitigated by computing both `customer_sk`s from the same `as_of` timestamp during apply.

### Neutral

- The model is intentionally larger than the strict minimum for the four endpoints. The status-events fact is not strictly required for any endpoint, but it satisfies the historical-fidelity must-have and is the natural landing zone for Debezium status events.

## Validation

The decision is validated when:

1. **Endpoint correctness.** Each of the four endpoints returns numerically identical results to a reference query computed against `staging` (the source-state mirror).
2. **Endpoint performance.** All four endpoints respond in p99 < 200 ms on the in-scope dataset.
3. **History reconstruction.** Given an `order_id` and a timestamp, `fact_order_status_events` can reconstruct the order's status at that timestamp.
4. **SCD2 correctness.** Updating a customer's address in the source DB produces exactly one new row in `dim_customer` with correct `valid_from`/`valid_to`/`is_current` on both old and new versions, and does NOT produce a new row when an unrelated field is updated.
5. **Denormalization invariant.** A DQ query asserting `fact_order_items.current_order_status = fact_orders.current_status` for the same `order_id` returns zero discrepancies.

