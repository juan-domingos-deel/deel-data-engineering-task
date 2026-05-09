# ADR-0002: Analytical Storage - Separate PostgreSQL Instance with Layered Schemas

## Context and Problem Statement

ADR-0001 establishes that change events flow from the source PostgreSQL via Debezium into Kafka, where a Python consumer reads them. This ADR resolves **where the consumer writes** and **how the analytical schema is organized**.

Two questions are entangled but separable:

1. **Storage location.** Same physical Postgres as the source, a separate dedicated Postgres instance, or a different DB technology entirely?
2. **Schema layering.** Should the consumer write straight to dimensional tables, or pass through one or more intermediate layers (raw, staging, marts)?

The brief explicitly suggests PostgreSQL and offers two location choices: "create a schema in the existing DB or instantiate a new one in the Docker stack." This ADR makes that choice explicit and adds the layering decision that the brief leaves open.

## Decision Drivers


| ID  | Driver                                                                                                                                                                   |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| D1  | **Workload isolation** - analytical queries (aggregations, scans, joins) must not contend for resources with the operational DB.                                         |
| D2  | **Dimensional-model fitness** - storage must support a Kimball star schema (SCDs, surrogate keys, fact tables).                                                          |
| D3  | **Replay/debuggability** - when transformation logic changes (e.g., SCD rule fix), it must be possible to re-derive marts without re-consuming Kafka from the beginning. |
| D4  | **Reviewer reproducibility** - full stack runs via `docker-compose up`.                                                                                                  |
| D5  | **Time-to-deliver** - take-home scope; new DB technologies must pay for themselves in signal.                                                                            |
| D6  | **Alignment with the brief** - Postgres as destination is the suggested path.                                                                                            |
| D7  | **API query performance** - the four reporting endpoints must serve in sub-second p99 latency at the data volumes implied (millions of records).                         |


## Considered Options

The two axes are independent. Each option below is a `(location, layering)` pair.

### Storage location options

- **L-A - Schema(s) in the source Postgres.** Add `staging` and `marts` schemas alongside the operational tables in the same DB instance.
- **L-B - Separate Postgres instance.** A second `postgres-analytics` container with its own credentials, storage volume, and tuning.
- **L-C - OLAP database** (DuckDB embedded in the API, ClickHouse, etc.). Purpose-built for analytical workloads.
- **L-D - Lakehouse** (Iceberg / Delta on MinIO, queried by Trino or DuckDB). Production-grade for the data volumes mentioned in the brief.

### Schema-layering options

- **S-1 - Marts only.** Consumer applies SCD logic and writes directly to `dim_`* and `fact_*` tables.
- **S-2 - Staging + marts.** Consumer materializes a current-state mirror of source tables in `staging`, then SQL transformations populate `marts`. Kafka itself is the immutable raw log.
- **S-3 - Raw + staging + marts.** Persist Kafka events as-is into a `raw` schema, then derive `staging`, then `marts`.
- **S-4 - Separate database per layer.** One Postgres instance per medallion layer (bronze / silver / gold).

## Comparison

### Storage location


| #   | Option                         | Workload isolation (D1)   | Compose footprint (D4) | Effort (D5)       | Aligns with brief (D6)     | Performance at 10M+ rows (D7)      | Outcome             |
| --- | ------------------------------ | ------------------------- | ---------------------- | ----------------- | -------------------------- | ---------------------------------- | ------------------- |
| L-A | Schema in source Postgres      | ✗ shares CPU/IO with OLTP | +0                     | Lowest            | ✓ explicitly offered       | Bottlenecked by source-DB tuning   | Fails D1            |
| L-B | **Separate Postgres instance** | ✓ fully isolated          | +1                     | Low               | ✓ explicitly offered       | Adequate with proper indexes / MVs | **Chosen**          |
| L-C | OLAP DB (DuckDB / ClickHouse)  | ✓                         | +1 (or in-process)     | Medium (new tech) | ~ deviates from suggestion | Better than Postgres               | Possible, see below |
| L-D | Lakehouse (Iceberg + Trino)    | ✓                         | +3–4                   | High              | ✗ deviates significantly   | Excellent                          | Out of scope        |


### Schema layering


| #   | Option                | Replay without re-consuming Kafka (D3)  | Storage cost           | Transformation locus                         | Outcome                                       |
| --- | --------------------- | --------------------------------------- | ---------------------- | -------------------------------------------- | --------------------------------------------- |
| S-1 | Marts only            | ✗ must re-read Kafka from earliest      | Lowest                 | Python (consumer)                            | Fails D3                                      |
| S-2 | **Staging + marts**   | ✓ replay marts SQL from staging         | Moderate               | SQL (with consumer doing upserts to staging) | **Chosen**                                    |
| S-3 | Raw + staging + marts | ✓ + full event-history queryable in SQL | Highest                | SQL throughout                               | Marginal gain over S-2 - Kafka already is raw |
| S-4 | DB per layer          | ✓                                       | Highest + ops overhead | Cross-DB ETL                                 | Over-engineered for scope                     |


## Decision Outcome

**Chosen: L-B (separate Postgres instance) + S-2 (staging + marts layering).**

The consumer materializes a current-state mirror of each source table into the `staging` schema, and SQL transformations (executed by the consumer after each Kafka batch commit, in the same DB transaction) populate `marts` with the dimensional model. Kafka topic retention serves as the raw layer.

### Rationale

**Why L-B over L-A.** The whole reason for adopting CDC in ADR-0001 was to decouple analytical workload from the operational DB. Co-locating the analytical tables in the source Postgres reintroduces resource contention at the storage layer, undoing half of that decoupling. The compose-footprint cost (+1 container) is small; the architectural integrity gain is large.

**Why not L-C/L-D.** A purpose-built OLAP store (ClickHouse, DuckDB, Iceberg+Trino) would query faster, but at the data volumes implied by the brief (low millions of rows), Postgres with appropriate indexes and a materialized view per endpoint comfortably meets D7. The brief explicitly suggests Postgres, so picking ClickHouse or a lakehouse signals "didn't read the prompt" rather than architectural sophistication. L-C/L-D are correctly noted in the README as the natural next step at higher scale.

**Why S-2 over S-1.** Staging exists for one specific reason: it lets us re-apply marts logic without re-consuming Kafka. Concretely: if we discover a bug in the `dim_customer` SCD logic three days into operation, we need to fix it without losing history or replaying tens of millions of CDC events. With S-2 the fix is a `TRUNCATE marts.dim_customer; INSERT … FROM staging …` - minutes, not hours. With S-1 it requires Kafka offset reset and a full re-consume, which is operationally painful and may not even be possible if Kafka retention has expired.

**Why not S-3.** Persisting raw Kafka events into a Postgres `raw` schema duplicates what Kafka already provides - an immutable, ordered, replayable event log. The duplicate buys nothing for our reporting workload and adds storage cost and write amplification. **Kafka is the raw layer**; we don't need a second one in Postgres. The exception would be if we needed SQL-queryable forensics over the full event history (e.g. compliance audits)

- not a requirement here.

**Why not S-4.** Separating layers across physical databases is a production-scale concern that buys isolation between teams owning different layers. With one engineer, one codebase, and one compose stack, it's pure overhead.

### Concrete schema layout

```
postgres-analytics
├── staging                      (1:1 mirror of source, current state only)
│   ├── stg_customers            -- upserted by consumer; PK = source customer_id
│   ├── stg_products             -- upserted by consumer; PK = source product_id
│   ├── stg_orders               -- upserted by consumer; PK = source order_id
│   └── stg_order_items          -- upserted by consumer; PK = source order_item_id
└── marts                        (Kimball star schema; defined in ADR-0003)
    ├── dim_customer             -- SCD2
    ├── dim_product              -- SCD2
    ├── dim_date                 -- generated, static
    ├── fact_orders              -- header grain
    └── fact_order_items         -- line-item grain
```

The consumer's apply loop is:

```
1. consume batch from Kafka
2. BEGIN transaction in postgres-analytics
3.   UPSERT into staging.stg_*
4.   apply incremental SQL: staging → marts (SCD2 logic, fact appends)
5. COMMIT
6. commit Kafka offsets
```

Steps 3–4 in one transaction; offset commit only after DB commit succeeds. This gives at-least-once semantics with idempotent application, satisfying the restart resilience criterion from ADR-0001.

## Consequences

### Positive

- **Workload isolation** - operational DB resources are not affected by analytical scans or aggregations (D1).
- **Replay-friendly** - SCD or marts logic changes don't require Kafka replay (D3).
- **Dimensional model is unconstrained** - Postgres supports surrogate keys, composite indexes, and materialized views needed for SCD2 + star schema (D2).
- **Simple operational story** - two Postgres instances and Kafka, all well-understood components (D4, D5).
- **Reviewer-friendly** - same DB engine source-and-destination means the reviewer doesn't need to learn a new tool to inspect the result (D6).

### Negative

- **+1 Postgres container** - memory and disk overhead on the reviewer machine. With Kafka and Debezium Connect from ADR-0001 the compose stack now totals 6–7 services; resource budget needs README documentation.
- **Postgres is not an OLAP database.** At ~10–50M rows per fact table, performance will degrade without careful indexing and materialized views. The four required endpoints will need either MVs refreshed by the consumer loop, or pre-aggregated summary tables. The README must explicitly call this out as the scaling boundary, with ClickHouse or Iceberg+Trino named as the next step.
- **Storage duplication** - staging duplicates the source's current state. Acceptable for the data volumes in scope; documented for transparency.
- **Schema drift risk** - staging schemas must track source schemas. DDL on the source still requires manual coordination (called out in ADR-0001 as well).

### Neutral

- Locked to Postgres on the destination side. The brief endorses this; the consumer's destination layer is small enough to swap if a future ADR upgrades to ClickHouse or a lakehouse.

## Validation

The decision is validated when:

1. **Workload isolation.** Running the four reporting endpoints at sustained load on the analytics DB produces no measurable change in source DB query latency.
2. **Replay test.** Drop and recreate `marts.dim_customer` from `staging.stg_customers` without restarting the consumer or rewinding Kafka offsets. Counts match before and after.
3. **API performance.** All four endpoints respond in p99 < 200 ms on the in-scope data volume, with the supporting indexes / materialized views defined in ADR-0003.

