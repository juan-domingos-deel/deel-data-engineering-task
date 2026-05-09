# Deel Streaming Take-Home - ACME Delivery Online Analytics

A streaming analytics pipeline that turns the ACME Delivery operational PostgreSQL into a queryable Kimball star with sub-second freshness, served through a documented REST API.

The architecture is laid out across five ADRs (`adr/0001` … `adr/0005`). TL;DR:

```
postgres-source ─▶ debezium ─▶ kafka ─▶ python consumer ─▶ postgres-analytics ─▶ FastAPI
                                                          │
                                                          └─▶ Soda Core (DQ) ─▶ Prometheus ─▶ Grafana
```

## Quickstart

> Reviewer note: the full stack runs ~10 services and ~3 GB RAM. If your
> Docker Desktop allocation is tight, jump to **Lite mode** below.

```bash
docker-compose up -d
```

After ~60 s everything should be healthy. Service URLs:


| What                  | Where                                                          |
| --------------------- | -------------------------------------------------------------- |
| API (Swagger UI)      | [http://localhost:8000/docs](http://localhost:8000/docs)       |
| API                   | [http://localhost:8000/v1/](http://localhost:8000/v1/)...      |
| Grafana (anon viewer) | [http://localhost:3000](http://localhost:3000)                 |
| Prometheus            | [http://localhost:9090](http://localhost:9090)                 |
| Consumer `/metrics`   | [http://localhost:8001/metrics](http://localhost:8001/metrics) |
| Source Postgres       | `localhost:5432` (finance_db_user / 1234)                      |
| Analytics Postgres    | `localhost:5433` (analytics_user / analytics_pw)               |


The source DB seeds itself with sample data and re-runs `update_customers`, `update_products`, `generate_orders` on cron - so by the time you load the API, you'll have hundreds to thousands of orders flowing through.

### Lite mode (no observability)

```bash
docker-compose -f docker-compose.yaml -f docker-compose.lite.yml up -d
```

Drops Prometheus, Grafana, exporters, and the Soda runner. ~1.6 GB RAM.

## The four reporting endpoints

All mounted under `/v1`. All responses use the standard envelope `{"data": [...], "meta": {"count": N, "generated_at": "..."}}`.

```bash
# 1. Open orders by delivery_date and status
curl 'http://localhost:8000/v1/analytics/orders?status=open' | jq

# 2. Top 3 delivery dates with most open orders
curl 'http://localhost:8000/v1/analytics/orders/top?limit=3' | jq

# 3. Open pending items by product_id
curl 'http://localhost:8000/v1/analytics/orders/products?limit=10' | jq

# 4. Top 3 customers with most pending orders
curl 'http://localhost:8000/v1/analytics/orders/customers?status=open&limit=3' | jq
```

The full OpenAPI schema is at `/v1/openapi.json` and Swagger UI at `/docs`.

## "Open" status mapping

The brief uses the word "open" but the source DB emits literal statuses: `PENDING`, `PROCESSING`, `REPROCESSING`, `COMPLETED`. We define `**open ≡ status IS DISTINCT FROM 'COMPLETED'**`, encoded once as the `marts.is_open_status(text)` SQL function so API endpoints, ad-hoc queries, and DQ rules all share the same definition. Choosing "everything except COMPLETED" instead of an explicit allow-list is the safe failure mode - any new status added upstream defaults to "needs attention" rather than silently dropping out of dashboards.

## Architecture


| Layer             | Service                                   | ADR            |
| ----------------- | ----------------------------------------- | -------------- |
| CDC source        | `transactions-db`                         | provided       |
| CDC capture       | `kafka-connect` (Debezium) + `kafka`      | ADR-0001       |
| Streaming bus     | `kafka`                                   | ADR-0001       |
| Apply / transform | `consumer`                                | ADR-0002, 0003 |
| Analytical store  | `postgres-analytics` (`staging`, `marts`) | ADR-0002       |
| Serving           | `api` (FastAPI + asyncpg)                 | ADR-0004       |
| Observability     | `prometheus`, `grafana`, `*-exporter`     | ADR-0005       |
| Data quality      | `soda-runner`                             | ADR-0005       |


### Dimensional model (`marts` schema)

- `dim_customer` - SCD2, hash-diff over `(customer_name, customer_address, is_active)`
- `dim_product` - SCD2, hash-diff over `(product_name, barcode, unity_price, is_active)`
- `dim_date` - pre-generated 2020-01-01 through 2030-12-31
- `fact_orders` - order-header grain, current state (UPSERTed); status is a degenerate dim
- `fact_order_items` - line-item grain, denormalized parent `current_order_status` for read-time scans
- `fact_order_status_events` - append-only transaction fact, one row per status transition

The hybrid `(current-state fact + status-events fact)` pattern satisfies both must-haves at once: current-state queries are direct one-statement reads, and `fact_order_status_events` preserves the full state-transition history the brief requires (ADR-0003 § Sub-decision B).

### Apply loop

Per ADR-0002 § Apply loop, the consumer runs:

```
1. consume batch from Kafka
2. BEGIN transaction in postgres-analytics
3.   UPSERT staging.stg_*
4.   SCD2 close-out + insert on dim_customer / dim_product (hash-diff)
5.   UPSERT fact_orders, INSERT fact_order_status_events on transitions,
     UPDATE items.current_order_status denorm
6.   UPSERT fact_order_items
7. COMMIT
8. commit Kafka offsets
```

Step 8 only after step 7 succeeds → at-least-once semantics, idempotent because UPSERTs and SCD2 hash-diffs absorb duplicates.

## Operational hazards

- **WAL accumulation on the source.** If the Debezium connector itself stops (not just the consumer), the replication slot stops being ACKed and source disk fills. ADR-0001 § Consequences. Slot lag is monitored via `pg_replication_slots_lag_bytes` on the Grafana dashboard.
- **Single-broker Kafka, single-replica everything.** Acceptable for the take-home; production needs replication factor ≥ 3, Alertmanager, etc.
- **No cache, no MV.** ADR-0004 § Sub-decision D explains why; both are documented as the next step if traffic grows.
- **SCD2 hash-diff is exact.** Adding a new tracked attribute requires also updating `consumer/consumer/apply/dims.py` `CUSTOMER_TRACKED` / `PRODUCT_TRACKED`. Drift = missing or spurious history rows.

## Verifying the stack

After `docker-compose up`, wait ~60 s, then:

```bash
# 1. End-to-end: trigger a source-side change, watch freshness on consumer.
docker exec transactions-db psql -U finance_db_user -d finance_db \
  -c "CALL update_customers(1);"
sleep 2
curl -s localhost:8001/metrics | grep '^freshness_seconds'

# 2. Hit the four endpoints.
for path in 'orders?status=open' 'orders/top?limit=3' \
            'orders/products?limit=5' 'orders/customers?limit=3'; do
  echo "/v1/analytics/$path"
  curl -s "localhost:8000/v1/analytics/$path" | jq '.meta'
done

# 3. Source-protection test: stop the consumer, confirm Debezium keeps acking
#    the slot (slot lag stays bounded).
docker stop consumer
sleep 30
docker exec transactions-db psql -U finance_db_user -d finance_db -c \
  "SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag
   FROM pg_replication_slots;"
docker start consumer

# 4. DQ violation, caught by Soda.
docker exec postgres-analytics psql -U analytics_user -d analytics_db -c \
  "UPDATE marts.fact_order_items SET current_order_status='broken'
   WHERE order_item_id = (SELECT MIN(order_item_id) FROM marts.fact_order_items);"
# Wait ≤60 s, then check Grafana 'Soda DQ checks' panel.
```

## Project layout

```
deel-data-engineering-task/
├── adr/                                  # 5 ADRs documenting all choices
├── api/                                  # FastAPI + asyncpg, raw SQL, Pydantic
├── consumer/                             # confluent-kafka + psycopg3 sync
├── db-scripts/
│   ├── initialize_db_ddl.sql             # source-side seed (provided)
│   └── analytics/                        # marts + staging DDL
├── debezium/                             # connector config (provided)
├── docker/{postgres-db,postgres-analytics}/
├── observability/                        # prometheus, grafana, soda
├── docker-compose.yaml                   # full stack
├── docker-compose.lite.yml               # observability-free override
└── README.md
```

## Decisions and tradeoffs

The five ADRs are the long-form. One-liners:

- **ADR-0001 - Debezium → Kafka → consumer.** Decoupling ACK of the replication slot from the analytical apply path is the dominant reason to put Kafka in the picture; it keeps consumer slowness from threatening the operational DB.
- **ADR-0002 - Separate `postgres-analytics` + staging/marts.** Workload isolation is the architectural win; staging exists specifically so marts logic can be replayed without rewinding Kafka.
- **ADR-0003 - Star schema, SCD2, hybrid facts.** Current-state fact for the four endpoints, append-only events fact for the history must-have. Status is a degenerate dim; denormalizing parent-order status onto items is a deliberate read/write tradeoff.
- **ADR-0004 - FastAPI + asyncpg + raw SQL, no cache.** Cache TTL is a freshness regression; ORMs hide what Postgres actually executes.
- **ADR-0005 - Prometheus + Grafana + structlog + Soda.** Freshness is the headline SLO; replication slot lag is the operational hazard monitor. Same `request_id` correlates logs / metrics / DQ.

## Next steps (out of scope)

- Materialized views per endpoint (ADR-0003 C.2) once base-table queries miss the latency budget.
- ClickHouse or Iceberg+Trino if the data volume outgrows Postgres.
- Alertmanager + paging.
- OpenTelemetry tracing (currently `request_id` correlation only).
- API auth, rate limiting, cursor pagination.

For any questions, see the ADRs in `adr/` - they document the *why* behind every choice.