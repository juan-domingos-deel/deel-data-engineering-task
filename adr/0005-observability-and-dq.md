# ADR-0005: Observability and Data Quality - Prometheus, Structured Logs, Soda Core

## Context and Problem Statement

The previous ADRs commit to architectural choices whose correctness depends on properties that aren't visible from the outside: replication slot WAL backlog, consumer apply latency, denormalization invariants, API tail latency. This ADR resolves how those properties are measured, surfaced, and checked.

It also closes loops left open in earlier ADRs:

- ADR-0001's D7 named WAL accumulation as the dominant operational hazard but did not specify monitoring. This ADR does.
- ADR-0003 introduced denormalization invariants (`fact_order_items. current_order_status` mirrors `fact_orders.current_status`) and deferred the DQ check to here.
- ADR-0004 added a `request_id` middleware and a `/ready` endpoint without defining what makes the service "ready." That definition lives here.

Five sub-decisions are resolved together:

- **A. SLOs** - what we promise, in measurable terms.
- **B. Metrics** - how SLOs and operational signals are emitted and stored.
- **C. Logging** - format, correlation, retention.
- **D. Data quality** - framework, rules, scheduling, alerting hook.
- **E. Compose footprint** - what services are added.

Distributed tracing is deliberately out of scope (see end of ADR).

## Decision Drivers


| ID  | Driver                                                                                                                                                                               |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| D1  | **Freshness measurability** - the brief's "ASAP" must-have implies a freshness SLO that can be observed and alerted on.                                                              |
| D2  | **DQ enforceability** - the dimensional model (ADR-0003) has invariants that must hold; violations must be detectable, not hopeful.                                                  |
| D3  | **Operational-hazard visibility** - replication-slot WAL backlog (ADR-0001 D7) must be observable before it threatens the source DB.                                                 |
| D4  | **Reviewer-friendliness** - a visual dashboard is more legible than prose claims about quality.                                                                                      |
| D5  | **Compose footprint** - observability stack must be light enough for a reviewer machine.                                                                                             |
| D6  | **Time-to-deliver** - observability must serve the brief, not gold-plate it.                                                                                                         |


## Sub-decision A - Service Level Objectives

The SLOs are the contract. Every metric, dashboard panel, and DQ rule exists to confirm or deny one of these.


| SLO                        | Target                                                     | Measurement                                                            |
| -------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------- |
| **End-to-end freshness**   | p95 ≤ 2 s, p99 ≤ 5 s                                       | `now() - source_commit_ts` at the moment a row is applied to `marts.`* |
| **API latency**            | p99 < 200 ms per endpoint                                  | Prometheus histogram on the API                                        |
| **API availability**       | ≥ 99.9% successful (non-5xx)                               | Prometheus counter ratio                                               |
| **Consumer apply success** | ≥ 99.9% of Kafka messages applied without retry-exhaustion | Prometheus counter ratio                                               |
| **Replication slot lag**   | < 50 MB WAL retained on source at p95                      | `pg_replication_slots` polled to Prometheus                            |
| **DQ pass rate**           | 100% on critical rules, ≥ 99% on non-critical              | Soda Core scan results emitted as metrics                              |


The freshness SLO is the headline metric: it is the operationalization of the brief's "ASAP" must-have, and the rest of the architecture exists to make it achievable.

## Sub-decision B - Metrics

### Considered options

- **B.1 - Prometheus + Grafana.** Pull-based metrics, time-series storage, rich dashboards, industry standard.
- **B.2 - OpenTelemetry → OTel Collector → Prometheus.** Full vendor-neutral pipeline; heavier setup.
- **B.3 - StatsD + Graphite.** Older stack, less common in modern Python.
- **B.4 - Cloud-native (CloudWatch / Datadog).** Out of scope - must run in Docker Compose locally.

### Comparison


| #   | Standardness                   | Compose footprint (D6) | Setup time (D7) | Visual artifact (D5) | Outcome                           |
| --- | ------------------------------ | ---------------------- | --------------- | -------------------- | --------------------------------- |
| B.1 | ✓ industry default             | +2 services            | Low             | ✓ Grafana            | **Chosen**                        |
| B.2 | ✓ vendor-neutral, future-proof | +3 services            | Medium          | ✓ via Grafana        | Reserved for production migration |
| B.3 | ~ legacy                       | +2 services            | Medium          | ~                    | No advantage over B.1             |
| B.4 | N/A                            | N/A                    | N/A             | N/A                  | Not deployable in compose         |


**Chosen: B.1 - Prometheus + Grafana.**

**Rationale.** The combined memory budget for Prometheus + Grafana is small (~400–500 MB). OpenTelemetry would be the right answer for a multi-language production stack; for a Python-only take-home it adds a collector hop without changing what gets measured.

### What gets emitted


| Source                                        | Metrics                                                                                                                                                                                                        |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **API** (FastAPI)                             | `http_requests_total{method,path,status}`, `http_request_duration_seconds_bucket{path}` (histogram), `db_pool_in_use`, `db_pool_idle`                                                                          |
| **Consumer**                                  | `kafka_messages_consumed_total{topic}`, `apply_batch_duration_seconds`, `apply_errors_total{kind}`, `dim_scd2_versions_written_total{dimension}`, `freshness_seconds` (gauge: now − source_commit_ts at apply) |
| **Postgres source** (via `postgres_exporter`) | `pg_replication_slot_lag_bytes`, `pg_stat_database_`*                                                                                                                                                          |
| **Kafka** (via `kafka-exporter`)              | `kafka_consumergroup_lag{group,topic,partition}`                                                                                                                                                               |
| **Soda Core**                                 | `soda_check_passed_total{check}`, `soda_check_failed_total{check}`                                                                                                                                             |


The `freshness_seconds` gauge is the single most important metric in the system. It is computed by the consumer using the `source_ts_ms` field that Debezium attaches to every change event, and emitted at the moment the analytical-side transaction commits. This is the only metric that directly measures the brief's must-have #3.

### Implementation notes

- API: `prometheus-fastapi-instrumentator` provides RED metrics with one line of wiring; custom gauges for the DB pool added via `prometheus_client`.
- Consumer: `prometheus_client` directly; `start_http_server(8001)` exposes `/metrics` independently of any HTTP framework.
- Postgres-source exporter and Kafka exporter are off-the-shelf containers.
- All Prometheus scrape targets defined in a single `prometheus.yml` shipped with the repo.

## Sub-decision C - Logging

### Considered options

- **C.1 - Plain Python logging to stdout.** What `logging.basicConfig` gives you. Unstructured.
- **C.2 - `structlog` emitting JSON to stdout.** Structured fields, easily parsed, forwarded by Docker's logging driver.
- **C.3 - Centralized logging stack (Loki / ELK).** Out of scope for compose footprint.

### Comparison


| #   | Searchable in production (D4) | Compose footprint (D6) | Effort (D7) | Outcome             |
| --- | ----------------------------- | ---------------------- | ----------- | ------------------- |
| C.1 | ✗ requires regex grep         | +0                     | Lowest      | Insufficient signal |
| C.2 | ✓ JSON parses cleanly         | +0                     | Low         | **Chosen**          |
| C.3 | ✓                             | +1–3 services          | Medium      | Reserved            |


**Chosen: C.2 - `structlog` JSON to stdout.**

### Conventions

Every log line is a JSON object with at minimum:

```json
{
  "ts":         "2026-05-08T14:30:00.123Z",
  "level":      "INFO",
  "logger":     "consumer.apply",
  "message":    "applied batch",
  "request_id": "uuid-or-empty",
  "kafka_offset": 12345,
  "batch_size": 100,
  "duration_ms": 47
}
```

`request_id` is sourced from ADR-0004's middleware on the API side and from the Kafka message offset on the consumer side. The same `request_id` appears in metrics labels (where cardinality permits) and DQ failure records, so a single value can pivot across all three telemetry sources.

Logs go to stdout only; aggregation is the deployment's problem (Docker's `json-file` driver in compose, a real log shipper in production).

## Sub-decision D - Data quality

The dimensional model has invariants that the consumer maintains by construction. DQ exists because "by construction" is a hypothesis until checked.

### Considered options

- **D.1 - Soda Core.** Declarative YAML rules over SQL, scheduled scans, Prometheus and JSON output formats.
- **D.2 - Great Expectations.** Heavier framework, richer ecosystem, more setup.
- **D.3 - dbt tests.** Requires adopting dbt, which would reframe the consumer's transformation layer.
- **D.4 - Custom SQL assertions** in a Python scheduler.

### Comparison


| #   | Declarative | Compose footprint (D6) | Operational maturity | Effort (D7) | Outcome                    |
| --- | ----------- | ---------------------- | -------------------- | ----------- | -------------------------- |
| D.1 | ✓           | +1 (cron-style runner) | High                 | Low         | **Chosen**                 |
| D.2 | ✓           | +1                     | High                 | Medium-high | Overkill for rule count    |
| D.3 | ✓           | +1 + dbt adoption      | High                 | High        | Doesn't fit pipeline shape |
| D.4 | ✗           | +0                     | Low                  | Low         | Loses senior signal        |


**Chosen: D.1 - Soda Core.**

### DQ rules

Defined in `dq/checks.yml` and run on schedule by a small Python runner that exposes results as Prometheus metrics.

```yaml
checks for marts.dim_customer:
  - row_count > 0
  - duplicate_count(customer_id, is_current) = 0:
      filter: is_current = true
      name: "exactly one current SCD2 row per customer_id"
  - missing_count(customer_sk) = 0
  - missing_count(valid_from) = 0

checks for marts.dim_product:
  - row_count > 0
  - duplicate_count(product_id, is_current) = 0:
      filter: is_current = true
      name: "exactly one current SCD2 row per product_id"

checks for marts.fact_orders:
  - missing_count(customer_sk) = 0
  - invalid_count(current_status) = 0:
      valid values: [open, in_progress, delivered, cancelled, returned]
      name: "status within known set"
  - failed rows:
      name: "every fact_orders.customer_sk exists in dim_customer"
      fail query: |
        SELECT f.order_id
        FROM marts.fact_orders f
        LEFT JOIN marts.dim_customer c ON c.customer_sk = f.customer_sk
        WHERE c.customer_sk IS NULL

checks for marts.fact_order_items:
  - failed rows:
      name: "denormalization invariant: items.current_order_status = orders.current_status"
      fail query: |
        SELECT i.order_item_id, i.current_order_status, f.current_status
        FROM marts.fact_order_items i
        JOIN marts.fact_orders f ON f.order_id = i.order_id
        WHERE i.current_order_status IS DISTINCT FROM f.current_status
```

### Severity and scheduling


| Rule                                             | Severity      | Frequency    |
| ------------------------------------------------ | ------------- | ------------ |
| SCD2 uniqueness (one is_current per natural key) | **Critical**  | Every 60 s   |
| Denormalization invariant on items               | **Critical**  | Every 60 s   |
| Referential integrity (fact → dim)               | **Critical**  | Every 5 min  |
| Status within known set                          | Warning       | Every 5 min  |
| Row counts > 0                                   | Informational | Every 15 min |


Critical failures emit `soda_check_failed_total` with `severity="critical"`, which (in production) would page; in compose, they appear on the Grafana dashboard's red panel.

## Sub-decision E - Compose footprint

### Final stack composition

The full `docker-compose.yml` after all five ADRs:


| Service                             | Source         | Memory budget |
| ----------------------------------- | -------------- | ------------- |
| `postgres-source`                   | brief-provided | ~256 MB       |
| `postgres-analytics`                | ADR-0002       | ~256 MB       |
| `kafka` (single broker, KRaft mode) | ADR-0001       | ~512 MB       |
| `debezium-connect`                  | ADR-0001       | ~512 MB       |
| `consumer`                          | this project   | ~256 MB       |
| `api`                               | ADR-0004       | ~256 MB       |
| `prometheus`                        | this ADR       | ~256 MB       |
| `grafana`                           | this ADR       | ~256 MB       |
| `postgres-exporter` (×2 instances)  | this ADR       | ~64 MB each   |
| `kafka-exporter`                    | this ADR       | ~64 MB        |
| `soda-runner` (small scheduler)     | this ADR       | ~128 MB       |


**Total estimated memory**: ~3.0 GB. README must call this out so reviewers allocate appropriate Docker resources (default Docker Desktop allocations on 8 GB machines may be tight).

This is the compose-footprint cost of "production-grade observability"; the README must defend it explicitly. The fallback if it's too heavy is to skip Grafana and the exporters in a `docker-compose.lite.yml` for cold-start testing - same metrics, no dashboard.

## Decision Outcome

**Adopted as a bundle:**

- SLOs from sub-decision A as the explicit observability contract.
- **Prometheus + Grafana** for metrics, with one preloaded dashboard shipped in `grafana/provisioning/dashboards/` that surfaces the freshness SLO, replication slot lag, consumer apply latency, API RED metrics, and DQ pass rate.
- `**structlog` JSON to stdout** for logging, with `request_id` correlation across API, consumer, and DQ.
- **Soda Core** for declarative DQ, scheduled by a small Python runner, results emitted as Prometheus metrics.

The dashboard is itself a deliverable artifact. A reviewer who runs `docker-compose up`, hits the API a few times, and opens `http://localhost:3000` should see a working dashboard with non-trivial data - that visual confirmation is worth more than any prose claim about quality.

## How the pieces compose at runtime

```
┌─────────────────────┐
│ postgres-source     │──── WAL ──┐
└─────────────────────┘           ▼
                          ┌────────────────┐
                          │ debezium-connect│──── topics ────┐
                          └────────────────┘                  ▼
                                                       ┌──────────┐
                                                       │  kafka   │
                                                       └──────────┘
                                                            │
                          ┌──────── /metrics ─────┐         ▼
                          │                       │ ┌─────────────┐
                          │                       │ │  consumer   │
                          ▼                       │ └─────────────┘
                  ┌──────────────┐                │        │
                  │ prometheus   │◄───/metrics────┤        ▼
                  └──────────────┘                │ ┌─────────────────┐
                          │                       │ │ postgres-       │
                          ▼                       │ │ analytics       │
                  ┌──────────────┐                │ └─────────────────┘
                  │   grafana    │                │        │
                  └──────────────┘                │        ▼
                          ▲                       │ ┌─────────────┐
                          └─────── /metrics ──────┴─│     api     │
                                                    └─────────────┘
                                                          │
                                                          ▼
                                                ┌─────────────────┐
                                                │  soda-runner    │
                                                │  scans every    │
                                                │   60 / 300 s    │
                                                └─────────────────┘
```

## Consequences

### Positive

- **Freshness is a first-class metric**, not a hope (D1).
- **Denormalization invariants are checked**, satisfying the validation criteria left open in ADR-0003 (D2).
- **WAL backlog is monitored**, closing the loop on ADR-0001's D7 (D3).
- **Visual artifact** - a working Grafana dashboard is one of the most legible signals a reviewer can encounter (D4, D5).
- **Soda Core matches modern data-platform tooling** and demonstrates the declarative-DQ paradigm rather than ad-hoc SQL.
- **Same `request_id` pivots across logs, metrics, and DQ records** - small detail, large debugging payoff in production.

### Negative

- **Compose stack grows to ~10 services / ~3 GB.** Documented in README with explicit resource expectations and a `compose.lite.yml` fallback that omits Grafana and exporters.
- **Soda Core adds a dependency and a runner.** The runner is small (~50 LOC of Python invoking `soda scan`) but it's still a service to operate.
- **No distributed tracing.** Cross-service causality (API → DB, consumer → DB → DQ) is reconstructed via `request_id` correlation rather than spans. Acceptable for scope; OpenTelemetry tracing is the upgrade path.
- **Grafana dashboard ships as JSON in the repo.** Edits to it during exploration must be exported back to the JSON or they are lost on container restart. README documents this.
- **No alerting.** Prometheus rules can fire alerts but compose has no receiver; an Alertmanager + email/webhook target is out of scope and noted as next step.

### Neutral

- Single-replica everything (Prometheus, Grafana). Fine for compose; in production, Prometheus would be HA-paired or replaced by a managed TSDB.

## Validation

The decision is validated when:

1. **Freshness measurable.** A row inserted in `postgres-source` produces a `freshness_seconds` sample on the consumer's `/metrics` within ≤ 2 s.
2. **WAL hazard observable.** Stopping the consumer for 60 s causes the `pg_replication_slot_lag_bytes` metric to grow visibly in Prometheus, then return to baseline after the consumer restarts. (Confirms the ADR-0001 D7 monitoring loop works in practice.)
3. **DQ catches violations.** Manually breaking the denormalization invariant (`UPDATE marts.fact_order_items SET current_order_status = 'broken' WHERE order_item_id = (SELECT MIN(order_item_id) FROM …)`) causes the corresponding Soda check to fail within one scan cycle and appear on the Grafana dashboard.
4. **Logs correlate.** A `request_id` from an API response can be grepped in container logs to find the exact request, the SQL it executed, and any downstream consumer activity in the same window.
5. **Dashboard renders.** A reviewer running `docker-compose up` from a fresh clone sees the freshness SLO, RED metrics, and DQ pass rate panels populated within 60 s.

