# ADR-0001: Use Debezium + Kafka for Source-to-Analytics CDC

## Context and Problem Statement

We are building an analytical platform for ACME Delivery Services. Tsenhe source is a live PostgreSQL database that is continuously updated by the operational system (orders, customers, products, order_items). The platform must satisfy three must-have requirements stated in the brief:

1. Query both historical and current state of records.
2. Expose a dimensional model for business queries.
3. Make data available for consumption as soon as possible.

A streaming pipeline is preferred but not strictly required.

The first architectural decision is **how data leaves the source database**, because this choice sets the latency floor, determines whether intermediate state changes are captured, and constrains every downstream layer (transport, modeling, recovery semantics).

## Decision Drivers


| ID  | Driver                                                                                                                                                                                                                                                            |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D1  | **Latency budget** - "ASAP" implies sub-minute, ideally sub-second, end-to-end.                                                                                                                                                                                   |
| D2  | **Historical fidelity** - intermediate states cannot be lost. An order may transition `pending → in_transit → delivered` faster than any reasonable poll interval.                                                                                                |
| D3  | **Source impact** - the source DB serves the operational system; ingestion must not meaningfully load it.                                                                                                                                                         |
| D4  | **Source ownership** - we do not control the source schema (no triggers, no extra audit columns).                                                                                                                                                                 |
| D5  | **Time-to-deliver** - take-home scope; infrastructure complexity must pay for itself in signal and value.                                                                                                                                                         |
| D6  | **Reviewer reproducibility** - the entire stack must run via `docker-compose up`.                                                                                                                                                                                 |
| D7  | **Source-protection / fault-tolerance** - failure or slowness of analytics-side components must not threaten the operational DB. The dominant CDC failure mode is replication-slot WAL accumulation, which can fill source disk and crash the operational system. |


## Considered Options

### Option 1 - Periodic full snapshot (truncate-and-reload)

Scheduled job runs `SELECT` * per table and replaces the analytical copy.

### Option 2 - Incremental polling on `updated_at`

Scheduled job selects rows where `updated_at > last_high_watermark` and merges into the analytical store.

### Option 3 - PostgreSQL logical replication with a Python consumer

Create a publication on the source and a logical replication slot. A Python service reads the replication stream (`pgoutput` / `wal2json`) and writes to the analytical schema.

### Option 4 - Debezium → Kafka → consumer

Run Debezium against the source, publish change events to Kafka, consume with a separate service that writes to the analytical store.

### Option 5 - Trigger-based audit table

Add `AFTER INSERT/UPDATE/DELETE` triggers on each source table that write changes to an audit table; a poller reads the audit table.

### Comparison


| #   | Option                                | Captures every change (D2) | Latency (D1)             | Source-protection on consumer failure (D7)                                        | Source-schema invasive (D4) | Compose footprint (D6) | Outcome      |
| --- | ------------------------------------- | -------------------------- | ------------------------ | --------------------------------------------------------------------------------- | --------------------------- | ---------------------- | ------------ |
| 1   | Full snapshot reload                  | ✗ history destroyed        | Minutes (poll interval)  | N/A - no streaming consumer, but full scans hurt source                           | No                          | +1 job                 | Fails D2     |
| 2   | Incremental polling on `updated_at`   | ✗ intermediate states lost | Bounded by poll interval | ✓ no slot to manage                                                               | No                          | +1 job                 | Fails D1, D2 |
| 3   | Logical replication + Python consumer | ✓                          | Sub-second               | ✗ slot WAL accumulates if Python consumer is slow or down                         | No                          | +1 service             | Fails D7     |
| 4   | **Debezium → Kafka → consumer**       | ✓                          | Sub-second               | ✓ Debezium fast-ACKs the slot; Kafka absorbs downstream slowness                  | No                          | +3 services            | **Chosen**   |
| 5   | Trigger-based audit table             | ✓                          | Bounded by poll interval | ~ audit table grows on source; doesn't crash as fast as WAL but still source-side | Yes (DDL on source)         | +1 job                 | Fails D4     |


## Decision Outcome

**Chosen: Option 4 - Debezium → Kafka → Python consumer.**

The initial draft of this ADR selected Option 3. That decision was revised after re-weighting D7 (source-protection), which had been understated. Option 3 remains a defensible choice in some contexts (greenfield infrastructure teams, anti-Kafka shops, embedded systems) but loses to Option 4 once D7 is given proper weight.

### Rationale

The dominant axis is D7. Both Options 3 and 4 use a PostgreSQL logical replication slot - the architectural difference is **what acknowledges that slot, and how quickly**:

- **Option 3** ACKs the slot only after the Python consumer has applied each change to the analytical schema. If the analytical DB is slow, or the consumer has bugs, or it falls behind, the source DB accumulates WAL indefinitely until disk fills and the operational system fails.
- **Option 4** has Debezium ACK the slot as soon as it has written the change to Kafka. **Kafka - not the source - becomes the buffer for downstream slowness.** Analytical-side incidents are decoupled from operational-DB health.

This **fast-ACK pattern** is the architectural reason production CDC at scale uses Kafka (or an equivalent log). Option 3 can be engineered to approximate it (read fast → local buffer → ACK → process buffer asynchronously), but at that point a meaningful fraction of Kafka's value proposition is being rebuilt by hand.

Secondary rationale:

- **Offset management is solved out of the box.** No custom LSN persistence, no edge cases around restart-during-apply. The time-to-deliver budget (D5) is reshuffled from "writing reliable plumbing" to "configuring well-known components" - net cost is comparable.
- **Production-readiness signal.** Debezium + Kafka is the canonical large-scale CDC pattern.
- **Reusable substrate.** Additional consumers (real-time dashboards, fraud detection, search indexers) can attach to the same Debezium topics later without changing the source-side setup.

Options 1, 2, and 5 remain disqualified for the reasons documented in the Comparison: history destruction (1, 2), latency floor bounded by poll interval (2), and source-schema invasiveness (5).

## Consequences

### Positive

- Sub-second end-to-end latency (D1).
- Every state transition captured, including deletes (D2).
- **Replication slot is decoupled from analytical-side latency.** Analytical consumer outages do not threaten the source DB (D7).
- Offset and LSN management delegated to mature tooling - no custom code to write, test, or debug for that concern (D5).
- Reusable for future consumers without rearchitecting the source side.
- Strong production-readiness signal in the artifact.

### Negative

- **Compose stack adds Kafka and Debezium Connect** beyond the consumer. Larger reviewer footprint, longer cold-start, more memory required (D6).
- **WAL accumulation risk is reduced, not eliminated.** If Debezium itself stops, the source still accumulates WAL. Slot lag must be monitored (`pg_replication_slots.confirmed_flush_lsn` vs `pg_current_wal_lsn`) and the README must call out this operational hazard.
- **Single-broker Kafka in compose** is acceptable for the take-home but is not production-grade (no replication factor, no quorum, no rack awareness). README should call this out as a deliberate scope choice.
- **More configuration surface.** Debezium connector config (publication setup, table whitelist, snapshot mode, tombstone handling), Kafka topic policies, consumer-group semantics. Each piece is small but they sum.
- DDL (source schema evolution) is still not propagated automatically. Operational note required.

### Neutral

- Locked to PostgreSQL on the source side - acceptable, source is fixed.
- Consumer is now coupled to Kafka client semantics (offset commits, group rebalances) - standard tradeoff for this pattern.

## Validation

The decision is validated when:

1. **Latency.** A change in the source DB (insert / update / delete) is reflected in the analytical schema within ≤ 2 seconds end-to-end.
2. **Source-protection (the D7 test).** Killing the analytical consumer for an extended period must NOT cause WAL accumulation on the source. Debezium continues to ACK the slot; messages buffer in Kafka. On consumer restart, it resumes from its committed Kafka offset with no data loss and no duplicate-apply effects in the analytical schema.
3. **Restart resilience.** Stop and restart the consumer mid-stream; verify at-least-once-with-idempotent-apply semantics - no missed events, no duplicate effects in dimension or fact tables.
4. **Reproducibility.** The full stack (`postgres-source`, `kafka`, `debezium-connect`, `consumer`, `postgres-analytics`, `api`) starts cleanly with `docker-compose up` on a reviewer machine without manual intervention.

