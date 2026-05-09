"""Consumer entrypoint.

Apply loop per ADR-0002 + ADR-0003:
  1. consume batch from Kafka
  2. BEGIN transaction
  3. UPSERT staging.stg_*
  4. SCD2 close-out + insert on dim_customer / dim_product
  5. UPSERT fact_orders, INSERT fact_order_status_events, UPDATE items.denorm
  6. UPSERT fact_order_items
  7. COMMIT
  8. commit Kafka offsets
"""

import signal
import time
from datetime import datetime, timezone

from .apply import dims, facts_items, facts_orders, staging
from .config import settings
from .db import close_pool, init_pool, transaction
from .debezium import ChangeEvent
from .kafka_client import build_consumer, commit_offsets, poll_batch
from .logging import configure_logging, get_logger
from .metrics import (
    apply_batch_duration_seconds,
    apply_errors_total,
    freshness_seconds,
    messages_consumed_total,
    start_metrics_server,
)

log = get_logger("consumer.apply")


def _split_by_table(events: list[ChangeEvent]) -> dict[str, list[ChangeEvent]]:
    by_table: dict[str, list[ChangeEvent]] = {}
    for e in events:
        by_table.setdefault(e.table, []).append(e)
    return by_table


def _apply_batch(events: list[ChangeEvent]) -> None:
    by_table = _split_by_table(events)
    customer_events = by_table.get("customers", [])
    product_events = by_table.get("products", [])
    order_events = by_table.get("orders", [])
    item_events = by_table.get("order_items", [])

    with transaction() as conn:
        # 3. staging
        staging.apply_customers(conn, customer_events)
        staging.apply_products(conn, product_events)
        staging.apply_orders(conn, order_events)
        staging.apply_order_items(conn, item_events)

        # 4. dims (SCD2)
        dims.apply_dim_customer(conn, customer_events)
        dims.apply_dim_product(conn, product_events)

        # 5. fact_orders + status_events + items denorm
        facts_orders.apply_fact_orders(conn, order_events)

        # 6. fact_order_items
        facts_items.apply_fact_order_items(conn, item_events)

    # Freshness gauge: now - max(source_ts_ms) of the batch.
    if events:
        max_ts_ms = max(e.source_ts_ms for e in events if e.source_ts_ms)
        if max_ts_ms:
            now_s = datetime.now(tz=timezone.utc).timestamp()
            freshness_seconds.set(max(0.0, now_s - max_ts_ms / 1000.0))


def main() -> None:
    configure_logging(settings.log_level)
    log.info("starting consumer", topics=settings.kafka_topics)
    start_metrics_server(settings.metrics_port)
    init_pool(settings.pg_dsn)

    consumer = build_consumer()
    consumer.subscribe(settings.kafka_topics)

    stopping = {"flag": False}

    def _stop(signum, frame):
        log.info("shutdown signal received", signal=signum)
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        while not stopping["flag"]:
            events, raw = poll_batch(consumer)
            if not events and not raw:
                time.sleep(0.05)
                continue
            for m in raw:
                messages_consumed_total.labels(topic=m.topic()).inc()
            t0 = time.monotonic()
            try:
                _apply_batch(events)
            except Exception as exc:
                apply_errors_total.labels(kind=type(exc).__name__).inc()
                log.exception("apply failed; will retry batch", error=str(exc))
                time.sleep(1.0)
                continue
            apply_batch_duration_seconds.observe(time.monotonic() - t0)

            commit_offsets(consumer, raw)
            log.info(
                "applied batch",
                events=len(events),
                tables=sorted({e.table for e in events}),
            )
    finally:
        log.info("closing consumer")
        try:
            consumer.close()
        finally:
            close_pool()


if __name__ == "__main__":
    main()
