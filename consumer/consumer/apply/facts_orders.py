"""Maintain marts.fact_orders (current-state) + fact_order_status_events
(append-only) + denormalized current_order_status on fact_order_items.

The full apply loop runs inside the same DB transaction the consumer is
already holding (ADR-0003 § How the apply loop populates this model).
"""

from typing import Iterable

import psycopg

from ..debezium import ChangeEvent, row
from ..temporal import to_date, to_ts


def _date_key(d) -> int | None:
    d = to_date(d)
    return None if d is None else d.year * 10000 + d.month * 100 + d.day


def apply_fact_orders(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> None:
    """Process orders events. Maintain fact_orders + status events + items denorm."""
    events = list(events)
    if not events:
        return

    with conn.cursor() as cur:
        for e in events:
            r = row(e)
            order_id = r.get("order_id")
            if order_id is None:
                continue

            if e.op == "d":
                cur.execute(
                    "UPDATE marts.fact_orders SET is_deleted = TRUE, deleted_at = NOW() "
                    "WHERE order_id = %s",
                    (order_id,),
                )
                cur.execute(
                    "UPDATE marts.fact_order_items SET is_deleted = TRUE, deleted_at = NOW() "
                    "WHERE order_id = %s",
                    (order_id,),
                )
                continue

            customer_id = r.get("customer_id")
            cur.execute(
                "SELECT customer_sk FROM marts.dim_customer "
                "WHERE customer_id = %s AND is_current = TRUE",
                (customer_id,),
            )
            row_sk = cur.fetchone()
            if row_sk is None:
                # Out-of-order: customer event hasn't been applied yet.
                # Insert a placeholder dim row so we can set the FK; the
                # later CDC event will re-version it.
                cur.execute(
                    """
                    INSERT INTO marts.dim_customer
                        (customer_id, customer_name, customer_address, is_active,
                         valid_from, valid_to, is_current, row_hash)
                    VALUES (%s, '__placeholder__', NULL, TRUE, NOW(), NULL, TRUE,
                            decode('00','hex'))
                    RETURNING customer_sk
                    """,
                    (customer_id,),
                )
                row_sk = cur.fetchone()
            customer_sk = row_sk[0]

            order_date = to_date(r.get("order_date"))
            delivery_date = to_date(r.get("delivery_date"))
            order_date_key = _date_key(order_date)
            delivery_date_key = _date_key(delivery_date)
            current_status = r.get("status") or "UNKNOWN"
            updated_at_ts = to_ts(r.get("updated_at"))
            event_ts = updated_at_ts or to_ts(e.source_ts_ms)

            # UPSERT fact_orders + detect status transition.
            cur.execute(
                """
                SELECT current_status FROM marts.fact_orders WHERE order_id = %s
                """,
                (order_id,),
            )
            existing = cur.fetchone()
            prior_status = existing[0] if existing else None

            cur.execute(
                """
                INSERT INTO marts.fact_orders (
                    order_id, customer_sk, order_date_key, delivery_date_key,
                    order_date, delivery_date,
                    current_status, current_status_at, last_updated_at,
                    is_deleted, deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, NULL)
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_sk = EXCLUDED.customer_sk,
                    order_date_key = EXCLUDED.order_date_key,
                    delivery_date_key = EXCLUDED.delivery_date_key,
                    order_date = EXCLUDED.order_date,
                    delivery_date = EXCLUDED.delivery_date,
                    current_status = EXCLUDED.current_status,
                    current_status_at = CASE
                        WHEN marts.fact_orders.current_status IS DISTINCT FROM EXCLUDED.current_status
                        THEN EXCLUDED.current_status_at
                        ELSE marts.fact_orders.current_status_at
                    END,
                    last_updated_at = EXCLUDED.last_updated_at,
                    is_deleted = FALSE,
                    deleted_at = NULL
                """,
                (
                    order_id, customer_sk, order_date_key, delivery_date_key,
                    order_date, delivery_date,
                    current_status, event_ts, event_ts,
                ),
            )

            if prior_status != current_status:
                cur.execute(
                    """
                    INSERT INTO marts.fact_order_status_events
                        (order_id, status, status_changed_at, event_lsn)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (order_id, status_changed_at, status) DO NOTHING
                    """,
                    (order_id, current_status, event_ts, e.source_lsn),
                )
                # Keep denormalized status on items in sync.
                cur.execute(
                    "UPDATE marts.fact_order_items SET current_order_status = %s "
                    "WHERE order_id = %s",
                    (current_status, order_id),
                )
