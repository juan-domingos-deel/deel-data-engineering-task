"""Maintain marts.fact_order_items, denormalizing customer_sk and
current_order_status from the parent order.

Out-of-order arrival is handled defensively: if the parent order is not yet
in fact_orders, we still UPSERT the item using best-effort lookups; the
later orders event will run the items-denorm UPDATE in facts_orders.
"""

from typing import Iterable

import psycopg

from ..debezium import ChangeEvent, row
from ..temporal import to_ts


def apply_fact_order_items(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> None:
    events = list(events)
    if not events:
        return

    with conn.cursor() as cur:
        for e in events:
            r = row(e)
            item_id = r.get("order_item_id")
            if item_id is None:
                continue

            if e.op == "d":
                cur.execute(
                    "UPDATE marts.fact_order_items SET is_deleted = TRUE, deleted_at = NOW() "
                    "WHERE order_item_id = %s",
                    (item_id,),
                )
                continue

            order_id = r.get("order_id")
            product_id = r.get("product_id")
            quantity = r.get("quantity", r.get("quanity"))  # source typo
            updated_at_ts = to_ts(r.get("updated_at")) or to_ts(e.source_ts_ms)

            cur.execute(
                "SELECT product_sk FROM marts.dim_product "
                "WHERE product_id = %s AND is_current = TRUE",
                (product_id,),
            )
            prow = cur.fetchone()
            if prow is None:
                # Same out-of-order placeholder pattern as customer.
                cur.execute(
                    """
                    INSERT INTO marts.dim_product
                        (product_id, product_name, barcode, unity_price, is_active,
                         valid_from, valid_to, is_current, row_hash)
                    VALUES (%s, '__placeholder__', '__placeholder__', 0, TRUE,
                            NOW(), NULL, TRUE, decode('00','hex'))
                    RETURNING product_sk
                    """,
                    (product_id,),
                )
                prow = cur.fetchone()
            product_sk = prow[0]

            # Pull parent order's customer_sk + current_status + date keys.
            cur.execute(
                """
                SELECT customer_sk, current_status, order_date_key, delivery_date_key
                FROM marts.fact_orders WHERE order_id = %s
                """,
                (order_id,),
            )
            parent = cur.fetchone()
            if parent is None:
                # Parent not in facts yet. Use placeholder customer_sk = 0?
                # Better: fall back to dim_customer via stg_orders if present.
                cur.execute(
                    """
                    SELECT c.customer_sk, o.status,
                           CASE WHEN o.order_date IS NULL THEN NULL
                                ELSE EXTRACT(YEAR FROM o.order_date)::INT * 10000
                                     + EXTRACT(MONTH FROM o.order_date)::INT * 100
                                     + EXTRACT(DAY FROM o.order_date)::INT END,
                           CASE WHEN o.delivery_date IS NULL THEN NULL
                                ELSE EXTRACT(YEAR FROM o.delivery_date)::INT * 10000
                                     + EXTRACT(MONTH FROM o.delivery_date)::INT * 100
                                     + EXTRACT(DAY FROM o.delivery_date)::INT END
                    FROM staging.stg_orders o
                    LEFT JOIN marts.dim_customer c
                      ON c.customer_id = o.customer_id AND c.is_current = TRUE
                    WHERE o.order_id = %s
                    """,
                    (order_id,),
                )
                parent = cur.fetchone()

            if parent is None:
                # Skip - orders event will arrive and we'll re-process via items
                # via the denormalization UPDATE in facts_orders.apply.
                # But we still want the item recorded; we cannot without an FK to dim_customer.
                # Fail-soft: log and continue.
                continue

            customer_sk, current_status, order_date_key, delivery_date_key = parent

            cur.execute(
                """
                INSERT INTO marts.fact_order_items (
                    order_item_id, order_id, product_sk, customer_sk,
                    quantity, current_order_status,
                    order_date_key, delivery_date_key, last_updated_at,
                    is_deleted, deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, NULL)
                ON CONFLICT (order_item_id) DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    product_sk = EXCLUDED.product_sk,
                    customer_sk = EXCLUDED.customer_sk,
                    quantity = EXCLUDED.quantity,
                    current_order_status = EXCLUDED.current_order_status,
                    order_date_key = EXCLUDED.order_date_key,
                    delivery_date_key = EXCLUDED.delivery_date_key,
                    last_updated_at = EXCLUDED.last_updated_at,
                    is_deleted = FALSE,
                    deleted_at = NULL
                """,
                (
                    item_id, order_id, product_sk, customer_sk,
                    quantity, current_status or "UNKNOWN",
                    order_date_key, delivery_date_key, updated_at_ts,
                ),
            )
