"""UPSERT change events into staging.stg_*.

Source-deletes (op=d) are encoded by setting is_deleted=true on the row;
the marts layer then reads is_deleted to soft-delete the corresponding facts.
"""

from typing import Iterable

import psycopg

from ..debezium import ChangeEvent, row
from ..temporal import to_date, to_decimal, to_ts

# Per-table column orderings used by INSERT statements below.
_CUSTOMER_COLS = (
    "customer_id", "customer_name", "is_active", "customer_address",
    "updated_at", "updated_by", "created_at", "created_by",
    "source_ts_ms", "is_deleted",
)
_PRODUCT_COLS = (
    "product_id", "product_name", "barcode", "unity_price", "is_active",
    "updated_at", "updated_by", "created_at", "created_by",
    "source_ts_ms", "is_deleted",
)
_ORDER_COLS = (
    "order_id", "order_date", "delivery_date", "customer_id", "status",
    "updated_at", "updated_by", "created_at", "created_by",
    "source_ts_ms", "source_lsn", "is_deleted",
)
_ORDER_ITEM_COLS = (
    "order_item_id", "order_id", "product_id", "quantity",
    "updated_at", "updated_by", "created_at", "created_by",
    "source_ts_ms", "is_deleted",
)


def _upsert(conn: psycopg.Connection, table: str, cols: tuple[str, ...], rows: list[tuple]) -> None:
    if not rows:
        return
    placeholders = "(" + ", ".join(["%s"] * len(cols)) + ")"
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != cols[0])
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES {placeholders} "
        f"ON CONFLICT ({cols[0]}) DO UPDATE SET {set_clause}"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def apply_customers(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> list[ChangeEvent]:
    rows = []
    kept: list[ChangeEvent] = []
    for e in events:
        r = row(e)
        if not r.get("customer_id"):
            continue
        rows.append(
            (
                r["customer_id"],
                r["customer_name"],
                r.get("is_active") if r.get("is_active") is not None else True,
                r.get("customer_address"),
                to_ts(r.get("updated_at")),
                r.get("updated_by"),
                to_ts(r.get("created_at")),
                r.get("created_by"),
                e.source_ts_ms,
                e.op == "d",
            )
        )
        kept.append(e)
    _upsert(conn, "staging.stg_customers", _CUSTOMER_COLS, rows)
    return kept


def apply_products(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> list[ChangeEvent]:
    rows = []
    kept: list[ChangeEvent] = []
    for e in events:
        r = row(e)
        if not r.get("product_id"):
            continue
        rows.append(
            (
                r["product_id"],
                r["product_name"],
                r["barcode"],
                to_decimal(r["unity_price"]),
                r.get("is_active"),
                to_ts(r.get("updated_at")),
                r.get("updated_by"),
                to_ts(r.get("created_at")),
                r.get("created_by"),
                e.source_ts_ms,
                e.op == "d",
            )
        )
        kept.append(e)
    _upsert(conn, "staging.stg_products", _PRODUCT_COLS, rows)
    return kept


def apply_orders(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> list[ChangeEvent]:
    rows = []
    kept: list[ChangeEvent] = []
    for e in events:
        r = row(e)
        if not r.get("order_id"):
            continue
        rows.append(
            (
                r["order_id"],
                to_date(r.get("order_date")),
                to_date(r.get("delivery_date")),
                r.get("customer_id"),
                r.get("status"),
                to_ts(r.get("updated_at")),
                r.get("updated_by"),
                to_ts(r.get("created_at")),
                r.get("created_by"),
                e.source_ts_ms,
                e.source_lsn,
                e.op == "d",
            )
        )
        kept.append(e)
    _upsert(conn, "staging.stg_orders", _ORDER_COLS, rows)
    return kept


def apply_order_items(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> list[ChangeEvent]:
    rows = []
    kept: list[ChangeEvent] = []
    for e in events:
        r = row(e)
        if not r.get("order_item_id"):
            continue
        # Source column is misspelled "quanity" - see initialize_db_ddl.sql.
        quantity = r.get("quantity", r.get("quanity"))
        rows.append(
            (
                r["order_item_id"],
                r.get("order_id"),
                r.get("product_id"),
                quantity,
                to_ts(r.get("updated_at")),
                r.get("updated_by"),
                to_ts(r.get("created_at")),
                r.get("created_by"),
                e.source_ts_ms,
                e.op == "d",
            )
        )
        kept.append(e)
    _upsert(conn, "staging.stg_order_items", _ORDER_ITEM_COLS, rows)
    return kept
