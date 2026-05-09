"""SCD2 close-out + insert for dim_customer / dim_product.

Hash-diff strategy: compute a stable hash over the SCD2-tracked attributes
(per ADR-0003 § Sub-decision A). If the hash matches the current version, do
nothing. Otherwise close the current version (set valid_to, is_current=false)
and insert a new version (is_current=true).
"""

import hashlib
from datetime import datetime, timezone
from typing import Iterable

import psycopg

from ..debezium import ChangeEvent, row
from ..metrics import dim_scd2_versions_written_total
from ..temporal import to_decimal

CUSTOMER_TRACKED = ("customer_name", "customer_address", "is_active")
PRODUCT_TRACKED = ("product_name", "barcode", "unity_price", "is_active")


def _hash(values: tuple) -> bytes:
    h = hashlib.sha256()
    for v in values:
        h.update(b"\x00")
        h.update(repr(v).encode("utf-8"))
    return h.digest()


def _upsert_scd2(
    conn: psycopg.Connection,
    *,
    table: str,
    nk_col: str,
    sk_col: str,
    tracked_cols: tuple[str, ...],
    rows: list[tuple],
) -> int:
    """rows: (natural_key, *tracked_values, hash_bytes, valid_from)."""
    if not rows:
        return 0

    written = 0
    with conn.cursor() as cur:
        for r in rows:
            nk = r[0]
            tracked = r[1 : 1 + len(tracked_cols)]
            row_hash = r[1 + len(tracked_cols)]
            valid_from = r[2 + len(tracked_cols)]

            cur.execute(
                f"SELECT {sk_col}, row_hash FROM {table} "
                f"WHERE {nk_col} = %s AND is_current = TRUE FOR UPDATE",
                (nk,),
            )
            existing = cur.fetchone()
            if existing is not None and bytes(existing[1]) == row_hash:
                continue  # no change

            if existing is not None:
                cur.execute(
                    f"UPDATE {table} SET valid_to = %s, is_current = FALSE "
                    f"WHERE {sk_col} = %s",
                    (valid_from, existing[0]),
                )

            cols = (
                f"{nk_col}, " + ", ".join(tracked_cols)
                + ", valid_from, valid_to, is_current, row_hash"
            )
            placeholders = ", ".join(["%s"] * (len(tracked_cols) + 1)) + ", NULL, TRUE, %s"
            cur.execute(
                f"INSERT INTO {table} ({cols}) VALUES (%s, {placeholders})",
                (nk, *tracked, valid_from, row_hash),
            )
            written += 1
    return written


def _now_or_event_ts(source_ts_ms: int) -> datetime:
    return datetime.fromtimestamp(source_ts_ms / 1000.0, tz=timezone.utc)


def apply_dim_customer(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> None:
    rows: list[tuple] = []
    seen: set[int] = set()
    for e in reversed(list(events)):
        # Take only the latest event per customer_id within this batch.
        r = row(e)
        cid = r.get("customer_id")
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        if e.op == "d":
            # Soft-delete on dims is not modelled (history preserved on facts).
            continue
        tracked = (
            r.get("customer_name"),
            r.get("customer_address"),
            bool(r.get("is_active")) if r.get("is_active") is not None else True,
        )
        rows.append((cid, *tracked, _hash(tracked), _now_or_event_ts(e.source_ts_ms)))
    written = _upsert_scd2(
        conn,
        table="marts.dim_customer",
        nk_col="customer_id",
        sk_col="customer_sk",
        tracked_cols=CUSTOMER_TRACKED,
        rows=rows,
    )
    if written:
        dim_scd2_versions_written_total.labels(dimension="customer").inc(written)


def apply_dim_product(conn: psycopg.Connection, events: Iterable[ChangeEvent]) -> None:
    rows: list[tuple] = []
    seen: set[int] = set()
    for e in reversed(list(events)):
        r = row(e)
        pid = r.get("product_id")
        if pid is None or pid in seen:
            continue
        seen.add(pid)
        if e.op == "d":
            continue
        tracked = (
            r.get("product_name"),
            r.get("barcode"),
            to_decimal(r.get("unity_price")),
            r.get("is_active"),
        )
        rows.append((pid, *tracked, _hash(tracked), _now_or_event_ts(e.source_ts_ms)))
    written = _upsert_scd2(
        conn,
        table="marts.dim_product",
        nk_col="product_id",
        sk_col="product_sk",
        tracked_cols=PRODUCT_TRACKED,
        rows=rows,
    )
    if written:
        dim_scd2_versions_written_total.labels(dimension="product").inc(written)
