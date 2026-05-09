"""Parse Debezium JSON envelopes into typed change events.

The connector is configured with `value.converter.schemas.enable=false`, so the
payload is the bare envelope. See finance-db-connector.json.

Envelope shape (after-only excerpt):
{
  "before": null | {...},
  "after":  null | {...},
  "source": {"ts_ms": 1234, "lsn": 5678, "table": "...", "schema": "operations"},
  "op":     "c" | "u" | "d" | "r" | "t",
  "ts_ms":  1234
}
"""

from dataclasses import dataclass
from typing import Any, Literal

Op = Literal["c", "u", "d", "r"]
"""c=create, u=update, d=delete, r=read (snapshot). 't' truncate is ignored."""


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    op: Op
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source_ts_ms: int
    source_lsn: int | None
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int


def parse(value: dict[str, Any] | None, *, topic: str, partition: int, offset: int) -> ChangeEvent | None:
    """Return a ChangeEvent or None if the message is a tombstone or unsupported op."""
    if value is None:
        return None  # tombstone (post-delete) - handled by the prior 'd' event
    op = value.get("op")
    if op not in ("c", "u", "d", "r"):
        return None
    source = value.get("source") or {}
    table = source.get("table") or ""
    return ChangeEvent(
        table=table,
        op=op,
        before=value.get("before"),
        after=value.get("after"),
        source_ts_ms=int(source.get("ts_ms") or value.get("ts_ms") or 0),
        source_lsn=source.get("lsn"),
        kafka_topic=topic,
        kafka_partition=partition,
        kafka_offset=offset,
    )


def row(event: ChangeEvent) -> dict[str, Any]:
    """Authoritative row state implied by the event.

    For c/u/r: 'after'. For d: 'before' (the row that was deleted).
    """
    if event.op == "d":
        return event.before or {}
    return event.after or {}
