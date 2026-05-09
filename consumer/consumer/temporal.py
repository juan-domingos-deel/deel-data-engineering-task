"""Convert Debezium's default temporal and decimal encodings to Python types.

Default Debezium for `time.precision.mode=adaptive` (the default):
- DATE → io.debezium.time.Date           → INT days since 1970-01-01
- TIMESTAMP(0..3) → io.debezium.time.Timestamp     → INT64 ms since epoch
- TIMESTAMP(4..6) → io.debezium.time.MicroTimestamp → INT64 us since epoch
- TIMESTAMP(7..9) → io.debezium.time.NanoTimestamp  → INT64 ns since epoch

We use the heuristic that DATE columns receive small ints (< 10**6) and
TIMESTAMP columns receive ms-since-epoch ints (>= 10**11 for any plausible
value past 1973). The source DDL uses TIMESTAMP(3) which maps to ms.
"""

import base64
from datetime import date, datetime, timezone
from decimal import Decimal


def to_date(v: int | str | date | None) -> date | None:
    if v is None or isinstance(v, date) and not isinstance(v, datetime):
        return v if not isinstance(v, datetime) else v.date()
    if isinstance(v, str):
        return date.fromisoformat(v)
    if isinstance(v, int):
        return date.fromordinal(date(1970, 1, 1).toordinal() + v)
    return None


def to_ts(v: int | str | datetime | None) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        # ISO-8601 with optional Z
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    if isinstance(v, int):
        # ms since epoch (TIMESTAMP(3) on the source).
        return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
    return None


def to_decimal(v) -> Decimal | float | None:
    """Decode a Debezium decimal payload to a Python value.

    With `decimal.handling.mode=double` the connector sends a JSON number;
    with `precise` (default) it sends either a base64 string (for fixed-
    precision NUMERIC) or a `{"scale": N, "value": "<base64>"}` struct
    (for VariableScaleDecimal - i.e. NUMERIC without precision, like our
    `unity_price DECIMAL`). This handles all three.
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, Decimal):
        return v
    if isinstance(v, str):
        # Could be either a numeric literal ("12.34") or a base64-encoded
        # unscaled-int (precise mode for fixed-precision NUMERIC).
        try:
            return Decimal(v)
        except Exception:
            raw = base64.b64decode(v)
            return Decimal(int.from_bytes(raw, byteorder="big", signed=True))
    if isinstance(v, dict) and "value" in v:
        scale = int(v.get("scale", 0))
        raw = base64.b64decode(v["value"])
        unscaled = int.from_bytes(raw, byteorder="big", signed=True)
        return Decimal(unscaled).scaleb(-scale)
    return None
