from consumer.debezium import parse, row


def _envelope(op="u", before=None, after=None, ts_ms=1715500000000, lsn=42, table="orders"):
    return {
        "before": before,
        "after": after,
        "source": {"ts_ms": ts_ms, "lsn": lsn, "table": table, "schema": "operations"},
        "op": op,
        "ts_ms": ts_ms,
    }


def test_parse_create():
    e = parse(_envelope(op="c", after={"order_id": 1}), topic="t", partition=0, offset=10)
    assert e is not None
    assert e.op == "c"
    assert e.table == "orders"
    assert e.source_ts_ms == 1715500000000
    assert row(e) == {"order_id": 1}


def test_parse_delete_uses_before():
    e = parse(_envelope(op="d", before={"order_id": 1}), topic="t", partition=0, offset=10)
    assert e is not None
    assert e.op == "d"
    assert row(e) == {"order_id": 1}


def test_tombstone_returns_none():
    assert parse(None, topic="t", partition=0, offset=0) is None


def test_unknown_op_skipped():
    e = parse(_envelope(op="t"), topic="t", partition=0, offset=0)
    assert e is None
