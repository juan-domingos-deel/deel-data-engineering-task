from datetime import date, datetime, timezone

from consumer.temporal import to_date, to_ts


def test_to_date_int_days_since_epoch():
    # 2024-01-01 = 19723 days since 1970-01-01.
    assert to_date(19723) == date(2024, 1, 1)


def test_to_date_iso_string():
    assert to_date("2024-05-09") == date(2024, 5, 9)


def test_to_ts_ms_since_epoch():
    # 1715000000000 ms = 2024-05-06T15:33:20Z
    assert to_ts(1715000000000) == datetime.fromtimestamp(1715000000, tz=timezone.utc)


def test_to_ts_none():
    assert to_ts(None) is None
