from datetime import datetime

from app.routers import envelope


def test_envelope_shape():
    payload = envelope([{"foo": 1}, {"foo": 2}])
    assert set(payload) == {"data", "meta"}
    assert payload["data"] == [{"foo": 1}, {"foo": 2}]
    assert payload["meta"]["count"] == 2
    # ISO-8601 string parses cleanly
    datetime.fromisoformat(payload["meta"]["generated_at"])


def test_envelope_empty():
    payload = envelope([])
    assert payload["data"] == []
    assert payload["meta"]["count"] == 0
