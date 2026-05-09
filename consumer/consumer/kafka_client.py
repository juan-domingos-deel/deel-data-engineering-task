"""confluent-kafka consumer with manual offset commits.

Per ADR-0002: at-least-once with idempotent apply. Offsets are committed
*after* the analytical-DB transaction succeeds, so a crash mid-apply re-delivers
the batch and the UPSERT/SCD2 hash-diff logic absorbs the duplication.
"""

import json
from typing import Iterator

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

from .config import settings
from .debezium import ChangeEvent, parse


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            # Allow long apply transactions without rebalance churn.
            "max.poll.interval.ms": 600000,
            "session.timeout.ms": 60000,
        }
    )


def poll_batch(consumer: Consumer) -> tuple[list[ChangeEvent], list[Message]]:
    """Drain up to `kafka_batch_max_messages` events.

    Returns (parsed events, raw messages). Raw messages are kept so we can
    commit their offsets after the DB transaction lands.
    """
    events: list[ChangeEvent] = []
    raw: list[Message] = []
    deadline_calls = settings.kafka_batch_max_messages
    while deadline_calls > 0:
        msg = consumer.poll(timeout=settings.kafka_poll_timeout_seconds if not raw else 0.0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(f"kafka error: {msg.error()}")
        raw.append(msg)
        deadline_calls -= 1
        payload = msg.value()
        if payload is None:
            continue  # tombstone - Debezium emits these after deletes
        try:
            value = json.loads(payload)
        except (TypeError, ValueError):
            continue
        evt = parse(
            value,
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )
        if evt is not None:
            events.append(evt)
    return events, raw


def commit_offsets(consumer: Consumer, raw: list[Message]) -> None:
    """Commit the highest offset per (topic, partition) seen in the batch."""
    if not raw:
        return
    # Highest offset wins per partition.
    high: dict[tuple[str, int], int] = {}
    for m in raw:
        key = (m.topic(), m.partition())
        if m.offset() > high.get(key, -1):
            high[key] = m.offset()
    tps = [
        TopicPartition(topic=t, partition=p, offset=o + 1)  # next-to-read
        for (t, p), o in high.items()
    ]
    consumer.commit(offsets=tps, asynchronous=False)
