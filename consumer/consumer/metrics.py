from prometheus_client import Counter, Gauge, Histogram, start_http_server

# RED-style consumer metrics + freshness SLO from ADR-0005.
messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Kafka messages consumed by topic.",
    ["topic"],
)
apply_batch_duration_seconds = Histogram(
    "apply_batch_duration_seconds",
    "Wall-clock seconds to apply one Kafka batch (parse + DB tx).",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
apply_errors_total = Counter(
    "apply_errors_total",
    "Errors during apply, by kind.",
    ["kind"],
)
dim_scd2_versions_written_total = Counter(
    "dim_scd2_versions_written_total",
    "New SCD2 versions written, by dimension.",
    ["dimension"],
)
freshness_seconds = Gauge(
    "freshness_seconds",
    "now() − source_commit_ts at the moment a batch was committed.",
)


def start_metrics_server(port: int) -> None:
    """Expose /metrics on a side thread; idempotent."""
    start_http_server(port)
