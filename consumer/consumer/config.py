from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CONSUMER_", env_file=None)

    # Kafka
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_group_id: str = "analytics-consumer"
    kafka_topics: list[str] = Field(
        default_factory=lambda: [
            "finance_db.operations.customers",
            "finance_db.operations.products",
            "finance_db.operations.orders",
            "finance_db.operations.order_items",
        ]
    )
    kafka_poll_timeout_seconds: float = 1.0
    kafka_batch_max_messages: int = 500

    # Postgres analytics
    pg_host: str = "postgres-analytics"
    pg_port: int = 5432
    pg_db: str = "analytics_db"
    pg_user: str = "analytics_user"
    pg_password: str = "analytics_pw"

    # Metrics HTTP
    metrics_port: int = 8001

    # Logging
    log_level: str = "INFO"

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


settings = Settings()
