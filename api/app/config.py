from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", env_file=None)

    pg_host: str = "postgres-analytics"
    pg_port: int = 5432
    pg_db: str = "analytics_db"
    pg_user: str = "analytics_user"
    pg_password: str = "analytics_pw"

    pool_min_size: int = 2
    pool_max_size: int = 10

    log_level: str = "INFO"

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


settings = Settings()
