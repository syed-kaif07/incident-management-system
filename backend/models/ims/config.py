from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    redis_url: str = "redis://redis:6379/0"
    mongo_url: str = "mongodb://mongo:27017"
    mongo_db: str = "ims"
    postgres_dsn: str = "postgresql+asyncpg://ims:ims@postgres:5432/ims"

    signal_stream: str = "ims:signals"
    signal_group: str = "ims-workers"
    dashboard_cache_key: str = "ims:dashboard:active"
    debounce_seconds: int = 10
    rate_limit_per_second: int = 15000

    api_cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.api_cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
