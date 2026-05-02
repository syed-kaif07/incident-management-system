from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── Infrastructure ────────────────────────────────────────────────────────
    redis_url: str = "redis://redis:6379/0"
    mongo_url: str = "mongodb://mongo:27017"
    mongo_db: str = "ims"
    postgres_dsn: str = "postgresql+asyncpg://ims:ims@postgres:5432/ims"

    # ── Stream / consumer group ───────────────────────────────────────────────
    signal_stream: str = "ims:signals"
    signal_group: str = "ims-workers"
    max_stream_len: int = 2_000_000        # MAXLEN cap on Redis Stream (approximate)
    backpressure_threshold: int = 1_800_000  # return 503 when stream depth exceeds this

    # ── Dashboard cache ───────────────────────────────────────────────────────
    dashboard_cache_key: str = "ims:dashboard:active"

    # ── Debounce ──────────────────────────────────────────────────────────────
    debounce_seconds: int = 10

    # ── Rate limiting (per client IP, not total system throughput) ────────────
    rate_limit_per_second: int = 500

    # ── Worker tuning ─────────────────────────────────────────────────────────
    worker_batch_size: int = 100           # messages per xreadgroup call
    worker_poll_timeout_ms: int = 1_000    # block timeout for xreadgroup
    worker_concurrency: int = 20           # max concurrent process_message coroutines
    pel_claim_idle_ms: int = 30_000        # claim PEL messages idle longer than 30s
    pel_claim_batch: int = 50              # messages to reclaim per sweep

    # ── Retry (DB writes) ─────────────────────────────────────────────────────
    retry_max_attempts: int = 5
    retry_min_wait_ms: float = 100         # tenacity wait_exponential min (seconds → converted below)
    retry_max_wait_ms: float = 2_000

    # ── CORS ──────────────────────────────────────────────────────────────────
    api_cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # ── Derived helpers ───────────────────────────────────────────────────────
    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.api_cors_origins.split(",") if o.strip()]

    @property
    def retry_min_wait_s(self) -> float:
        return self.retry_min_wait_ms / 1000

    @property
    def retry_max_wait_s(self) -> float:
        return self.retry_max_wait_ms / 1000


@lru_cache
def get_settings() -> Settings:
    return Settings()