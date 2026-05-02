from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis, ConnectionPool
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ims.config import get_settings
from ims.sql_models import Base

settings = get_settings()

# ── PostgreSQL ────────────────────────────────────────────────────────────────
# pool_pre_ping=True  → validates connections before use (detects stale sockets)
# pool_size=10        → steady-state connections kept alive
# max_overflow=20     → burst connections allowed beyond pool_size (total max=30)
# pool_timeout=30     → seconds to wait for a connection before raising
engine = create_async_engine(
    settings.postgres_dsn,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
)
SessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

# ── Redis ─────────────────────────────────────────────────────────────────────
# Explicit ConnectionPool so max_connections is enforced.
# Without this, redis-py defaults to 10 connections which saturates
# immediately under worker concurrency of 20+.
_redis_pool = ConnectionPool.from_url(
    settings.redis_url,
    decode_responses=True,
    max_connections=100,
)
redis_client = Redis(connection_pool=_redis_pool)

# ── MongoDB ───────────────────────────────────────────────────────────────────
# maxPoolSize=50            → connection pool cap (default is 100, explicit is better)
# serverSelectionTimeoutMS  → fail fast if Mongo is unreachable (3s)
# connectTimeoutMS          → fail fast on initial TCP connect (3s)
# Without these, Motor will queue operations silently and appear hung
# when Mongo is slow or down.
mongo_client = AsyncIOMotorClient(
    settings.mongo_url,
    maxPoolSize=50,
    serverSelectionTimeoutMS=3_000,
    connectTimeoutMS=3_000,
)
mongo_db = mongo_client[settings.mongo_db]


# ── Startup helpers ───────────────────────────────────────────────────────────

async def init_postgres() -> None:
    """
    Creates tables if they do not exist.
    Safe to call on every startup in development.
    In production, use Alembic migrations instead and guard this behind
    an environment flag (e.g. RUN_MIGRATIONS=false).
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def check_postgres() -> bool:
    """Ping Postgres. Returns False instead of raising — used in /health."""
    try:
        async with engine.connect() as conn:
            await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        return True
    except Exception:
        return False


async def check_redis() -> bool:
    """Ping Redis. Returns False instead of raising — used in /health."""
    try:
        await redis_client.ping()
        return True
    except Exception:
        return False


async def check_mongo() -> bool:
    """Ping Mongo. Returns False instead of raising — used in /health."""
    try:
        await mongo_db.command("ping")
        return True
    except Exception:
        return False


# ── Graceful shutdown ─────────────────────────────────────────────────────────

async def close_connections() -> None:
    """
    Release all connections cleanly on shutdown.
    Called from the FastAPI lifespan context or worker shutdown hook.
    Without this, containers restart with lingering TCP connections
    which Postgres/Redis treat as client crashes (filling error logs).
    """
    await redis_client.aclose()
    await engine.dispose()
    mongo_client.close()


# ── FastAPI lifespan (replaces deprecated @app.on_event) ─────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Use in FastAPI app construction:
        app = FastAPI(lifespan=lifespan)

    Handles startup (DB init) and shutdown (connection cleanup).
    """
    await init_postgres()
    yield
    await close_connections()


# ── Session dependency ────────────────────────────────────────────────────────

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session