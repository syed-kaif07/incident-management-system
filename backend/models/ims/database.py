from collections.abc import AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ims.config import get_settings
from ims.sql_models import Base

settings = get_settings()

engine = create_async_engine(settings.postgres_dsn, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
mongo_client = AsyncIOMotorClient(settings.mongo_url)
mongo_db = mongo_client[settings.mongo_db]


async def init_postgres() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session
