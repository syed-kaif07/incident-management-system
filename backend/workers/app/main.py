import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
from uuid import UUID

from redis.exceptions import ResponseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ims.alerting import map_signal_severity
from ims.config import get_settings
from ims.dashboard import upsert_active_incident
from ims.database import SessionLocal, init_postgres, mongo_db, redis_client
from ims.schemas import SignalIn
from ims.sql_models import WorkItem

settings = get_settings()
logger = logging.getLogger("ims.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

consumer_name = os.getenv("WORKER_NAME", f"worker-{os.getpid()}")
stop_event = asyncio.Event()
metrics = {"processed": 0, "errors": 0, "db_retries": 0}


def _handle_stop(*_: object) -> None:
    stop_event.set()


def _decode_signal(message: dict[str, str]) -> SignalIn:
    return SignalIn(
        component_id=message["component_id"],
        timestamp=datetime.fromisoformat(message["timestamp"]),
        severity=message["severity"],
        payload=json.loads(message.get("payload") or "{}"),
    )


async def ensure_stream_group() -> None:
    try:
        await redis_client.xgroup_create(settings.signal_stream, settings.signal_group, id="0", mkstream=True)
        logger.info("created_consumer_group=%s stream=%s", settings.signal_group, settings.signal_stream)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def ensure_indexes() -> None:
    await mongo_db.raw_signals.create_index([("work_item_id", 1), ("timestamp", 1)])
    await mongo_db.raw_signals.create_index([("component_id", 1), ("timestamp", 1)])


async def find_recent_work_item(session: AsyncSession, component_id: str, now: datetime) -> WorkItem | None:
    lower_bound = now.timestamp() - settings.debounce_seconds
    result = await session.execute(
        select(WorkItem)
        .where(WorkItem.component_id == component_id)
        .where(WorkItem.start_time >= datetime.fromtimestamp(lower_bound, tz=timezone.utc))
        .order_by(WorkItem.start_time.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=0.1, min=0.1, max=2),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def create_work_item(signal_in: SignalIn, severity: str) -> WorkItem:
    try:
        async with SessionLocal() as session:
            item = WorkItem(
                component_id=signal_in.component_id,
                severity=severity,
                status="OPEN",
                start_time=signal_in.timestamp,
            )
            session.add(item)
            await session.commit()
            await session.refresh(item)
            return item
    except Exception:
        metrics["db_retries"] += 1
        raise


async def get_or_create_work_item(signal_in: SignalIn) -> UUID:
    debounce_key = f"debounce:component:{signal_in.component_id}"
    existing = await redis_client.get(debounce_key)
    if existing:
        return UUID(existing)

    lock_key = f"lock:{debounce_key}"
    lock_acquired = await redis_client.set(lock_key, consumer_name, nx=True, ex=settings.debounce_seconds)
    if not lock_acquired:
        for _ in range(20):
            await asyncio.sleep(0.05)
            existing = await redis_client.get(debounce_key)
            if existing:
                return UUID(existing)
        lock_acquired = await redis_client.set(lock_key, consumer_name, nx=True, ex=settings.debounce_seconds)
        if not lock_acquired:
            raise RuntimeError(f"debounce creator did not publish work item for {signal_in.component_id}")

    async with SessionLocal() as session:
        try:
            existing_item = await find_recent_work_item(session, signal_in.component_id, signal_in.timestamp)
            if existing_item:
                await redis_client.set(debounce_key, str(existing_item.id), ex=settings.debounce_seconds)
                return existing_item.id

            item = await create_work_item(signal_in, map_signal_severity(signal_in))
            await redis_client.set(debounce_key, str(item.id), ex=settings.debounce_seconds)
            await upsert_active_incident(redis_client, item)
            logger.info("created_work_item=%s component=%s severity=%s", item.id, item.component_id, item.severity)
            return item.id
        finally:
            await redis_client.delete(lock_key)


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=0.1, min=0.1, max=2),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def store_raw_signal(signal_in: SignalIn, work_item_id: UUID) -> None:
    await mongo_db.raw_signals.insert_one(
        {
            "work_item_id": str(work_item_id),
            "component_id": signal_in.component_id,
            "timestamp": signal_in.timestamp,
            "severity": signal_in.severity,
            "payload": signal_in.payload,
            "stored_at": datetime.now(timezone.utc),
        }
    )


async def process_message(message_id: str, fields: dict[str, str]) -> None:
    try:
        signal_in = _decode_signal(fields)
        work_item_id = await get_or_create_work_item(signal_in)
        await store_raw_signal(signal_in, work_item_id)
        await redis_client.xack(settings.signal_stream, settings.signal_group, message_id)
        metrics["processed"] += 1
    except Exception:
        metrics["errors"] += 1
        logger.exception("failed_message=%s", message_id)
        raise


async def log_metrics() -> None:
    last_count = 0
    while not stop_event.is_set():
        await asyncio.sleep(5)
        processed = metrics["processed"]
        delta = processed - last_count
        last_count = processed
        pending = await redis_client.xpending_range(settings.signal_stream, settings.signal_group, "-", "+", 10)
        logger.info(
            "worker=%s signals_per_sec=%.2f processed=%s errors=%s db_retries=%s pending_sample=%s",
            consumer_name,
            delta / 5,
            processed,
            metrics["errors"],
            metrics["db_retries"],
            len(pending),
        )


async def consume_forever() -> None:
    await init_postgres()
    await ensure_indexes()
    await ensure_stream_group()
    asyncio.create_task(log_metrics())
    logger.info("worker_started=%s group=%s", consumer_name, settings.signal_group)

    while not stop_event.is_set():
        response = await redis_client.xreadgroup(
            settings.signal_group,
            consumer_name,
            streams={settings.signal_stream: ">"},
            count=200,
            block=1000,
        )
        for _, messages in response:
            await asyncio.gather(*(process_message(message_id, fields) for message_id, fields in messages))


def main() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_stop)
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()
