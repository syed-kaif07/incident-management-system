"""
Worker — Redis Streams Consumer
================================
Reads signals from Redis Streams using a consumer group.
All DB writes (Postgres + MongoDB) happen here — never in the ingest API.

Key behaviours:
  - Semaphore caps concurrent DB sessions to settings.worker_concurrency (default 20)
  - PEL recovery loop reclaims messages idle > pel_claim_idle_ms from dead workers
  - Dead-letter: messages that fail all retries are moved to ims:signals:dead
  - signal_count on WorkItem is incremented atomically per stored signal
  - xpending summary (O(1)) used for metrics, not xpending_range (O(N))
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
from uuid import UUID

from redis.exceptions import ResponseError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ims.alerting import map_signal_severity
from ims.config import get_settings
from ims.dashboard import upsert_active_incident
from ims.database import SessionLocal, init_postgres, mongo_db, redis_client
from ims.mttr import compute_mttr_safe
from ims.schemas import SignalIn
from ims.sql_models import WorkItem

settings = get_settings()
logger = logging.getLogger("ims.worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

consumer_name = os.getenv("WORKER_NAME", f"worker-{os.getpid()}")
stop_event = asyncio.Event()

# Semaphore: caps concurrent process_message coroutines so we never
# exceed the Postgres connection pool (pool_size=10, max_overflow=20 → max 30).
# worker_concurrency defaults to 20 — leaves headroom for API connections.
_sem: asyncio.Semaphore | None = None

DEAD_LETTER_STREAM = f"{settings.signal_stream}:dead"

metrics: dict[str, int] = {
    "processed": 0,
    "errors": 0,
    "db_retries": 0,
    "dead_lettered": 0,
}


# ── Signal handling ───────────────────────────────────────────────────────────

def _handle_stop(*_: object) -> None:
    logger.info("shutdown_signal_received worker=%s", consumer_name)
    stop_event.set()


# ── Startup ───────────────────────────────────────────────────────────────────

async def ensure_stream_group() -> None:
    try:
        await redis_client.xgroup_create(
            settings.signal_stream,
            settings.signal_group,
            id="0",
            mkstream=True,
        )
        logger.info(
            "created_consumer_group=%s stream=%s",
            settings.signal_group,
            settings.signal_stream,
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def ensure_indexes() -> None:
    await mongo_db.raw_signals.create_index([("work_item_id", 1), ("timestamp", 1)])
    await mongo_db.raw_signals.create_index([("component_id", 1), ("timestamp", 1)])


# ── Message decoding ──────────────────────────────────────────────────────────

def _decode_signal(message: dict[str, str]) -> SignalIn:
    return SignalIn(
        signal_id=message.get("signal_id", ""),
        component_id=message["component_id"],
        timestamp=datetime.fromisoformat(message["timestamp"]),
        severity=message["severity"],
        payload=json.loads(message.get("payload") or "{}"),
    )


# ── Debounce + WorkItem creation ──────────────────────────────────────────────

async def _find_recent_work_item(
    session: AsyncSession,
    component_id: str,
    now: datetime,
) -> WorkItem | None:
    lower_bound = now.timestamp() - settings.debounce_seconds
    result = await session.execute(
        select(WorkItem)
        .where(WorkItem.component_id == component_id)
        .where(
            WorkItem.start_time
            >= datetime.fromtimestamp(lower_bound, tz=timezone.utc)
        )
        .where(WorkItem.status.in_(["OPEN", "INVESTIGATING"]))
        .order_by(WorkItem.start_time.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(
        multiplier=0.1,
        min=settings.retry_min_wait_s,
        max=settings.retry_max_wait_s,
    ),
    stop=stop_after_attempt(settings.retry_max_attempts),
    reraise=True,
)
async def _create_work_item(signal_in: SignalIn, severity: str) -> WorkItem:
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
    """
    Debounce logic:
      1. Check Redis for existing debounce key → return cached work_item_id.
      2. Acquire distributed lock to prevent duplicate creation under concurrency.
      3. Re-check Redis (another worker may have created it while we waited).
      4. Query Postgres for a recent WorkItem within debounce_seconds.
      5. If none found, create a new WorkItem.
      6. Publish work_item_id to debounce key, release lock.

    All signals within the debounce window link to the same WorkItem.
    """
    debounce_key = f"debounce:component:{signal_in.component_id}"
    existing = await redis_client.get(debounce_key)
    if existing:
        return UUID(existing)

    lock_key = f"lock:{debounce_key}"
    lock_acquired = await redis_client.set(
        lock_key, consumer_name, nx=True, ex=settings.debounce_seconds
    )

    if not lock_acquired:
        # Another worker is creating the WorkItem — poll for the result.
        for _ in range(20):
            await asyncio.sleep(0.05)
            existing = await redis_client.get(debounce_key)
            if existing:
                return UUID(existing)
        # Last attempt to grab the lock after polling timeout.
        lock_acquired = await redis_client.set(
            lock_key, consumer_name, nx=True, ex=settings.debounce_seconds
        )
        if not lock_acquired:
            raise RuntimeError(
                f"Debounce lock not released for component={signal_in.component_id}. "
                "The creating worker may have crashed."
            )

    try:
        async with SessionLocal() as session:
            existing_item = await _find_recent_work_item(
                session, signal_in.component_id, signal_in.timestamp
            )
            if existing_item:
                await redis_client.set(
                    debounce_key, str(existing_item.id), ex=settings.debounce_seconds
                )
                return existing_item.id

            severity = map_signal_severity(signal_in)
            item = await _create_work_item(signal_in, severity)
            await redis_client.set(
                debounce_key, str(item.id), ex=settings.debounce_seconds
            )
            await upsert_active_incident(redis_client, item)
            logger.info(
                "created_work_item=%s component=%s severity=%s",
                item.id,
                item.component_id,
                item.severity,
            )
            return item.id
    finally:
        await redis_client.delete(lock_key)


# ── Signal storage ────────────────────────────────────────────────────────────

@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(
        multiplier=0.1,
        min=settings.retry_min_wait_s,
        max=settings.retry_max_wait_s,
    ),
    stop=stop_after_attempt(settings.retry_max_attempts),
    reraise=True,
)
async def _store_raw_signal(signal_in: SignalIn, work_item_id: UUID) -> None:
    """Store raw signal in MongoDB and increment WorkItem.signal_count in Postgres."""
    await mongo_db.raw_signals.insert_one(
        {
            "signal_id": signal_in.signal_id,
            "work_item_id": str(work_item_id),
            "component_id": signal_in.component_id,
            "timestamp": signal_in.timestamp,
            "severity": signal_in.severity,
            "payload": signal_in.payload,
            "stored_at": datetime.now(timezone.utc),
        }
    )
    # Increment denormalized counter on WorkItem.
    # Uses SQL UPDATE with increment to avoid read-modify-write race.
    async with SessionLocal() as session:
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == work_item_id)
            .values(signal_count=WorkItem.signal_count + 1)
        )
        await session.commit()


# ── Dead-letter ───────────────────────────────────────────────────────────────

async def _dead_letter(message_id: str, fields: dict[str, str], reason: str) -> None:
    """
    Move a permanently failing message to the dead-letter stream.
    ACK it from the main stream so it doesn't block the PEL forever.
    """
    await redis_client.xadd(
        DEAD_LETTER_STREAM,
        {**fields, "original_id": message_id, "failure_reason": reason},
        maxlen=50_000,
        approximate=True,
    )
    await redis_client.xack(settings.signal_stream, settings.signal_group, message_id)
    metrics["dead_lettered"] += 1
    logger.error(
        "dead_lettered message_id=%s component=%s reason=%s",
        message_id,
        fields.get("component_id", "unknown"),
        reason,
    )


# ── Message processing ────────────────────────────────────────────────────────

async def _process_message(message_id: str, fields: dict[str, str]) -> None:
    """
    Process a single stream message under the semaphore.

    Flow:
      decode → get_or_create_work_item (debounce) → store_raw_signal → xack

    xack happens only on success. Failed messages stay in PEL and are
    retried by the PEL recovery loop or reclaimed by another worker.
    """
    assert _sem is not None
    async with _sem:
        try:
            signal_in = _decode_signal(fields)
            work_item_id = await get_or_create_work_item(signal_in)
            await _store_raw_signal(signal_in, work_item_id)
            await redis_client.xack(
                settings.signal_stream, settings.signal_group, message_id
            )
            metrics["processed"] += 1
        except Exception as exc:
            metrics["errors"] += 1
            logger.exception("failed_message=%s component=%s", message_id, fields.get("component_id"))
            # Do NOT xack — message stays in PEL for recovery loop.
            raise exc


# ── PEL recovery ──────────────────────────────────────────────────────────────

async def _recover_pel() -> None:
    """
    Reclaim messages that have been idle in the PEL longer than
    pel_claim_idle_ms. This handles crashed or slow workers.

    Uses XAUTOCLAIM (Redis 6.2+) to atomically transfer ownership
    of stale PEL entries to this worker.

    Runs every 30 seconds in the background.
    """
    while not stop_event.is_set():
        await asyncio.sleep(30)
        try:
            # XAUTOCLAIM: reclaim up to pel_claim_batch messages idle > pel_claim_idle_ms
            result = await redis_client.xautoclaim(
                settings.signal_stream,
                settings.signal_group,
                consumer_name,
                settings.pel_claim_idle_ms,
                "0-0",
                count=settings.pel_claim_batch,
            )
            # result = (next_start_id, [(message_id, fields), ...], [deleted_ids])
            claimed_messages = result[1] if isinstance(result, (list, tuple)) else []
            if claimed_messages:
                logger.info(
                    "pel_recovery claimed=%s worker=%s",
                    len(claimed_messages),
                    consumer_name,
                )
                tasks = [
                    _process_message(mid, fields)
                    for mid, fields in claimed_messages
                    if fields  # skip deleted/empty entries
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # Dead-letter anything that fails PEL recovery too.
                for (mid, fields), result in zip(claimed_messages, results):
                    if isinstance(result, Exception) and fields:
                        await _dead_letter(mid, fields, str(result))
        except Exception:
            logger.exception("pel_recovery_error worker=%s", consumer_name)


# ── Observability ─────────────────────────────────────────────────────────────

async def _log_metrics() -> None:
    """
    Emit throughput metrics every 5 seconds.

    Uses xpending summary (O(1)) — not xpending_range (O(N)).
    xpending_range scans the PEL list; xpending returns aggregate counts.
    """
    last_count = 0
    while not stop_event.is_set():
        await asyncio.sleep(5)
        try:
            processed = metrics["processed"]
            delta = processed - last_count
            last_count = processed

            # xlen = total messages in stream (including unacked)
            stream_depth = await redis_client.xlen(settings.signal_stream)

            # xpending summary: returns (total_pending, min_id, max_id, consumer_list)
            pending_summary = await redis_client.xpending(
                settings.signal_stream, settings.signal_group
            )
            total_pending = pending_summary.get("pending", 0) if pending_summary else 0

            logger.info(
                "worker=%s signals_per_sec=%.2f processed=%s errors=%s "
                "db_retries=%s dead_lettered=%s stream_depth=%s pel_pending=%s",
                consumer_name,
                delta / 5,
                processed,
                metrics["errors"],
                metrics["db_retries"],
                metrics["dead_lettered"],
                stream_depth,
                total_pending,
            )
        except Exception:
            logger.exception("metrics_error worker=%s", consumer_name)


# ── Main consume loop ─────────────────────────────────────────────────────────

async def consume_forever() -> None:
    global _sem
    _sem = asyncio.Semaphore(settings.worker_concurrency)

    await init_postgres()
    await ensure_indexes()
    await ensure_stream_group()

    asyncio.create_task(_log_metrics())
    asyncio.create_task(_recover_pel())

    logger.info(
        "worker_started=%s group=%s concurrency=%s",
        consumer_name,
        settings.signal_group,
        settings.worker_concurrency,
    )

    while not stop_event.is_set():
        try:
            response = await redis_client.xreadgroup(
                settings.signal_group,
                consumer_name,
                streams={settings.signal_stream: ">"},
                count=settings.worker_batch_size,
                block=settings.worker_poll_timeout_ms,
            )
        except Exception:
            logger.exception("xreadgroup_error worker=%s", consumer_name)
            await asyncio.sleep(1)
            continue

        if not response:
            continue

        for _, messages in response:
            # gather with return_exceptions=True so one failure doesn't
            # cancel the rest of the batch.
            await asyncio.gather(
                *(_process_message(mid, fields) for mid, fields in messages),
                return_exceptions=True,
            )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_stop)
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()