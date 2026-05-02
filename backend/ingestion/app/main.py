import json
import logging
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from ims.config import get_settings
from ims.database import check_mongo, check_postgres, check_redis, lifespan, redis_client
from ims.rate_limit import allow_request
from ims.schemas import IngestResponse, SignalIn

settings = get_settings()
logger = logging.getLogger("ims.ingestion")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = FastAPI(title="IMS Ingestion API", version="0.2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_redis() -> Redis:
    return redis_client


def _payload_to_stream(signal: SignalIn) -> dict[str, str]:
    return {
        "signal_id": signal.signal_id,
        "component_id": signal.component_id,
        "timestamp": signal.timestamp.isoformat(),
        "severity": signal.severity,
        "payload": json.dumps(signal.payload, separators=(",", ":")),
    }


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health(redis: Annotated[Redis, Depends(get_redis)]) -> dict:
    """
    Deep health check — reports status of every dependency plus
    current stream depth and backpressure state.

    Returns 200 even when degraded so load balancers keep routing
    to this instance. Callers should inspect the 'status' field.
    """
    redis_ok = await check_redis()
    postgres_ok = await check_postgres()
    mongo_ok = await check_mongo()

    stream_depth: int | None = None
    backpressure_active = False

    if redis_ok:
        try:
            stream_depth = await redis.xlen(settings.signal_stream)
            backpressure_active = stream_depth >= settings.backpressure_threshold
        except Exception:
            pass

    overall = "ok" if (redis_ok and postgres_ok and mongo_ok) else "degraded"

    return {
        "status": overall,
        "service": "ingestion",
        "dependencies": {
            "redis": "ok" if redis_ok else "error",
            "postgres": "ok" if postgres_ok else "error",
            "mongo": "ok" if mongo_ok else "error",
        },
        "stream": {
            "depth": stream_depth,
            "backpressure_active": backpressure_active,
            "backpressure_threshold": settings.backpressure_threshold,
        },
    }


# ── Ingestion ─────────────────────────────────────────────────────────────────

@app.post("/signals", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_signals(
    body: SignalIn | list[SignalIn],
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
) -> IngestResponse:
    """
    Accept signals and publish them to Redis Streams.

    This endpoint NEVER writes to PostgreSQL or MongoDB directly.
    All DB writes happen in the worker consumer group.

    Backpressure:
        If the stream depth exceeds backpressure_threshold, return 503
        so upstream callers can back off. This protects the worker from
        falling further behind when DB is slow.

    Rate limiting:
        Fixed-window per client IP. Batch cost = number of signals.
        Returns 429 when limit exceeded.
    """
    signals = body if isinstance(body, list) else [body]

    if not signals:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="at least one signal is required",
        )

    # ── Backpressure check ────────────────────────────────────────────────────
    # Check stream depth before accepting more signals.
    # This is the primary defence when workers fall behind:
    #   producers slow down → stream drains → 503s stop → normal flow resumes.
    stream_depth = await redis.xlen(settings.signal_stream)
    if stream_depth >= settings.backpressure_threshold:
        logger.warning(
            "backpressure_triggered stream=%s depth=%s threshold=%s",
            settings.signal_stream,
            stream_depth,
            settings.backpressure_threshold,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "ingestion_backpressure",
                "message": "Stream queue is full. Retry after backing off.",
                "stream_depth": stream_depth,
                "threshold": settings.backpressure_threshold,
            },
        )

    # ── Rate limit ────────────────────────────────────────────────────────────
    client_id = request.client.host if request.client else "unknown"
    allowed = await allow_request(
        redis,
        f"rate:ingest:{client_id}",
        settings.rate_limit_per_second,
        cost=len(signals),
    )
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="ingestion rate limit exceeded",
        )

    # ── Publish to stream ─────────────────────────────────────────────────────
    # Pipeline: all xadd commands sent in a single round-trip.
    # maxlen + approximate=True keeps stream bounded without blocking.
    stream_ids: list[str] = []
    async with redis.pipeline(transaction=False) as pipe:
        for signal in signals:
            pipe.xadd(
                settings.signal_stream,
                _payload_to_stream(signal),
                maxlen=settings.max_stream_len,
                approximate=True,
            )
        results = await pipe.execute()
        stream_ids = [r.decode() if isinstance(r, bytes) else str(r) for r in results]

    component_ids = list({s.component_id for s in signals})
    logger.info(
        "accepted signals=%s stream=%s components=%s",
        len(signals),
        settings.signal_stream,
        component_ids,
    )

    return IngestResponse(
        accepted=len(signals),
        stream=settings.signal_stream,
        stream_ids=stream_ids,
    )