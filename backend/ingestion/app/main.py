import json
import logging
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from ims.config import get_settings
from ims.database import redis_client
from ims.rate_limit import allow_request
from ims.schemas import IngestResponse, SignalIn

settings = get_settings()
logger = logging.getLogger("ims.ingestion")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = FastAPI(title="IMS Ingestion API", version="0.1.0")
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
        "component_id": signal.component_id,
        "timestamp": signal.timestamp.isoformat(),
        "severity": signal.severity,
        "payload": json.dumps(signal.payload, separators=(",", ":")),
    }


@app.get("/health")
async def health(redis: Annotated[Redis, Depends(get_redis)]) -> dict[str, str]:
    await redis.ping()
    return {"status": "ok", "service": "ingestion"}


@app.post("/signals", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_signals(
    body: SignalIn | list[SignalIn],
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
) -> IngestResponse:
    signals = body if isinstance(body, list) else [body]
    if not signals:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="at least one signal is required")

    client_id = request.client.host if request.client else "unknown"
    allowed = await allow_request(
        redis,
        f"rate:ingest:{client_id}",
        settings.rate_limit_per_second,
        cost=len(signals),
    )
    if not allowed:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="ingestion rate limit exceeded")

    async with redis.pipeline(transaction=False) as pipe:
        for signal in signals:
            pipe.xadd(settings.signal_stream, _payload_to_stream(signal), maxlen=2_000_000, approximate=True)
        await pipe.execute()

    logger.info("accepted_signals=%s stream=%s", len(signals), settings.signal_stream)
    return IngestResponse(accepted=len(signals), stream=settings.signal_stream)
