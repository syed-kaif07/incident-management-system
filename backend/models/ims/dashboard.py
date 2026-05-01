import json
from uuid import UUID

from redis.asyncio import Redis

from ims.config import get_settings
from ims.sql_models import WorkItem

settings = get_settings()


def _item_to_payload(item: WorkItem) -> str:
    return json.dumps(
        {
            "id": str(item.id),
            "component_id": item.component_id,
            "status": item.status,
            "severity": item.severity,
            "start_time": item.start_time.isoformat(),
            "end_time": item.end_time.isoformat() if item.end_time else None,
            "mttr_seconds": item.mttr_seconds,
        }
    )


async def upsert_active_incident(redis: Redis, item: WorkItem) -> None:
    if item.status == "CLOSED":
        await redis.hdel(settings.dashboard_cache_key, str(item.id))
        return
    await redis.hset(settings.dashboard_cache_key, str(item.id), _item_to_payload(item))


async def remove_active_incident(redis: Redis, item_id: UUID | str) -> None:
    await redis.hdel(settings.dashboard_cache_key, str(item_id))


async def list_active_incidents(redis: Redis) -> list[dict]:
    raw = await redis.hvals(settings.dashboard_cache_key)
    severity_rank = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "P4": 4}
    items = [json.loads(value) for value in raw]
    return sorted(items, key=lambda item: (severity_rank.get(item["severity"], 99), item["start_time"]))
