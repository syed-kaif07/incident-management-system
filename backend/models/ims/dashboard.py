import json
from uuid import UUID

from redis.asyncio import Redis

from ims.config import get_settings
from ims.sql_models import WorkItem

settings = get_settings()

# Severity sort order — lower number = higher priority on dashboard.
_SEVERITY_RANK: dict[str, int] = {
    "P0": 0,
    "P1": 1,
    "P2": 2,
    "P3": 3,
    "P4": 4,
}

# Statuses that should remain visible on the active dashboard.
# RESOLVED is included so operators can see incidents awaiting closure.
# CLOSED is excluded — remove from cache when WorkItem reaches CLOSED.
_ACTIVE_STATUSES = {"OPEN", "INVESTIGATING", "RESOLVED"}


# ── Serialization ─────────────────────────────────────────────────────────────

def _item_to_payload(item: WorkItem) -> str:
    """
    Serialize a WorkItem to a JSON string for storage in the Redis hash.

    Includes signal_count and resolved_at so the dashboard can display
    these without hitting Postgres on every UI refresh.
    """
    return json.dumps(
        {
            "id": str(item.id),
            "component_id": item.component_id,
            "status": item.status,
            "severity": item.severity,
            "start_time": item.start_time.isoformat(),
            "resolved_at": item.resolved_at.isoformat() if item.resolved_at else None,
            "end_time": item.end_time.isoformat() if item.end_time else None,
            "mttr_seconds": item.mttr_seconds,
            "signal_count": getattr(item, "signal_count", 0),
        }
    )


# ── Cache operations ──────────────────────────────────────────────────────────

async def upsert_active_incident(redis: Redis, item: WorkItem) -> None:
    """
    Write or update a WorkItem in the active dashboard hash.

    - CLOSED items are removed from the hash entirely.
    - All other statuses (OPEN, INVESTIGATING, RESOLVED) are kept visible
      so operators can track incidents through to closure.

    Design: Redis Hash (HSET / HDEL) gives O(1) upsert and delete per
    incident regardless of total incident count.
    """
    if item.status not in _ACTIVE_STATUSES:
        # CLOSED or any future terminal status — evict from dashboard.
        await redis.hdel(settings.dashboard_cache_key, str(item.id))
        return
    await redis.hset(
        settings.dashboard_cache_key,
        str(item.id),
        _item_to_payload(item),
    )


async def remove_active_incident(redis: Redis, item_id: UUID | str) -> None:
    """
    Explicitly remove an incident from the dashboard cache.
    Called when a WorkItem is force-closed via the /close endpoint.
    """
    await redis.hdel(settings.dashboard_cache_key, str(item_id))


async def list_active_incidents(
    redis: Redis,
    limit: int = 200,
) -> list[dict]:
    """
    Return active incidents sorted by severity then start_time.

    Args:
        limit: Safety cap on returned items. Default 200.
               Prevents memory spikes when the hash grows large.
               At scale, replace Redis Hash with a Redis Sorted Set
               keyed on severity+time for server-side ordering and
               range queries without loading all values into Python.

    Returns:
        List of incident dicts sorted by (severity_rank, start_time).
    """
    raw = await redis.hvals(settings.dashboard_cache_key)
    if not raw:
        return []

    items = [json.loads(value) for value in raw]
    items.sort(
        key=lambda i: (
            _SEVERITY_RANK.get(i.get("severity", "P4"), 99),
            i.get("start_time", ""),
        )
    )
    return items[:limit]