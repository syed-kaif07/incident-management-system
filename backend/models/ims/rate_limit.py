import time

from redis.asyncio import Redis

# ---------------------------------------------------------------------------
# Atomic Lua script: INCRBY + EXPIRE in a single round-trip.
#
# Why Lua?
#   Two separate commands (INCRBY then EXPIRE) have a race window:
#   if the process crashes between them the key never expires → permanent
#   rate-block for that client.  Redis executes Lua atomically, so there
#   is no intermediate state.
#
# KEYS[1] → bucket key  (e.g. "rate:ingest:127.0.0.1:1746123456")
# ARGV[1] → cost        (number of signals in this batch)
# ARGV[2] → ttl         (window_seconds + 2 safety buffer)
# ---------------------------------------------------------------------------
_RATE_LIMIT_SCRIPT = """
local count = redis.call('INCRBY', KEYS[1], ARGV[1])
if count == tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return count
"""


async def allow_request(
    redis: Redis,
    key: str,
    limit: int,
    window_seconds: int = 1,
    cost: int = 1,
) -> bool:
    """
    Fixed-window rate limiter.

    Args:
        redis:          Async Redis client.
        key:            Unique key per client (e.g. "rate:ingest:<ip>").
        limit:          Max allowed cost units within window_seconds.
        window_seconds: Length of the time bucket in seconds.
        cost:           Cost of this request (batch size for bulk ingestion).

    Returns:
        True  → request is within limit, allow it.
        False → limit exceeded, reject with 429.

    Known trade-off:
        Fixed-window allows up to 2× limit at window boundaries
        (tail of one window + head of next).  This is acceptable for
        high-throughput ingestion where slight bursts are tolerable.
        Upgrade to sliding-window if stricter enforcement is needed.
    """
    bucket = int(time.time() // window_seconds)
    bucket_key = f"{key}:{bucket}"
    ttl = window_seconds + 2  # safety buffer so key always expires

    count = await redis.eval(_RATE_LIMIT_SCRIPT, 1, bucket_key, cost, ttl)
    return int(count) <= limit