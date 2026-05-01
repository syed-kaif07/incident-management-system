import time

from redis.asyncio import Redis


async def allow_request(redis: Redis, key: str, limit: int, window_seconds: int = 1, cost: int = 1) -> bool:
    bucket = int(time.time() // window_seconds)
    bucket_key = f"{key}:{bucket}"
    count = await redis.incrby(bucket_key, cost)
    if count == cost:
        await redis.expire(bucket_key, window_seconds + 2)
    return int(count) <= limit
