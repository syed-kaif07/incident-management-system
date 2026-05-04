"""
IMS Load Test
=============
Simulates high-throughput signal ingestion at 5k–10k signals/sec.

Usage:
    python load_test.py --rate 10000 --duration 30
    python load_test.py --rate 5000 --duration 60 --batch-size 250
    python load_test.py --endpoint http://localhost:8001/signals --rate 8000
"""

import argparse
import asyncio
import time
from datetime import datetime, timezone
from random import choice, randint, random

import httpx

COMPONENTS = [
    ("RDBMS_PRIMARY_01", "rdbms"),
    ("RDBMS_REPLICA_02", "rdbms"),
    ("CACHE_CLUSTER_01", "cache"),
    ("CACHE_CLUSTER_02", "cache"),
    ("MCP_HOST_07", "mcp"),
    ("MCP_HOST_08", "mcp"),
    ("ASYNC_QUEUE_03", "queue"),
    ("ASYNC_QUEUE_04", "queue"),
    ("MONGO_PRIMARY_01", "mongodb"),
    ("API_GATEWAY_01", "api"),
]

SEVERITIES = ["P0", "P1", "P1", "P2", "P2", "P3", "P4"]  # weighted toward P1/P2


def make_signal(index: int) -> dict:
    component_id, component_type = choice(COMPONENTS)
    return {
        "component_id": component_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": choice(SEVERITIES),
        "payload": {
            "component_type": component_type,
            "error_code": f"SIM-{index % 17}",
            "message": "synthetic high-volume failure signal",
            "latency_ms": 500 + (index % 100),
            "trace_id": f"trace-{index}",
        },
    }


async def send_batch(
    client: httpx.AsyncClient,
    url: str,
    batch: list[dict],
    stats: dict,
) -> None:
    try:
        response = await client.post(url, json=batch)
        if response.status_code == 202:
            stats["sent"] += len(batch)
        elif response.status_code == 503:
            stats["backpressure"] += len(batch)
        elif response.status_code == 429:
            stats["rate_limited"] += len(batch)
        else:
            stats["errors"] += len(batch)
    except Exception as exc:
        stats["errors"] += len(batch)
        stats["last_error"] = str(exc)


async def run(rate: int, duration: int, endpoint: str, batch_size: int) -> None:
    stats: dict = {
        "sent": 0,
        "errors": 0,
        "backpressure": 0,
        "rate_limited": 0,
        "last_error": None,
    }

    batches_per_second = max(1, rate // batch_size)
    signal_index = 0
    start_wall = time.monotonic()

    # Connection pool sized for concurrent batches
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)

    async with httpx.AsyncClient(timeout=10, limits=limits) as client:
        for second in range(duration):
            second_start = time.monotonic()

            tasks = []
            for _ in range(batches_per_second):
                batch = [make_signal(signal_index + i) for i in range(batch_size)]
                signal_index += len(batch)
                tasks.append(send_batch(client, endpoint, batch, stats))

            await asyncio.gather(*tasks)

            elapsed = time.monotonic() - second_start
            sleep_for = max(0.0, 1.0 - elapsed)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

            actual_rate = stats["sent"] / max(1, time.monotonic() - start_wall)
            print(
                f"second={second + 1}/{duration} "
                f"sent={stats['sent']} "
                f"errors={stats['errors']} "
                f"backpressure={stats['backpressure']} "
                f"rate_limited={stats['rate_limited']} "
                f"actual_rate={actual_rate:.0f}/s"
            )

    # ── Summary ───────────────────────────────────────────────────────────────
    total_wall = time.monotonic() - start_wall
    total_attempted = signal_index
    print("\n" + "=" * 60)
    print("LOAD TEST SUMMARY")
    print("=" * 60)
    print(f"Duration:        {total_wall:.1f}s")
    print(f"Target rate:     {rate}/s")
    print(f"Actual rate:     {stats['sent'] / total_wall:.0f}/s")
    print(f"Total attempted: {total_attempted}")
    print(f"Accepted (202):  {stats['sent']}")
    print(f"Errors:          {stats['errors']}")
    print(f"Backpressure:    {stats['backpressure']} (503)")
    print(f"Rate limited:    {stats['rate_limited']} (429)")
    if stats["last_error"]:
        print(f"Last error:      {stats['last_error']}")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="IMS high-volume signal load test")
    parser.add_argument("--rate", type=int, default=10_000, help="Target signals/sec")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    parser.add_argument("--endpoint", default="http://localhost:8001/signals")
    parser.add_argument("--batch-size", type=int, default=500, help="Signals per HTTP request")
    args = parser.parse_args()
    asyncio.run(run(args.rate, args.duration, args.endpoint, args.batch_size))


if __name__ == "__main__":
    main()