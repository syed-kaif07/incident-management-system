import argparse
import asyncio
from datetime import datetime, timezone
from random import choice, random

import httpx


COMPONENTS = [
    ("RDBMS_PRIMARY_01", "rdbms"),
    ("CACHE_CLUSTER_01", "cache"),
    ("MCP_HOST_07", "mcp"),
    ("ASYNC_QUEUE_03", "queue"),
]


def make_signal(index: int) -> dict:
    component_id, component_type = choice(COMPONENTS)
    return {
        "component_id": component_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": "P1" if random() > 0.2 else "P2",
        "payload": {
            "component_type": component_type,
            "error_code": f"SIM-{index % 17}",
            "message": "synthetic high-volume failure signal",
            "latency_ms": 500 + (index % 100),
        },
    }


async def send_batch(client: httpx.AsyncClient, url: str, batch: list[dict]) -> None:
    response = await client.post(url, json=batch)
    response.raise_for_status()


async def run(rate: int, duration: int, endpoint: str, batch_size: int) -> None:
    total = rate * duration
    batches_per_second = max(1, rate // batch_size)
    sent = 0
    async with httpx.AsyncClient(timeout=10) as client:
        for second in range(duration):
            tasks = []
            for _ in range(batches_per_second):
                batch = [make_signal(sent + i) for i in range(batch_size)]
                sent += len(batch)
                tasks.append(send_batch(client, endpoint, batch))
            await asyncio.gather(*tasks)
            print(f"second={second + 1} sent={sent}/{total}")
            await asyncio.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate high-volume IMS ingestion.")
    parser.add_argument("--rate", type=int, default=10_000, help="Signals per second")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    parser.add_argument("--endpoint", default="http://localhost:8001/signals", help="Ingestion endpoint")
    parser.add_argument("--batch-size", type=int, default=500, help="Signals per HTTP request")
    args = parser.parse_args()
    asyncio.run(run(args.rate, args.duration, args.endpoint, args.batch_size))


if __name__ == "__main__":
    main()
