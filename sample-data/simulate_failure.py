"""
Failure Simulation Script
=========================
Simulates a realistic multi-component failure cascade:

  Phase 1 — RDBMS Outage (t=0s to t=15s)
    Primary database starts throwing connection timeouts.
    Signals arrive at high volume → worker creates P0 WorkItem.

  Phase 2 — MCP Host Failure (t=10s to t=25s)
    MCP hosts start failing mid-RDBMS outage (cascading failure).
    Debounce creates separate P1 WorkItem for MCP.

  Phase 3 — Cache Degradation (t=20s to t=30s)
    Cache cluster degrades as connection pool exhausts.
    Separate P2 WorkItem created.

  Phase 4 — Recovery signals (t=30s)
    All components return to healthy state.
    No new WorkItems created (debounce window expired).

Usage:
    python simulate_failure.py
    python simulate_failure.py --ingestion-url http://localhost:8001/signals
    python simulate_failure.py --phase rdbms   # run single phase only
"""

import argparse
import asyncio
import json
from datetime import datetime, timezone

import httpx

INGESTION_URL = "http://localhost:8001/signals"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Phase definitions ─────────────────────────────────────────────────────────

PHASES = {
    "rdbms": {
        "label": "Phase 1 — RDBMS Primary Outage",
        "signals": [
            {
                "component_id": "RDBMS_PRIMARY_01",
                "timestamp": _now(),
                "severity": "P0",
                "payload": {
                    "component_type": "rdbms",
                    "error_code": "CONN_TIMEOUT",
                    "message": "Database connection timeout after 5000ms",
                    "latency_ms": 5000,
                    "host": "pg-primary-01.internal",
                    "port": 5432,
                    "trace_id": "trace-rdbms-001",
                },
            },
            {
                "component_id": "RDBMS_PRIMARY_01",
                "timestamp": _now(),
                "severity": "P0",
                "payload": {
                    "component_type": "rdbms",
                    "error_code": "MAX_CONNECTIONS",
                    "message": "FATAL: remaining connection slots reserved for replication",
                    "latency_ms": 0,
                    "host": "pg-primary-01.internal",
                    "active_connections": 100,
                    "max_connections": 100,
                    "trace_id": "trace-rdbms-002",
                },
            },
            {
                "component_id": "RDBMS_REPLICA_02",
                "timestamp": _now(),
                "severity": "P0",
                "payload": {
                    "component_type": "rdbms",
                    "error_code": "REPLICATION_LAG",
                    "message": "Replica lag exceeded 30s — reads may return stale data",
                    "latency_ms": 30000,
                    "host": "pg-replica-02.internal",
                    "lag_seconds": 34,
                    "trace_id": "trace-rdbms-003",
                },
            },
        ],
        "repeat": 5,
        "interval_s": 2,
    },
    "mcp": {
        "label": "Phase 2 — MCP Host Cascade Failure",
        "signals": [
            {
                "component_id": "MCP_HOST_07",
                "timestamp": _now(),
                "severity": "P1",
                "payload": {
                    "component_type": "mcp",
                    "error_code": "DB_POOL_EXHAUSTED",
                    "message": "MCP host cannot acquire DB connection — pool exhausted by RDBMS outage",
                    "latency_ms": 8000,
                    "host": "mcp-host-07.internal",
                    "downstream": "RDBMS_PRIMARY_01",
                    "trace_id": "trace-mcp-001",
                },
            },
            {
                "component_id": "MCP_HOST_08",
                "timestamp": _now(),
                "severity": "P1",
                "payload": {
                    "component_type": "mcp",
                    "error_code": "HEALTH_CHECK_FAILED",
                    "message": "MCP host health check failed — dependency RDBMS_PRIMARY_01 is down",
                    "latency_ms": 3000,
                    "host": "mcp-host-08.internal",
                    "downstream": "RDBMS_PRIMARY_01",
                    "trace_id": "trace-mcp-002",
                },
            },
        ],
        "repeat": 4,
        "interval_s": 2,
    },
    "cache": {
        "label": "Phase 3 — Cache Cluster Degradation",
        "signals": [
            {
                "component_id": "CACHE_CLUSTER_01",
                "timestamp": _now(),
                "severity": "P2",
                "payload": {
                    "component_type": "cache",
                    "error_code": "EVICTION_SPIKE",
                    "message": "Cache eviction rate spiked — hot keys being dropped",
                    "latency_ms": 450,
                    "host": "redis-cluster-01.internal",
                    "eviction_rate": "12000/s",
                    "memory_used_pct": 97,
                    "trace_id": "trace-cache-001",
                },
            },
        ],
        "repeat": 3,
        "interval_s": 2,
    },
    "recovery": {
        "label": "Phase 4 — Recovery Signals",
        "signals": [
            {
                "component_id": "RDBMS_PRIMARY_01",
                "timestamp": _now(),
                "severity": "P3",
                "payload": {
                    "component_type": "rdbms",
                    "error_code": "RECOVERING",
                    "message": "Primary DB accepting connections — connection count normalising",
                    "latency_ms": 120,
                    "host": "pg-primary-01.internal",
                    "active_connections": 45,
                    "trace_id": "trace-recovery-001",
                },
            },
            {
                "component_id": "MCP_HOST_07",
                "timestamp": _now(),
                "severity": "P3",
                "payload": {
                    "component_type": "mcp",
                    "error_code": "RECOVERING",
                    "message": "MCP host reconnected to DB — health checks passing",
                    "latency_ms": 80,
                    "host": "mcp-host-07.internal",
                    "trace_id": "trace-recovery-002",
                },
            },
            {
                "component_id": "CACHE_CLUSTER_01",
                "timestamp": _now(),
                "severity": "P4",
                "payload": {
                    "component_type": "cache",
                    "error_code": "STABLE",
                    "message": "Cache eviction rate normalised",
                    "latency_ms": 2,
                    "host": "redis-cluster-01.internal",
                    "eviction_rate": "120/s",
                    "memory_used_pct": 71,
                    "trace_id": "trace-recovery-003",
                },
            },
        ],
        "repeat": 1,
        "interval_s": 0,
    },
}

PHASE_ORDER = ["rdbms", "mcp", "cache", "recovery"]
PHASE_DELAYS = {"rdbms": 0, "mcp": 10, "cache": 20, "recovery": 30}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def send_signals(
    client: httpx.AsyncClient,
    url: str,
    signals: list[dict],
    label: str,
) -> None:
    # Refresh timestamps just before sending
    for signal in signals:
        signal["timestamp"] = _now()
    try:
        response = await client.post(url, json=signals)
        if response.status_code == 202:
            data = response.json()
            print(f"  ✓ {label} → accepted={data['accepted']} ids={data['stream_ids'][:2]}...")
        elif response.status_code == 503:
            print(f"  ⚠ {label} → backpressure (503) — stream queue full")
        elif response.status_code == 429:
            print(f"  ⚠ {label} → rate limited (429)")
        else:
            print(f"  ✗ {label} → HTTP {response.status_code}: {response.text[:100]}")
    except Exception as exc:
        print(f"  ✗ {label} → connection error: {exc}")


# ── Phase runner ──────────────────────────────────────────────────────────────

async def run_phase(
    client: httpx.AsyncClient,
    url: str,
    phase_key: str,
) -> None:
    phase = PHASES[phase_key]
    print(f"\n{'='*60}")
    print(f"{phase['label']}")
    print(f"{'='*60}")

    for i in range(phase["repeat"]):
        await send_signals(
            client,
            url,
            phase["signals"],
            f"batch {i+1}/{phase['repeat']}",
        )
        if phase["interval_s"] > 0 and i < phase["repeat"] - 1:
            print(f"  ... waiting {phase['interval_s']}s ...")
            await asyncio.sleep(phase["interval_s"])


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(ingestion_url: str, phase: str | None) -> None:
    print("\nIMS Failure Simulation")
    print(f"Target: {ingestion_url}")
    print(f"Time:   {_now()}")

    async with httpx.AsyncClient(timeout=10) as client:
        # Verify ingestion is up
        try:
            resp = await client.get(ingestion_url.replace("/signals", "/health"))
            health = resp.json()
            print(f"\nIngestion health: {health.get('status')} | "
                  f"stream_depth={health.get('stream', {}).get('depth')}")
        except Exception:
            print("\n⚠ Cannot reach ingestion API — is it running?")
            return

        if phase:
            # Run single phase
            if phase not in PHASES:
                print(f"Unknown phase: {phase}. Choose from: {list(PHASES.keys())}")
                return
            await run_phase(client, ingestion_url, phase)
        else:
            # Run full cascade with delays
            print("\nRunning full failure cascade...")
            print("Phase sequence: RDBMS → MCP (t+10s) → Cache (t+20s) → Recovery (t+30s)")

            tasks = []
            for phase_key in PHASE_ORDER:
                delay = PHASE_DELAYS[phase_key]
                tasks.append(_delayed_phase(client, ingestion_url, phase_key, delay))
            await asyncio.gather(*tasks)

    print(f"\n{'='*60}")
    print("Simulation complete.")
    print(f"Check dashboard at http://localhost:5173")
    print(f"Check incidents at  http://localhost:8000/incidents/active")
    print(f"{'='*60}\n")


async def _delayed_phase(
    client: httpx.AsyncClient,
    url: str,
    phase_key: str,
    delay: int,
) -> None:
    if delay > 0:
        await asyncio.sleep(delay)
    await run_phase(client, url, phase_key)


def main() -> None:
    parser = argparse.ArgumentParser(description="IMS failure cascade simulator")
    parser.add_argument(
        "--ingestion-url",
        default=INGESTION_URL,
        help="Ingestion API URL (default: http://localhost:8001/signals)",
    )
    parser.add_argument(
        "--phase",
        choices=list(PHASES.keys()),
        default=None,
        help="Run a single phase only (default: run full cascade)",
    )
    args = parser.parse_args()
    asyncio.run(run(args.ingestion_url, args.phase))


if __name__ == "__main__":
    main()
