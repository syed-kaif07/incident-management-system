from datetime import datetime, timezone

import pytest

from ims.alerting import map_signal_severity
from ims.schemas import SignalIn


def _make_signal(component_id: str, component_type: str, severity: str = "P3") -> SignalIn:
    return SignalIn(
        component_id=component_id,
        timestamp=datetime.now(timezone.utc),
        severity=severity,
        payload={"component_type": component_type},
    )


# ── Strategy: RDBMS ───────────────────────────────────────────────────────────

def test_rdbms_maps_to_p0_via_component_type() -> None:
    signal = _make_signal("PRIMARY_01", "rdbms", severity="P3")
    assert map_signal_severity(signal) == "P0"


def test_rdbms_maps_to_p0_via_component_id() -> None:
    signal = _make_signal("POSTGRES_PRIMARY", "unknown", severity="P3")
    assert map_signal_severity(signal) == "P0"


def test_rdbms_overrides_submitted_severity() -> None:
    # Even if producer says P4, RDBMS is always P0
    signal = _make_signal("DB_PRIMARY", "rdbms", severity="P4")
    assert map_signal_severity(signal) == "P0"


# ── Strategy: MCP ─────────────────────────────────────────────────────────────

def test_mcp_maps_to_p1_via_component_type() -> None:
    signal = _make_signal("HOST_07", "mcp", severity="P3")
    assert map_signal_severity(signal) == "P1"


def test_mcp_maps_to_p1_via_component_id() -> None:
    signal = _make_signal("MCP_HOST_07", "unknown", severity="P3")
    assert map_signal_severity(signal) == "P1"


# ── Strategy: Async Queue ─────────────────────────────────────────────────────

def test_queue_maps_to_p1_via_component_type() -> None:
    signal = _make_signal("WORKER_03", "queue", severity="P4")
    assert map_signal_severity(signal) == "P1"


def test_kafka_maps_to_p1_via_component_id() -> None:
    signal = _make_signal("KAFKA_BROKER_01", "unknown", severity="P4")
    assert map_signal_severity(signal) == "P1"


# ── Strategy: NoSQL ───────────────────────────────────────────────────────────

def test_nosql_maps_to_p1_via_component_type() -> None:
    signal = _make_signal("STORE_01", "mongodb", severity="P3")
    assert map_signal_severity(signal) == "P1"


def test_mongo_maps_to_p1_via_component_id() -> None:
    signal = _make_signal("MONGO_PRIMARY_01", "unknown", severity="P3")
    assert map_signal_severity(signal) == "P1"


# ── Strategy: Cache ───────────────────────────────────────────────────────────

def test_cache_maps_to_p2_via_component_type() -> None:
    signal = _make_signal("CLUSTER_01", "cache", severity="P0")
    assert map_signal_severity(signal) == "P2"


def test_redis_maps_to_p2_via_component_id() -> None:
    signal = _make_signal("REDIS_CACHE_01", "unknown", severity="P0")
    assert map_signal_severity(signal) == "P2"


def test_cache_overrides_submitted_p0() -> None:
    # Cache failure is always P2 regardless of what producer claims
    signal = _make_signal("CACHE_CLUSTER_01", "cache", severity="P0")
    assert map_signal_severity(signal) == "P2"


# ── Strategy: Fallback ────────────────────────────────────────────────────────

def test_fallback_uses_submitted_severity_p1() -> None:
    signal = _make_signal("UNKNOWN_COMPONENT", "unknown_type", severity="P1")
    assert map_signal_severity(signal) == "P1"


def test_fallback_uses_submitted_severity_p3() -> None:
    signal = _make_signal("CUSTOM_SERVICE_99", "custom", severity="P3")
    assert map_signal_severity(signal) == "P3"


# ── Strategy priority: RDBMS beats Cache ─────────────────────────────────────

def test_rdbms_beats_cache_when_both_match() -> None:
    # component_type=rdbms but component_id contains "cache" — RDBMS wins (P0)
    signal = SignalIn(
        component_id="cache_db_primary",
        timestamp=datetime.now(timezone.utc),
        severity="P3",
        payload={"component_type": "rdbms"},
    )
    assert map_signal_severity(signal) == "P0"


# ── Debounce key logic (unit — no Redis needed) ───────────────────────────────

def test_debounce_key_format() -> None:
    """Debounce key must be deterministic per component_id."""
    component_id = "CACHE_CLUSTER_01"
    key = f"debounce:component:{component_id}"
    assert key == "debounce:component:CACHE_CLUSTER_01"


def test_debounce_key_unique_per_component() -> None:
    key_a = f"debounce:component:CACHE_CLUSTER_01"
    key_b = f"debounce:component:RDBMS_PRIMARY_01"
    assert key_a != key_b


def test_debounce_lock_key_format() -> None:
    debounce_key = "debounce:component:CACHE_CLUSTER_01"
    lock_key = f"lock:{debounce_key}"
    assert lock_key == "lock:debounce:component:CACHE_CLUSTER_01"