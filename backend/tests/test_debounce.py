from datetime import datetime, timezone

from ims.alerting import map_signal_severity
from ims.schemas import SignalIn


def test_alert_strategy_maps_rdbms_to_p0() -> None:
    signal = SignalIn(
        component_id="RDBMS_PRIMARY_01",
        timestamp=datetime.now(timezone.utc),
        severity="P3",
        payload={"component_type": "rdbms"},
    )
    assert map_signal_severity(signal) == "P0"


def test_alert_strategy_maps_cache_to_p2() -> None:
    signal = SignalIn(
        component_id="CACHE_CLUSTER_01",
        timestamp=datetime.now(timezone.utc),
        severity="P0",
        payload={"component_type": "cache"},
    )
    assert map_signal_severity(signal) == "P2"
