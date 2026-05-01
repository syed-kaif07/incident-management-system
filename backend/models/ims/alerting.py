from abc import ABC, abstractmethod

from ims.schemas import SignalIn


class AlertStrategy(ABC):
    @abstractmethod
    def matches(self, signal: SignalIn) -> bool:
        raise NotImplementedError

    @abstractmethod
    def severity(self, signal: SignalIn) -> str:
        raise NotImplementedError


class RDBMSAlertStrategy(AlertStrategy):
    markers = ("rdbms", "postgres", "postgresql", "mysql", "oracle", "db_")

    def matches(self, signal: SignalIn) -> bool:
        component_type = str(signal.payload.get("component_type", "")).lower()
        component_id = signal.component_id.lower()
        return component_type in self.markers or any(marker in component_id for marker in self.markers)

    def severity(self, signal: SignalIn) -> str:
        return "P0"


class CacheAlertStrategy(AlertStrategy):
    markers = ("cache", "redis", "memcached")

    def matches(self, signal: SignalIn) -> bool:
        component_type = str(signal.payload.get("component_type", "")).lower()
        component_id = signal.component_id.lower()
        return component_type in self.markers or any(marker in component_id for marker in self.markers)

    def severity(self, signal: SignalIn) -> str:
        return "P2"


class SubmittedSeverityStrategy(AlertStrategy):
    def matches(self, signal: SignalIn) -> bool:
        return True

    def severity(self, signal: SignalIn) -> str:
        return signal.severity


STRATEGIES: tuple[AlertStrategy, ...] = (
    RDBMSAlertStrategy(),
    CacheAlertStrategy(),
    SubmittedSeverityStrategy(),
)


def map_signal_severity(signal: SignalIn) -> str:
    for strategy in STRATEGIES:
        if strategy.matches(signal):
            return strategy.severity(signal)
    return signal.severity
