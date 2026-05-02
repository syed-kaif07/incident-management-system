from abc import ABC, abstractmethod

from ims.schemas import SignalIn


# ── Base ──────────────────────────────────────────────────────────────────────

class AlertStrategy(ABC):
    """
    Strategy pattern for mapping a signal to an incident severity.

    Each strategy answers two questions:
      1. matches()  → does this strategy apply to the given signal?
      2. severity() → what severity should the resulting WorkItem carry?

    Strategies are evaluated in priority order (STRATEGIES tuple below).
    First match wins. SubmittedSeverityStrategy is always last as fallback.
    """

    @abstractmethod
    def matches(self, signal: SignalIn) -> bool:
        raise NotImplementedError

    @abstractmethod
    def severity(self, signal: SignalIn) -> str:
        raise NotImplementedError

    # ── Shared helper ─────────────────────────────────────────────────────────
    def _matches_markers(self, signal: SignalIn, markers: tuple[str, ...]) -> bool:
        """
        Check both component_type (from payload) and component_id for any marker.
        component_type is the canonical field; component_id is a fallback for
        signals that don't populate payload correctly.
        """
        component_type = str(signal.payload.get("component_type", "")).lower()
        component_id = signal.component_id.lower()
        return (
            component_type in markers
            or any(marker in component_id for marker in markers)
        )


# ── Concrete strategies ───────────────────────────────────────────────────────

class RDBMSAlertStrategy(AlertStrategy):
    """
    RDBMS failures are P0 — source of truth is down, data writes are failing.
    Covers: PostgreSQL, MySQL, Oracle, generic 'db_' prefixed components.
    """
    _MARKERS = ("rdbms", "postgres", "postgresql", "mysql", "oracle", "db_")

    def matches(self, signal: SignalIn) -> bool:
        return self._matches_markers(signal, self._MARKERS)

    def severity(self, signal: SignalIn) -> str:
        return "P0"


class MCPAlertStrategy(AlertStrategy):
    """
    MCP Host failures are P1 — control plane is impacted, agent routing breaks.
    Covers: MCP hosts and MCP-prefixed component IDs.
    """
    _MARKERS = ("mcp", "mcp_host", "mcp-host")

    def matches(self, signal: SignalIn) -> bool:
        return self._matches_markers(signal, self._MARKERS)

    def severity(self, signal: SignalIn) -> str:
        return "P1"


class AsyncQueueAlertStrategy(AlertStrategy):
    """
    Async queue failures are P1 — message delivery is broken, downstream
    consumers are starving. Covers: Kafka, RabbitMQ, Celery, SQS, Redis Streams.
    """
    _MARKERS = ("queue", "kafka", "rabbitmq", "celery", "sqs", "stream")

    def matches(self, signal: SignalIn) -> bool:
        return self._matches_markers(signal, self._MARKERS)

    def severity(self, signal: SignalIn) -> str:
        return "P1"


class NoSQLAlertStrategy(AlertStrategy):
    """
    NoSQL failures are P1 — audit log / signal store is down.
    Covers: MongoDB, Cassandra, DynamoDB.
    """
    _MARKERS = ("mongo", "mongodb", "nosql", "cassandra", "dynamodb")

    def matches(self, signal: SignalIn) -> bool:
        return self._matches_markers(signal, self._MARKERS)

    def severity(self, signal: SignalIn) -> str:
        return "P1"


class CacheAlertStrategy(AlertStrategy):
    """
    Cache failures are P2 — hot path is degraded but persistent stores are up.
    Covers: Redis, Memcached, generic 'cache' components.
    """
    _MARKERS = ("cache", "redis", "memcached")

    def matches(self, signal: SignalIn) -> bool:
        return self._matches_markers(signal, self._MARKERS)

    def severity(self, signal: SignalIn) -> str:
        return "P2"


class APIAlertStrategy(AlertStrategy):
    """
    API / service failures are P2 — one service is degraded but infra is up.
    Covers: generic API, service, gateway, proxy components.
    """
    _MARKERS = ("api", "service", "gateway", "proxy", "endpoint")

    def matches(self, signal: SignalIn) -> bool:
        return self._matches_markers(signal, self._MARKERS)

    def severity(self, signal: SignalIn) -> str:
        return "P2"


class SubmittedSeverityStrategy(AlertStrategy):
    """
    Fallback: trust whatever severity the signal producer submitted.
    Always matches — must remain last in STRATEGIES.
    """

    def matches(self, signal: SignalIn) -> bool:
        return True

    def severity(self, signal: SignalIn) -> str:
        return signal.severity


# ── Priority-ordered strategy chain ──────────────────────────────────────────
#
# Order matters: first match wins.
#
# Priority  Strategy                  Severity  Rationale
# ────────  ────────────────────────  ────────  ─────────────────────────────
#   1       RDBMSAlertStrategy        P0        Source of truth down
#   2       MCPAlertStrategy          P1        Control plane impacted
#   3       AsyncQueueAlertStrategy   P1        Message delivery broken
#   4       NoSQLAlertStrategy        P1        Audit log / signal store down
#   5       CacheAlertStrategy        P2        Hot path degraded
#   6       APIAlertStrategy          P2        Single service degraded
#   7       SubmittedSeverityStrategy —         Fallback: trust producer
#
STRATEGIES: tuple[AlertStrategy, ...] = (
    RDBMSAlertStrategy(),
    MCPAlertStrategy(),
    AsyncQueueAlertStrategy(),
    NoSQLAlertStrategy(),
    CacheAlertStrategy(),
    APIAlertStrategy(),
    SubmittedSeverityStrategy(),
)


def map_signal_severity(signal: SignalIn) -> str:
    """
    Walk STRATEGIES in priority order and return the first matching severity.
    Always returns a value — SubmittedSeverityStrategy guarantees a match.
    """
    for strategy in STRATEGIES:
        if strategy.matches(signal):
            return strategy.severity(signal)
    # Unreachable: SubmittedSeverityStrategy always matches.
    return signal.severity