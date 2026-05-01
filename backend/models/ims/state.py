from dataclasses import dataclass


class InvalidTransition(ValueError):
    pass


class MissingRCA(ValueError):
    pass


@dataclass(frozen=True)
class IncidentState:
    name: str
    allowed_next: tuple[str, ...]

    def can_transition(self, target: str) -> bool:
        return target in self.allowed_next or target == self.name


STATES = {
    "OPEN": IncidentState("OPEN", ("INVESTIGATING",)),
    "INVESTIGATING": IncidentState("INVESTIGATING", ("RESOLVED",)),
    "RESOLVED": IncidentState("RESOLVED", ("CLOSED",)),
    "CLOSED": IncidentState("CLOSED", ()),
}


def validate_transition(current: str, target: str, has_complete_rca: bool = False) -> None:
    if current not in STATES or target not in STATES:
        raise InvalidTransition(f"Unknown state transition {current} -> {target}")

    state = STATES[current]
    if not state.can_transition(target):
        raise InvalidTransition(f"Cannot transition work item from {current} to {target}")

    if target == "CLOSED" and not has_complete_rca:
        raise MissingRCA("RCA is required before closing a work item")
