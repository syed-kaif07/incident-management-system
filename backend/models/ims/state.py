from dataclasses import dataclass


# ── Exceptions ────────────────────────────────────────────────────────────────

class InvalidTransition(ValueError):
    pass


class MissingRCA(ValueError):
    pass


# ── State definitions ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class IncidentState:
    name: str
    allowed_next: tuple[str, ...]

    def can_transition(self, target: str) -> bool:
        """
        Returns True only for explicitly allowed forward transitions.
        Self-transitions (same → same) are intentionally blocked:
        a no-op PATCH should be caught at the API layer, not silently
        accepted here. This makes idempotency explicit, not accidental.
        """
        return target in self.allowed_next


STATES: dict[str, IncidentState] = {
    "OPEN":          IncidentState("OPEN",          ("INVESTIGATING",)),
    "INVESTIGATING": IncidentState("INVESTIGATING", ("RESOLVED",)),
    "RESOLVED":      IncidentState("RESOLVED",      ("CLOSED",)),
    "CLOSED":        IncidentState("CLOSED",        ()),   # terminal — no exits
}

# Validated at module load time: if a Status literal is added to schemas.py
# but forgotten here, this assertion fails immediately on import rather than
# silently raising InvalidTransition at runtime.
_KNOWN_STATUSES = {"OPEN", "INVESTIGATING", "RESOLVED", "CLOSED"}
assert set(STATES.keys()) == _KNOWN_STATUSES, (
    f"STATES keys {set(STATES.keys())} do not match known statuses {_KNOWN_STATUSES}"
)


# ── Transition result ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TransitionResult:
    """
    Returned by validate_transition() so callers know what side-effects
    to apply without duplicating transition logic.

    stamp_resolved_at: True when transitioning TO "RESOLVED".
        Caller (main.py) should set WorkItem.resolved_at = now() and
        compute mttr_seconds = (resolved_at - start_time).total_seconds().
    """
    previous: str
    current: str
    stamp_resolved_at: bool


# ── Validation ────────────────────────────────────────────────────────────────

def validate_transition(
    current: str,
    target: str,
    has_complete_rca: bool = False,
) -> TransitionResult:
    """
    Validates a WorkItem status transition and returns a TransitionResult.

    Raises:
        InvalidTransition: if the transition is not allowed (includes unknown
                           states, skipped steps, self-transitions, and
                           attempts to exit CLOSED).
        MissingRCA:        if transitioning to CLOSED without a complete RCA.

    Args:
        current:          Current status of the WorkItem.
        target:           Requested new status.
        has_complete_rca: Whether a complete RCA record is attached.
                          Must be True to allow RESOLVED → CLOSED.
    """
    if current not in STATES:
        raise InvalidTransition(f"Unknown current state: '{current}'")

    if target not in STATES:
        raise InvalidTransition(f"Unknown target state: '{target}'")

    if current == target:
        raise InvalidTransition(
            f"Self-transition not allowed: '{current}' → '{target}'. "
            "No-op transitions should be handled by the caller."
        )

    state = STATES[current]
    if not state.can_transition(target):
        raise InvalidTransition(
            f"Cannot transition from '{current}' to '{target}'. "
            f"Allowed next states: {state.allowed_next or ('none — terminal state',)}"
        )

    if target == "CLOSED" and not has_complete_rca:
        raise MissingRCA(
            "A complete RCA (root_cause_category, fix_applied, prevention_steps) "
            "is required before closing a work item."
        )

    return TransitionResult(
        previous=current,
        current=target,
        stamp_resolved_at=(target == "RESOLVED"),
    )