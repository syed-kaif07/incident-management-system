from datetime import datetime, timezone

import pytest

from ims.mttr import compute_mttr, compute_mttr_safe, format_mttr
from ims.schemas import RCAIn
from ims.state import InvalidTransition, MissingRCA, TransitionResult, validate_transition


# ── Valid transitions ─────────────────────────────────────────────────────────

def test_valid_full_lifecycle() -> None:
    validate_transition("OPEN", "INVESTIGATING")
    validate_transition("INVESTIGATING", "RESOLVED")
    validate_transition("RESOLVED", "CLOSED", has_complete_rca=True)


def test_valid_transition_returns_result() -> None:
    result = validate_transition("OPEN", "INVESTIGATING")
    assert isinstance(result, TransitionResult)
    assert result.previous == "OPEN"
    assert result.current == "INVESTIGATING"


# ── stamp_resolved_at ─────────────────────────────────────────────────────────

def test_stamp_resolved_at_true_only_on_resolved() -> None:
    result = validate_transition("INVESTIGATING", "RESOLVED")
    assert result.stamp_resolved_at is True


def test_stamp_resolved_at_false_on_other_transitions() -> None:
    r1 = validate_transition("OPEN", "INVESTIGATING")
    assert r1.stamp_resolved_at is False

    r2 = validate_transition("RESOLVED", "CLOSED", has_complete_rca=True)
    assert r2.stamp_resolved_at is False


# ── Invalid transitions ───────────────────────────────────────────────────────

def test_skip_open_to_resolved_rejected() -> None:
    with pytest.raises(InvalidTransition):
        validate_transition("OPEN", "RESOLVED")


def test_skip_open_to_closed_rejected() -> None:
    with pytest.raises(InvalidTransition):
        validate_transition("OPEN", "CLOSED", has_complete_rca=True)


def test_skip_investigating_to_closed_rejected() -> None:
    with pytest.raises(InvalidTransition):
        validate_transition("INVESTIGATING", "CLOSED", has_complete_rca=True)


def test_backward_transition_rejected() -> None:
    with pytest.raises(InvalidTransition):
        validate_transition("INVESTIGATING", "OPEN")


def test_resolved_to_open_rejected() -> None:
    with pytest.raises(InvalidTransition):
        validate_transition("RESOLVED", "OPEN")


# ── Terminal state ────────────────────────────────────────────────────────────

def test_closed_is_terminal_rejects_all() -> None:
    for target in ("OPEN", "INVESTIGATING", "RESOLVED", "CLOSED"):
        with pytest.raises(InvalidTransition):
            validate_transition("CLOSED", target)


# ── Unknown states ────────────────────────────────────────────────────────────

def test_unknown_current_state_raises() -> None:
    with pytest.raises(InvalidTransition, match="Unknown current state"):
        validate_transition("DELETED", "OPEN")


def test_unknown_target_state_raises() -> None:
    with pytest.raises(InvalidTransition, match="Unknown target state"):
        validate_transition("OPEN", "ARCHIVED")


# ── Self-transition ───────────────────────────────────────────────────────────

def test_self_transition_rejected() -> None:
    for state in ("OPEN", "INVESTIGATING", "RESOLVED"):
        with pytest.raises(InvalidTransition):
            validate_transition(state, state)


# ── RCA gate ──────────────────────────────────────────────────────────────────

def test_closed_requires_complete_rca() -> None:
    with pytest.raises(MissingRCA):
        validate_transition("RESOLVED", "CLOSED", has_complete_rca=False)


def test_closed_allowed_with_complete_rca() -> None:
    result = validate_transition("RESOLVED", "CLOSED", has_complete_rca=True)
    assert result.current == "CLOSED"


# ── RCA schema validation ─────────────────────────────────────────────────────

def test_rca_empty_root_cause_rejected() -> None:
    with pytest.raises(ValueError):
        RCAIn(root_cause_category="", fix_applied="patched", prevention_steps="add monitor")


def test_rca_empty_fix_applied_rejected() -> None:
    with pytest.raises(ValueError):
        RCAIn(root_cause_category="Capacity", fix_applied="", prevention_steps="add monitor")


def test_rca_empty_prevention_steps_rejected() -> None:
    with pytest.raises(ValueError):
        RCAIn(root_cause_category="Capacity", fix_applied="patched", prevention_steps="")


def test_rca_normalizes_naive_timestamp_to_utc() -> None:
    rca = RCAIn(
        root_cause_category="Capacity",
        fix_applied="Scaled primary pool",
        prevention_steps="Add predictive alert",
        submitted_at=datetime(2026, 5, 1, 12, 0),  # naive
    )
    assert rca.completed_at().tzinfo == timezone.utc


def test_rca_defaults_submitted_at_to_now_when_missing() -> None:
    rca = RCAIn(
        root_cause_category="Capacity",
        fix_applied="Scaled primary pool",
        prevention_steps="Add predictive alert",
    )
    completed = rca.completed_at()
    assert completed.tzinfo == timezone.utc
    assert (datetime.now(timezone.utc) - completed).total_seconds() < 5


# ── MTTR calculation ──────────────────────────────────────────────────────────

def test_mttr_30_minutes() -> None:
    start = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    resolved = datetime(2026, 5, 1, 10, 30, 0, tzinfo=timezone.utc)
    assert compute_mttr(start, resolved) == 1800.0


def test_mttr_sub_second_precision() -> None:
    from datetime import timedelta
    start = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    resolved = start + timedelta(seconds=4.732)
    assert abs(compute_mttr(start, resolved) - 4.732) < 0.001


def test_mttr_raises_when_resolved_before_start() -> None:
    start = datetime(2026, 5, 1, 10, 30, 0, tzinfo=timezone.utc)
    resolved = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="before start_time"):
        compute_mttr(start, resolved)


def test_mttr_safe_returns_none_on_missing_timestamps() -> None:
    assert compute_mttr_safe(None, None) is None
    assert compute_mttr_safe(datetime.now(timezone.utc), None) is None


def test_mttr_safe_returns_none_on_invalid() -> None:
    start = datetime(2026, 5, 1, 10, 30, 0, tzinfo=timezone.utc)
    resolved = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    assert compute_mttr_safe(start, resolved) is None


# ── MTTR formatting ───────────────────────────────────────────────────────────

def test_format_mttr_none() -> None:
    assert format_mttr(None) == "—"


def test_format_mttr_sub_second() -> None:
    assert format_mttr(0.5) == "< 1s"


def test_format_mttr_seconds() -> None:
    assert format_mttr(45.0) == "45s"


def test_format_mttr_minutes_and_seconds() -> None:
    assert format_mttr(132.0) == "2m 12s"


def test_format_mttr_hours_and_minutes() -> None:
    assert format_mttr(3672.0) == "1h 1m"


def test_format_mttr_days() -> None:
    assert format_mttr(90000.0) == "1d 1h"