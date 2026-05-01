from datetime import datetime, timezone

import pytest

from ims.schemas import RCAIn
from ims.state import InvalidTransition, MissingRCA, validate_transition


def test_valid_state_progression_allows_ordered_transitions() -> None:
    validate_transition("OPEN", "INVESTIGATING")
    validate_transition("INVESTIGATING", "RESOLVED")
    validate_transition("RESOLVED", "CLOSED", has_complete_rca=True)


def test_state_pattern_rejects_skipped_transitions() -> None:
    with pytest.raises(InvalidTransition):
        validate_transition("OPEN", "RESOLVED")


def test_closed_requires_complete_rca() -> None:
    with pytest.raises(MissingRCA):
        validate_transition("RESOLVED", "CLOSED", has_complete_rca=False)


def test_rca_validation_requires_content() -> None:
    with pytest.raises(ValueError):
        RCAIn(root_cause_category="", fix_applied="patched", prevention_steps="add monitor")


def test_rca_normalizes_submission_time() -> None:
    rca = RCAIn(
        root_cause_category="Capacity",
        fix_applied="Scaled primary pool",
        prevention_steps="Add predictive alert",
        submitted_at=datetime(2026, 5, 1, 12, 0),
    )
    assert rca.completed_at().tzinfo == timezone.utc
