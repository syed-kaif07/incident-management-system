from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

Severity = Literal["P0", "P1", "P2", "P3", "P4"]
Status = Literal["OPEN", "INVESTIGATING", "RESOLVED", "CLOSED"]


# ── Ingestion ─────────────────────────────────────────────────────────────────

class SignalIn(BaseModel):
    # Client-generated trace ID — survives across retries, useful for dedup
    # and end-to-end tracing (ingest log → stream → worker → Mongo).
    signal_id: str = Field(default_factory=lambda: str(uuid4()))
    component_id: str = Field(min_length=1, max_length=200)
    timestamp: datetime
    severity: Severity
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class IngestResponse(BaseModel):
    accepted: int
    stream: str
    # Redis xadd returns one ID per message (format: "<ms>-<seq>").
    # Returning these lets clients trace exactly which stream entries
    # correspond to their signals — useful for debugging and load tests.
    stream_ids: list[str]


# ── Work Items ────────────────────────────────────────────────────────────────

class WorkItemOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    component_id: str
    status: Status
    severity: Severity
    start_time: datetime
    end_time: datetime | None = None
    resolved_at: datetime | None = None   # stamped when status → RESOLVED
    mttr_seconds: float | None = None     # float for sub-second precision
    signal_count: int = 0                 # denormalized counter, incremented by worker


class StatusUpdate(BaseModel):
    status: Status
    # Note: transition validation (no skipping, RCA gate) is enforced in
    # state.py → validate_transition(), not here. Schema only checks the
    # value is a known Status literal.


# ── RCA ───────────────────────────────────────────────────────────────────────

class RCAIn(BaseModel):
    root_cause_category: str = Field(min_length=1, max_length=120)
    fix_applied: str = Field(min_length=1, max_length=4000)
    prevention_steps: str = Field(min_length=1, max_length=4000)
    submitted_at: datetime | None = None

    def completed_at(self) -> datetime:
        value = self.submitted_at or datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class RCAOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    work_item_id: UUID
    root_cause_category: str
    fix_applied: str
    prevention_steps: str
    submitted_at: datetime


# ── Detail + Pagination ───────────────────────────────────────────────────────

class WorkItemDetail(WorkItemOut):
    rca: RCAOut | None = None


class PaginatedWorkItems(BaseModel):
    """
    Used by GET /incidents/active to prevent unbounded list responses.
    Default page_size = 50, max enforced at API layer.
    """
    items: list[WorkItemOut]
    total: int
    page: int
    page_size: int