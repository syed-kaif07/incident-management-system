from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

Severity = Literal["P0", "P1", "P2", "P3", "P4"]
Status = Literal["OPEN", "INVESTIGATING", "RESOLVED", "CLOSED"]


class SignalIn(BaseModel):
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


class WorkItemOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    component_id: str
    status: Status
    severity: Severity
    start_time: datetime
    end_time: datetime | None = None
    mttr_seconds: int | None = None


class StatusUpdate(BaseModel):
    status: Status


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


class WorkItemDetail(WorkItemOut):
    rca: RCAOut | None = None
