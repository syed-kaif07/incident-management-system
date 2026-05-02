from datetime import datetime
from uuid import UUID as PythonUUID
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


# ── WorkItem ──────────────────────────────────────────────────────────────────

class WorkItem(Base):
    __tablename__ = "work_items"

    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    component_id: Mapped[str] = mapped_column(
        String(200), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="OPEN"
    )
    severity: Mapped[str] = mapped_column(
        String(8), nullable=False
    )

    # ── Timestamps ────────────────────────────────────────────────────────────
    start_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        # Stamped when status transitions to RESOLVED (not when RCA is submitted).
        # Used as the numerator for MTTR: resolved_at - start_time.
        DateTime(timezone=True), nullable=True
    )
    end_time: Mapped[datetime | None] = mapped_column(
        # Stamped when RCA is submitted (may differ from resolved_at).
        DateTime(timezone=True), nullable=True
    )

    # ── MTTR ──────────────────────────────────────────────────────────────────
    # Float for sub-second precision.
    # Formula: (resolved_at - start_time).total_seconds()
    # Computed in mttr.py, stored here for dashboard queries.
    mttr_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── Denormalized signal counter ───────────────────────────────────────────
    # Incremented by worker on every store_raw_signal() call.
    # Avoids a Mongo count query on every dashboard render.
    signal_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # ── Audit timestamps (DB-side defaults, not Python-side) ──────────────────
    # server_default=func.now() means the DB sets this at INSERT time,
    # not Python at object-creation time. More accurate under async load.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # ── Relationship ──────────────────────────────────────────────────────────
    rca: Mapped["RCA | None"] = relationship(
        back_populates="work_item",
        uselist=False,
        cascade="all, delete-orphan",
    )

    # ── Indexes ───────────────────────────────────────────────────────────────
    __table_args__ = (
        # Most common dashboard query: active incidents for a component.
        Index("ix_work_items_component_status", "component_id", "status"),
        # MTTR time-range queries and signal timeline queries.
        Index("ix_work_items_start_time", "start_time"),
        # Active incident feed sorted by severity.
        Index("ix_work_items_severity_status", "severity", "status"),
    )


# ── RCA ───────────────────────────────────────────────────────────────────────

class RCA(Base):
    __tablename__ = "rcas"

    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    work_item_id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("work_items.id"),
        unique=True,   # enforces 1 RCA per WorkItem at DB level
        index=True,
    )
    root_cause_category: Mapped[str] = mapped_column(String(120), nullable=False)
    fix_applied: Mapped[str] = mapped_column(Text, nullable=False)
    prevention_steps: Mapped[str] = mapped_column(Text, nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    work_item: Mapped[WorkItem] = relationship(back_populates="rca")