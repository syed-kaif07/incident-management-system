"""
IMS Core API
============
Handles the incident workflow: status transitions, RCA submission, signals view.

Key changes from v0.1:
  - lifespan replaces deprecated @app.on_event
  - /health is deep: pings all dependencies + stream depth
  - update_status stamps resolved_at + computes mttr_seconds on → RESOLVED
  - /incidents/active is paginated (prevents unbounded Redis hvals response)
  - close_incident merged into update_status (no duplicate CLOSED logic)
  - validate_transition returns TransitionResult (stamp_resolved_at signal)
"""

from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ims.config import get_settings
from ims.dashboard import list_active_incidents, remove_active_incident, upsert_active_incident
from ims.database import check_mongo, check_postgres, check_redis, get_session, lifespan, mongo_db, redis_client
from ims.mttr import compute_mttr_safe, format_mttr
from ims.schemas import PaginatedWorkItems, RCAIn, RCAOut, StatusUpdate, WorkItemDetail, WorkItemOut
from ims.sql_models import RCA, WorkItem
from ims.state import InvalidTransition, MissingRCA, validate_transition

settings = get_settings()

app = FastAPI(title="IMS Core API", version="0.2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Dependencies ──────────────────────────────────────────────────────────────

async def get_redis() -> Redis:
    return redis_client


async def _get_work_item(session: AsyncSession, work_item_id: UUID) -> WorkItem:
    result = await session.execute(
        select(WorkItem)
        .options(selectinload(WorkItem.rca))
        .where(WorkItem.id == work_item_id)
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"work item {work_item_id} not found",
        )
    return item


def _rca_is_complete(rca: RCA | None) -> bool:
    return bool(
        rca
        and rca.root_cause_category.strip()
        and rca.fix_applied.strip()
        and rca.prevention_steps.strip()
    )


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health(redis: Annotated[Redis, Depends(get_redis)]) -> dict:
    """
    Deep health check — verifies all three dependencies and reports
    Redis stream depth so ops can spot worker lag without SSH-ing in.

    Returns 200 in all cases (including degraded) so load balancers
    keep routing. Callers must inspect the 'status' field.
    """
    redis_ok = await check_redis()
    postgres_ok = await check_postgres()
    mongo_ok = await check_mongo()

    stream_depth: int | None = None
    if redis_ok:
        try:
            stream_depth = await redis.xlen(settings.signal_stream)
        except Exception:
            pass

    overall = "ok" if (redis_ok and postgres_ok and mongo_ok) else "degraded"

    return {
        "status": overall,
        "service": "api",
        "dependencies": {
            "redis": "ok" if redis_ok else "error",
            "postgres": "ok" if postgres_ok else "error",
            "mongo": "ok" if mongo_ok else "error",
        },
        "stream": {
            "depth": stream_depth,
            "backpressure_threshold": settings.backpressure_threshold,
        },
    }


# ── Incidents ─────────────────────────────────────────────────────────────────

@app.get("/incidents/active", response_model=PaginatedWorkItems)
async def active_incidents(
    redis: Annotated[Redis, Depends(get_redis)],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> PaginatedWorkItems:
    """
    Return active incidents sorted by severity then start_time.
    Paginated to prevent unbounded responses at scale.

    Source: Redis Hash (hot path — no Postgres query).
    """
    all_items = await list_active_incidents(redis, limit=10_000)
    total = len(all_items)
    start = (page - 1) * page_size
    end = start + page_size
    return PaginatedWorkItems(
        items=all_items[start:end],
        total=total,
        page=page,
        page_size=page_size,
    )


@app.get("/incidents/{work_item_id}", response_model=WorkItemDetail)
async def incident_detail(
    work_item_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> WorkItem:
    return await _get_work_item(session, work_item_id)


@app.get("/incidents/{work_item_id}/signals")
async def incident_signals(
    work_item_id: UUID,
    limit: int = Query(default=200, ge=1, le=1000),
) -> list[dict]:
    cursor = (
        mongo_db.raw_signals.find(
            {"work_item_id": str(work_item_id)},
            {"_id": False},
        )
        .sort("timestamp", 1)
        .limit(limit)
    )
    return [doc async for doc in cursor]


# ── Status transition ─────────────────────────────────────────────────────────

@app.patch("/incidents/{work_item_id}/status", response_model=WorkItemOut)
async def update_status(
    work_item_id: UUID,
    request: StatusUpdate,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> WorkItem:
    """
    Transition a WorkItem through its lifecycle:
      OPEN → INVESTIGATING → RESOLVED → CLOSED

    On → RESOLVED:
      - resolved_at is stamped (used as MTTR numerator)
      - mttr_seconds is computed immediately
      - Dashboard cache is updated

    On → CLOSED:
      - RCA must be complete (validated by state machine)
      - Incident is removed from active dashboard cache

    Self-transitions and state skips are rejected (422).
    Missing RCA on CLOSED is rejected (409).
    """
    item = await _get_work_item(session, work_item_id)

    try:
        result = validate_transition(
            item.status,
            request.status,
            has_complete_rca=_rca_is_complete(item.rca),
        )
    except MissingRCA as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except InvalidTransition as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    item.status = request.status

    # ── Side effects on RESOLVED ──────────────────────────────────────────────
    if result.stamp_resolved_at:
        now = datetime.now(timezone.utc)
        item.resolved_at = now
        item.mttr_seconds = compute_mttr_safe(item.start_time, now)
        import logging
        logging.getLogger("ims.api").info(
            "resolved work_item=%s mttr=%s",
            item.id,
            format_mttr(item.mttr_seconds),
        )

    await session.commit()
    await session.refresh(item)

    # ── Cache update ──────────────────────────────────────────────────────────
    if item.status == "CLOSED":
        await remove_active_incident(redis, item.id)
    else:
        await upsert_active_incident(redis, item)

    return item


# ── RCA ───────────────────────────────────────────────────────────────────────

@app.post(
    "/incidents/{work_item_id}/rca",
    response_model=RCAOut,
    status_code=status.HTTP_201_CREATED,
)
async def submit_rca(
    work_item_id: UUID,
    request: RCAIn,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> RCA:
    """
    Submit or update the RCA for a WorkItem.

    - Sets end_time to RCA submission time (when operator closed the loop).
    - Does NOT overwrite mttr_seconds — that was set when status → RESOLVED.
      MTTR measures time-to-fix, not time-to-document.
    - Updates dashboard cache so frontend shows updated end_time.
    """
    item = await _get_work_item(session, work_item_id)
    submitted_at = request.completed_at()

    start_time = item.start_time
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    if item.rca:
        # Update existing RCA (idempotent re-submission allowed).
        rca = item.rca
        rca.root_cause_category = request.root_cause_category
        rca.fix_applied = request.fix_applied
        rca.prevention_steps = request.prevention_steps
        rca.submitted_at = submitted_at
    else:
        rca = RCA(
            work_item_id=item.id,
            root_cause_category=request.root_cause_category,
            fix_applied=request.fix_applied,
            prevention_steps=request.prevention_steps,
            submitted_at=submitted_at,
        )
        session.add(rca)

    # end_time = when RCA was submitted (may be days after resolution).
    item.end_time = submitted_at

    # mttr_seconds: set only if not already set (resolved_at path in update_status).
    # Fallback: compute from end_time if operator submits RCA without going
    # through RESOLVED state explicitly (e.g. direct CLOSED via API).
    if item.mttr_seconds is None:
        item.mttr_seconds = compute_mttr_safe(start_time, submitted_at)

    await session.commit()
    await session.refresh(rca)
    await session.refresh(item)
    await upsert_active_incident(redis, item)
    return rca