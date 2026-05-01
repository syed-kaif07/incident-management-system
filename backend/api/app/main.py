from datetime import timezone
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ims.config import get_settings
from ims.dashboard import list_active_incidents, remove_active_incident, upsert_active_incident
from ims.database import get_session, init_postgres, mongo_db, redis_client
from ims.schemas import RCAIn, RCAOut, StatusUpdate, WorkItemDetail, WorkItemOut
from ims.sql_models import RCA, WorkItem
from ims.state import InvalidTransition, MissingRCA, validate_transition

settings = get_settings()

app = FastAPI(title="IMS Core API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup() -> None:
    await init_postgres()


async def get_redis() -> Redis:
    return redis_client


async def _get_work_item(session: AsyncSession, work_item_id: UUID) -> WorkItem:
    result = await session.execute(
        select(WorkItem).options(selectinload(WorkItem.rca)).where(WorkItem.id == work_item_id)
    )
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="work item not found")
    return item


def _rca_is_complete(rca: RCA | None) -> bool:
    return bool(
        rca
        and rca.root_cause_category.strip()
        and rca.fix_applied.strip()
        and rca.prevention_steps.strip()
    )


@app.get("/health")
async def health(redis: Annotated[Redis, Depends(get_redis)]) -> dict[str, str]:
    await redis.ping()
    await mongo_db.command("ping")
    return {"status": "ok", "service": "api"}


@app.get("/incidents/active")
async def active_incidents(redis: Annotated[Redis, Depends(get_redis)]) -> list[dict]:
    return await list_active_incidents(redis)


@app.get("/incidents/{work_item_id}", response_model=WorkItemDetail)
async def incident_detail(work_item_id: UUID, session: Annotated[AsyncSession, Depends(get_session)]) -> WorkItem:
    return await _get_work_item(session, work_item_id)


@app.get("/incidents/{work_item_id}/signals")
async def incident_signals(work_item_id: UUID, limit: int = 200) -> list[dict]:
    cursor = (
        mongo_db.raw_signals.find({"work_item_id": str(work_item_id)}, {"_id": False})
        .sort("timestamp", 1)
        .limit(min(max(limit, 1), 1000))
    )
    return [doc async for doc in cursor]


@app.patch("/incidents/{work_item_id}/status", response_model=WorkItemOut)
async def update_status(
    work_item_id: UUID,
    request: StatusUpdate,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> WorkItem:
    item = await _get_work_item(session, work_item_id)
    try:
        validate_transition(item.status, request.status, has_complete_rca=_rca_is_complete(item.rca))
    except MissingRCA as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except InvalidTransition as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    item.status = request.status
    await session.commit()
    await session.refresh(item)
    await upsert_active_incident(redis, item)
    return item


@app.post("/incidents/{work_item_id}/rca", response_model=RCAOut, status_code=status.HTTP_201_CREATED)
async def submit_rca(
    work_item_id: UUID,
    request: RCAIn,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> RCA:
    item = await _get_work_item(session, work_item_id)
    submitted_at = request.completed_at()
    start_time = item.start_time
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    if item.rca:
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

    item.end_time = submitted_at
    item.mttr_seconds = max(0, int((submitted_at - start_time).total_seconds()))
    await session.commit()
    await session.refresh(rca)
    await session.refresh(item)
    await upsert_active_incident(redis, item)
    return rca


@app.post("/incidents/{work_item_id}/close", response_model=WorkItemOut)
async def close_incident(
    work_item_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> WorkItem:
    item = await _get_work_item(session, work_item_id)
    try:
        validate_transition(item.status, "CLOSED", has_complete_rca=_rca_is_complete(item.rca))
    except MissingRCA as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except InvalidTransition as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    item.status = "CLOSED"
    await session.commit()
    await session.refresh(item)
    await remove_active_incident(redis, item.id)
    return item
