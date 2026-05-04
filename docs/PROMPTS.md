# Prompts, Specs & Architecture Decisions

This document contains the engineering spec, prompts, and key architecture
decisions made during the design and build of the Incident Management System.

---

## Original Engineering Spec

See: `docs/Engineering_Assignment__Incident_Management_System.pdf`

Key requirements extracted:

| Requirement | Implementation |
|---|---|
| High-throughput ingestion (10k signals/sec) | Redis Streams + pipeline batched xadd |
| Debounce: 1 WorkItem per 10s per component | Redis key + distributed lock in worker |
| Raw signal audit log | MongoDB `raw_signals` collection |
| Structured WorkItems + RCA | PostgreSQL with SQLAlchemy async |
| Real-time dashboard state | Redis Hash (O(1) upsert/delete) |
| MTTR calculation | `resolved_at - start_time` on → RESOLVED |
| Mandatory RCA before CLOSED | State machine gate in `state.py` |
| Alerting strategy per component type | Strategy pattern in `alerting.py` |
| State machine: OPEN → INVESTIGATING → RESOLVED → CLOSED | State pattern in `state.py` |
| Rate limiting on ingestion | Fixed-window Lua atomic in `rate_limit.py` |
| /health + throughput metrics every 5s | `/health` endpoint + worker `log_metrics()` |

---

## Architecture Prompts Used

### Prompt 1 — Initial Audit Request
```
You are a senior SRE + backend architect. I have already built a partial
Incident Management System (IMS). Audit the system, identify architectural
gaps, upgrade it to production-grade without overengineering, generate
missing code where needed, improve system reliability, scalability,
and observability.
```

### Prompt 2 — Specific Gap List
```
What is missing:
- Proper queue-based ingestion (Redis Streams not implemented fully)
- No clear debouncing logic (group signals within 10 seconds)
- Backpressure handling not explicitly designed
- No worker consumer group implementation
- No throughput metrics (signals/sec)
- No MTTR display
- Limited observability
- No load testing setup
```

### Prompt 3 — File-by-file audit
Each file was pasted individually for audit before rewriting:
- `ingest_api.py` → identified missing backpressure, stream_ids, shallow health
- `config.py` → identified hardcoded values, missing worker/retry config
- `rate_limit.py` → identified race condition between INCRBY and EXPIRE
- `schemas.py` → identified missing signal_id, stream_ids, resolved_at, signal_count
- `sql_models.py` → identified missing columns, wrong types, missing indexes
- `database.py` → identified missing pool limits, no shutdown hook
- `worker.py` → identified unbounded gather, no PEL recovery, no dead-letter
- `main.py` (api) → identified missing resolved_at stamp, shallow health, no pagination
- `alerting.py` → identified missing MCP, Queue, NoSQL, API strategies
- `dashboard.py` → identified missing signal_count, RESOLVED not removed
- `state.py` → identified missing TransitionResult, self-transition allowed

---

## Key Architecture Decisions

### Decision 1: Redis Streams over Kafka
**Why:** Kafka requires ZooKeeper/KRaft, complex setup for a single-team system.
Redis Streams provide consumer groups, PEL, acknowledgement, and MAXLEN
backpressure in a single dependency already used for caching.
**Trade-off:** Redis Streams don't support log compaction or long-term retention.
Acceptable because MongoDB is the permanent audit log.

### Decision 2: Debounce with Distributed Lock (not DB transaction)
**Why:** Pure DB transactions for debounce require `SELECT FOR UPDATE` which
serializes all workers on the same component. Redis lock is ~1ms vs ~10ms
for a Postgres round-trip, and releases immediately after WorkItem creation.
**Trade-off:** If Redis crashes between lock acquisition and WorkItem creation,
the lock key expires after `debounce_seconds` and a new WorkItem is created.
Acceptable — rare event, results in a duplicate incident, not data loss.

### Decision 3: MTTR = resolved_at - start_time (not end_time)
**Why:** `end_time` is when the RCA is submitted, which can be hours or days
after the incident was actually fixed. MTTR should measure time-to-fix,
not time-to-document. `resolved_at` is stamped the moment the operator
marks the incident RESOLVED.
**Trade-off:** If an operator marks RESOLVED incorrectly and then re-opens
(not possible in current state machine), MTTR would be wrong. Acceptable
because RESOLVED → OPEN is an invalid transition.

### Decision 4: Denormalized signal_count on WorkItem
**Why:** Counting signals from MongoDB on every dashboard refresh requires
a cross-database aggregation query. Storing a counter on the WorkItem
allows O(1) reads from the Redis dashboard cache.
**Trade-off:** Counter can drift if a MongoDB insert succeeds but the
subsequent Postgres UPDATE fails. Tenacity retries on both make this
extremely unlikely. Acceptable for a dashboard metric (not billing).

### Decision 5: Fixed-window rate limiter (not sliding window)
**Why:** Fixed-window is O(1) with a single Lua script. Sliding window
requires a sorted set with O(log N) operations per request.
At 10k signals/sec, the sliding window overhead is measurable.
**Trade-off:** Fixed window allows up to 2× limit at window boundaries.
Acceptable for ingestion where slight bursts are tolerable.

### Decision 6: Backpressure via 503 (not blocking)
**Why:** Blocking the ingest API thread while waiting for stream to drain
would tie up uvicorn workers. A 503 response lets the producer decide
how to back off (exponential retry, circuit breaker) without coupling
the ingest API to worker throughput.
**Trade-off:** Producers must implement retry logic. Acceptable — any
production HTTP client should handle 503 + Retry-After.

### Decision 7: PEL Recovery via XAUTOCLAIM (not manual XCLAIM)
**Why:** XAUTOCLAIM (Redis 6.2+) atomically transfers ownership of idle
PEL entries in a single command. Manual XCLAIM requires listing pending
messages first (XPENDING) then claiming each one (XCLAIM) — two round
trips with a race window.
**Trade-off:** Requires Redis 6.2+. Redis 7-alpine in docker-compose
satisfies this.

---

## Design Patterns Used

| Pattern | File | Implementation |
|---|---|---|
| **Strategy** | `alerting.py` | `AlertStrategy` ABC with `matches()` + `severity()`. Each component type is a separate strategy. First match wins. |
| **State Machine** | `state.py` | `IncidentState` frozen dataclass with `allowed_next` tuple. `validate_transition()` returns `TransitionResult`. |
| **Consumer Group** | `worker.py` | Redis `XREADGROUP` with named consumer group. Multiple workers share the stream load. |
| **Distributed Lock** | `worker.py` | Redis `SET NX EX` for debounce lock. Prevents duplicate WorkItem creation under concurrent signal bursts. |
| **Dead Letter Queue** | `worker.py` | `ims:signals:dead` stream. Messages that exhaust retries are moved here for inspection. |
| **Backpressure** | `ingest_api.py` | `XLEN >= threshold → 503`. Producers slow down when workers fall behind. |
| **Repository (implicit)** | `dashboard.py` | Encapsulates Redis Hash read/write logic. API layer never touches Redis keys directly. |

---

## File Placement Reference

```
backend/
  api/app/
    main.py              ← Core API (status, RCA, health, incidents)
  ingestion/app/
    main.py              ← Ingestion API (signal ingest, backpressure)
  models/ims/
    alerting.py          ← Strategy pattern for severity mapping
    config.py            ← Centralised settings (pydantic-settings)
    dashboard.py         ← Redis Hash cache for active incidents
    database.py          ← DB clients, pool config, lifespan, health checks
    mttr.py              ← Pure MTTR computation (no DB dependency)
    rate_limit.py        ← Atomic Lua fixed-window rate limiter
    schemas.py           ← Pydantic models (in/out/paginated)
    sql_models.py        ← SQLAlchemy ORM models
    state.py             ← State machine + TransitionResult
  workers/app/
    main.py              ← Consumer group worker
scripts/
  load_test.py           ← 5k-10k signals/sec async load test
sample-data/
  simulate_failure.py    ← RDBMS → MCP → Cache cascade simulation
tests/
  test_debounce.py       ← Alert strategy + debounce key tests
  test_state_and_rca.py  ← State machine + MTTR + RCA tests
docs/
  PROMPTS.md             ← This file
  README_UPGRADE.md      ← Production architecture docs
```
