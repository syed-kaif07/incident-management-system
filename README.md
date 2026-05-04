# Incident Management System — Production Architecture

## Architecture Overview

```
                        ┌─────────────────────────────────────────────┐
                        │              PRODUCERS                       │
                        │  (services, monitors, synthetic load tests)  │
                        └──────────────────┬──────────────────────────┘
                                           │ POST /signals (batch)
                                           ▼
                        ┌─────────────────────────────────────────────┐
                        │           INGESTION API (FastAPI)            │
                        │  • Rate limit per client IP (fixed window)   │
                        │  • Backpressure: 503 if stream > threshold   │
                        │  • NEVER writes to DB directly               │
                        │  • xadd → Redis Stream (pipeline batched)    │
                        └──────────────────┬──────────────────────────┘
                                           │ XADD
                                           ▼
                        ┌─────────────────────────────────────────────┐
                        │         REDIS STREAM: ims:signals            │
                        │  • MAXLEN 2,000,000 (approximate trim)       │
                        │  • Backpressure threshold: 1,800,000         │
                        │  • Dead-letter: ims:signals:dead             │
                        └──────────────────┬──────────────────────────┘
                                           │ XREADGROUP
                          ┌────────────────┼────────────────┐
                          ▼                ▼                ▼
                   ┌────────────┐  ┌────────────┐  ┌────────────┐
                   │  Worker 1  │  │  Worker 2  │  │  Worker N  │
                   │ (consumer  │  │ (consumer  │  │ (consumer  │
                   │  group)    │  │  group)    │  │  group)    │
                   └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
                         │               │               │
                         └───────────────┼───────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    ▼                    ▼                     ▼
          ┌──────────────────┐  ┌──────────────┐   ┌─────────────────┐
          │   PostgreSQL     │  │   MongoDB    │   │     Redis       │
          │  Work Items      │  │ Raw Signals  │   │ Dashboard Hash  │
          │  RCA Records     │  │ (audit log)  │   │ Debounce Keys   │
          │  MTTR (float)    │  │              │   │ Rate Limit Bkts │
          └──────────────────┘  └──────────────┘   └─────────────────┘
                    ▲
                    │ PATCH /status, POST /rca
          ┌──────────────────┐
          │   CORE API       │
          │   (FastAPI)      │
          │  Status machine  │
          │  RCA validation  │
          │  MTTR compute    │
          └──────────────────┘
                    ▲
          ┌──────────────────┐
          │   FRONTEND       │
          │   (React)        │
          │  Live dashboard  │
          │  Incident detail │
          │  RCA form        │
          └──────────────────┘
```

---

## Data Flow

### Signal Ingestion (hot path)
```
Producer → POST /signals
  → rate_limit check (Redis, Lua atomic)
  → backpressure check (xlen vs threshold)
  → pipeline xadd × N signals
  → return 202 + stream_ids
```

### Worker Processing (async, decoupled)
```
xreadgroup (batch=100, block=1s)
  → for each message:
      decode SignalIn
      → debounce check (Redis GET debounce:component:{id})
          hit  → reuse work_item_id
          miss → acquire lock → check Postgres → create WorkItem
      → store raw signal (MongoDB)
      → increment signal_count (Postgres UPDATE atomic)
      → xack
  → PEL recovery every 30s (xautoclaim stale messages)
  → dead-letter after all retries exhausted
```

### Status Transition
```
PATCH /incidents/{id}/status
  → load WorkItem + RCA (selectinload)
  → validate_transition() → TransitionResult
  → on RESOLVED: stamp resolved_at, compute mttr_seconds
  → on CLOSED:   require complete RCA (MissingRCA gate)
  → commit Postgres
  → upsert/remove Redis dashboard hash
```

---

## Backpressure Handling

### Problem
When Postgres or MongoDB is slow, workers fall behind. Without backpressure,
producers keep pushing and the Redis Stream grows unbounded → OOM.

### Solution: Two-layer defence

**Layer 1 — Stream depth check (Ingestion API)**
```python
stream_depth = await redis.xlen(settings.signal_stream)
if stream_depth >= settings.backpressure_threshold:   # 1,800,000
    raise HTTPException(503, "ingestion_backpressure")
```
Producers receive 503 and must back off with exponential retry.
Stream drains as workers catch up → 503s stop → normal flow resumes.

**Layer 2 — MAXLEN trim (Stream)**
```python
xadd(..., maxlen=2_000_000, approximate=True)
```
Hard cap on stream size. Approximate trim avoids blocking on every write.
Old unprocessed messages are evicted if workers are catastrophically behind.

**Behaviour under DB slowness**
```
DB slow → workers slow → PEL grows → stream depth grows
→ depth > 1,800,000 → ingest returns 503
→ producers back off → stream depth stabilises
→ DB recovers → workers catch up → depth drops
→ 503s stop → producers resume
```

**PEL Recovery**
If a worker crashes mid-processing, messages sit in the Pending Entry List (PEL).
The `_recover_pel()` loop runs every 30s and uses `XAUTOCLAIM` to reassign
messages idle longer than 30s to the current healthy worker.

---

## Debouncing

**Goal:** 100 signals for `CACHE_CLUSTER_01` within 10 seconds → 1 WorkItem, 100 signals linked.

**Implementation:**
```
signal arrives for component X
  → GET debounce:component:X           # O(1) Redis lookup
      hit  → return cached work_item_id
      miss → SET lock:debounce:X NX EX 10   # distributed lock
               → query Postgres (recent WorkItem within 10s)
               → create WorkItem if none
               → SET debounce:component:X {id} EX 10
               → DEL lock:debounce:X
```

All signals within the 10s window share the same WorkItem.
New signals after the window create a new WorkItem.

---

## MTTR Calculation

**Formula:** `resolved_at - start_time` (not `end_time - start_time`)

- `start_time` = timestamp of first signal (set on WorkItem creation)
- `resolved_at` = when operator marks status → RESOLVED
- `end_time` = when RCA is submitted (can be days later, not counted)
- `mttr_seconds` = stored as `Float` in Postgres, returned in `WorkItemOut`

**Where it's computed:** `mttr.compute_mttr_safe()` called in `update_status()` on `→ RESOLVED`.

**Frontend:** `WorkItemOut.mttr_seconds` (float | None). Display with:
```javascript
const formatMTTR = (s) => {
  if (s == null) return "—";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.floor(s/60)}m ${Math.round(s%60)}s`;
  return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`;
};
```

---

## Scaling Strategy

| Component | How to scale | Notes |
|---|---|---|
| Ingestion API | `--workers N` (uvicorn) or multiple containers | Stateless, scales horizontally |
| Workers | `docker compose up --scale worker=N` | Set unique `WORKER_NAME` per replica |
| Redis | Single node sufficient to ~100k msg/s | Redis Cluster if needed beyond that |
| Postgres | Read replicas for dashboard queries | Writes go to primary only |
| MongoDB | Replica set for HA | Sharding if signals exceed 1TB |

**Worker scaling example:**
```bash
# Scale to 3 workers with unique names
WORKER_NAME=worker-1 docker compose up -d worker
WORKER_NAME=worker-2 docker compose up -d --scale worker=2
```

---

## Design Patterns Used

| Pattern | Where | Why |
|---|---|---|
| **Strategy** | `alerting.py` | Swap severity logic per component type without conditionals |
| **State Machine** | `state.py` | Enforce valid lifecycle transitions, reject illegal moves |
| **Consumer Group** | `worker.py` | Distributed processing with at-least-once delivery |
| **Debounce + Distributed Lock** | `worker.py` | Prevent duplicate WorkItems under concurrent signal bursts |
| **Dead Letter Queue** | `worker.py` | Isolate poison messages, don't block healthy processing |
| **Backpressure** | `ingest_api.py` | Protect DB layer from producer overload |
| **Repository (implicit)** | `dashboard.py` | Separate cache read/write logic from API handlers |
| **Circuit Breaker (partial)** | `database.py` health checks | Detect dependency failures before serving requests |

---

## Setup

```bash
# Start all services
docker compose up -d

# Scale workers
docker compose up -d --scale worker=3

# Run load test
python scripts/load_test.py --rate 5000 --duration 30

# Run tests
pytest tests/ -v
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection |
| `POSTGRES_DSN` | `postgresql+asyncpg://...` | Postgres connection |
| `MONGO_URL` | `mongodb://mongo:27017` | MongoDB connection |
| `RATE_LIMIT_PER_SECOND` | `500` | Per client IP |
| `DEBOUNCE_SECONDS` | `10` | Signal grouping window |
| `WORKER_BATCH_SIZE` | `100` | Messages per xreadgroup |
| `WORKER_CONCURRENCY` | `20` | Max concurrent DB sessions per worker |
| `BACKPRESSURE_THRESHOLD` | `1800000` | Stream depth for 503 |
| `MAX_STREAM_LEN` | `2000000` | Redis Stream MAXLEN |