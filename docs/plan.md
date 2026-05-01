# Production IMS Build Plan

This repository implements the requested assignment: a Dockerized Incident Management System with separate FastAPI ingestion, FastAPI incident API, Redis Stream workers, MongoDB raw signal storage, PostgreSQL work items/RCA, Redis cache/debounce, and a React dashboard.

The implemented architecture follows the submitted plan:

- Ingestion API writes only to Redis Streams.
- Workers asynchronously persist raw signals, debounce work item creation, and update dashboard cache.
- PostgreSQL is the source of truth for work items and RCA.
- MongoDB stores immutable raw signal audit records.
- Redis owns the queue, rate-limit state, debounce TTL keys, and active incident dashboard cache.
- State Pattern enforces incident transitions.
- Strategy Pattern maps component failures to alert severity.
