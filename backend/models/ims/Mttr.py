"""
MTTR — Mean Time To Repair
==========================

Responsibility: compute, nothing else.
  - No DB access here.
  - No Redis access here.
  - Pure functions only — easy to unit test, easy to reuse.

Where MTTR is stored:
  WorkItem.mttr_seconds (Float, Postgres)

Where MTTR is computed:
  1. Primary:  api/main.py → submit_rca()
               When operator submits RCA, end_time is known.
               mttr = resolved_at - start_time  (time to fix, not time to close)

  2. Secondary: api/main.py → update_status()
               When status → RESOLVED, resolved_at is stamped.
               mttr is computed immediately so dashboard shows it
               before RCA is submitted.

MTTR formula:
  resolved_at - start_time
  (NOT end_time - start_time, because end_time = RCA submission
   which can happen days after the incident was actually fixed)

Frontend integration:
  WorkItemOut.mttr_seconds (float | None) is already in the schema.
  Display as: format_mttr(seconds) → "4m 32s" or "1h 12m"
  Show None as "—" (incident not yet resolved).
"""

from datetime import datetime, timezone


# ── Core computation ──────────────────────────────────────────────────────────

def compute_mttr(start_time: datetime, resolved_at: datetime) -> float:
    """
    Compute MTTR in seconds between incident start and resolution.

    Args:
        start_time:  When the first signal arrived (WorkItem.start_time).
        resolved_at: When the incident was marked RESOLVED (WorkItem.resolved_at).

    Returns:
        Elapsed seconds as float. Always >= 0.
        Sub-second precision is preserved (e.g. 4.732 seconds).

    Raises:
        ValueError: if resolved_at is before start_time (data integrity issue).
    """
    # Normalize to UTC so tz-aware and tz-naive datetimes don't silently
    # produce wrong results.
    start = _to_utc(start_time)
    resolved = _to_utc(resolved_at)

    if resolved < start:
        raise ValueError(
            f"resolved_at ({resolved.isoformat()}) is before "
            f"start_time ({start.isoformat()}). Cannot compute MTTR."
        )

    return (resolved - start).total_seconds()


def compute_mttr_safe(
    start_time: datetime | None,
    resolved_at: datetime | None,
) -> float | None:
    """
    Safe variant — returns None instead of raising if either timestamp
    is missing or invalid.

    Use this in worker/API code where you want best-effort MTTR without
    crashing the request on bad data.
    """
    if start_time is None or resolved_at is None:
        return None
    try:
        return compute_mttr(start_time, resolved_at)
    except ValueError:
        return None


# ── Display formatting (used by frontend or logs) ─────────────────────────────

def format_mttr(seconds: float | None) -> str:
    """
    Format MTTR seconds into a human-readable string.

    Examples:
        None      → "—"
        0.5       → "< 1s"
        45.0      → "45s"
        132.0     → "2m 12s"
        3672.0    → "1h 1m"
        90000.0   → "1d 1h"

    Intended for log lines and API responses.
    Frontend can use its own formatter if needed.
    """
    if seconds is None:
        return "—"

    if seconds < 1:
        return "< 1s"

    total = int(seconds)
    days, remainder = divmod(total, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, secs = divmod(remainder, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs and not days:
        # Drop seconds once we're in hours+ range — not meaningful.
        parts.append(f"{secs}s")

    return " ".join(parts) if parts else "< 1s"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _to_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC-aware. Treats naive datetimes as UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)