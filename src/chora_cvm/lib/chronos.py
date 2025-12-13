"""
Domain: Chronos (Time)
ID Prefix: chronos.*

Time-related primitives for protocols that need temporal awareness.
All timestamps are in ISO 8601 format with UTC timezone.

Primitives:
  - chronos.now: Get current UTC timestamp
  - chronos.offset: Get timestamp offset from now
  - chronos.diff: Calculate difference between two timestamps
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from ..schema import ExecutionContext


def now(
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: chronos.now

    Get current UTC timestamp in ISO 8601 format.

    Args:
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {"status": "success", "timestamp": "2025-12-10T12:34:56.789000+00:00"}
    """
    return {
        "status": "success",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def offset(
    _ctx: ExecutionContext,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
    negate: bool = False,
) -> Dict[str, Any]:
    """
    Primitive: chronos.offset

    Get a timestamp offset from now.

    Args:
        _ctx: Execution context (MANDATORY in lib/)
        days: Number of days to offset
        hours: Number of hours to offset
        minutes: Number of minutes to offset
        seconds: Number of seconds to offset
        negate: If True, negate all values (useful for "7 days ago")

    Returns:
        {"status": "success", "timestamp": "2025-12-10T...", "offset_applied": {...}}

    Example:
        offset(_ctx, days=-7)  # 7 days ago
        offset(_ctx, days=7, negate=True)  # Also 7 days ago
    """
    if negate:
        days, hours, minutes, seconds = -days, -hours, -minutes, -seconds

    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    result = datetime.now(timezone.utc) + delta

    return {
        "status": "success",
        "timestamp": result.isoformat(),
        "offset_applied": {
            "days": days,
            "hours": hours,
            "minutes": minutes,
            "seconds": seconds,
        },
    }


def diff(
    timestamp_a: str,
    timestamp_b: str,
    _ctx: ExecutionContext,
) -> Dict[str, Any]:
    """
    Primitive: chronos.diff

    Calculate the difference between two ISO 8601 timestamps.

    Returns the difference as (a - b), so positive means a is later than b.

    Args:
        timestamp_a: First timestamp (ISO 8601)
        timestamp_b: Second timestamp (ISO 8601)
        _ctx: Execution context (MANDATORY in lib/)

    Returns:
        {
            "status": "success",
            "total_seconds": float,
            "days": int,
            "hours": int,
            "minutes": int,
            "seconds": float,
            "a_is_later": bool,
        }
        {"status": "error", "error": str} on parse failure
    """
    try:
        dt_a = datetime.fromisoformat(timestamp_a)
        dt_b = datetime.fromisoformat(timestamp_b)

        delta = dt_a - dt_b
        total_seconds = delta.total_seconds()

        # Break down into components
        abs_seconds = abs(total_seconds)
        days = int(abs_seconds // 86400)
        remaining = abs_seconds % 86400
        hours = int(remaining // 3600)
        remaining = remaining % 3600
        minutes = int(remaining // 60)
        seconds = remaining % 60

        return {
            "status": "success",
            "total_seconds": total_seconds,
            "days": days,
            "hours": hours,
            "minutes": minutes,
            "seconds": seconds,
            "a_is_later": total_seconds > 0,
        }
    except ValueError as e:
        return {
            "status": "error",
            "error": f"Invalid timestamp format: {e}",
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
