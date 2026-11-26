"""Time and interval helpers.

All business intervals are half-open: ``[start, until)``.  Input timestamps
must be ISO-8601 strings with an explicit UTC offset.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_dt(value: Any, *, field: str = "timestamp", required: bool = True) -> datetime | None:
    if value is None:
        if required:
            from .errors import ValidationError

            raise ValidationError(f"{field} is required", details={"field": field})
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    if not isinstance(value, str) or not value.strip():
        from .errors import ValidationError

        raise ValidationError(f"{field} must be an ISO-8601 timestamp", details={"field": field})
    text = value.strip()
    try:
        # Python accepts Z only from 3.11, but make this explicit anyway.
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        from .errors import ValidationError

        raise ValidationError(
            f"{field} must be an ISO-8601 timestamp with offset",
            details={"field": field, "value": value},
        ) from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        from .errors import ValidationError

        raise ValidationError(
            f"{field} must include a UTC offset",
            details={"field": field, "value": value},
        )
    return dt


def iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def now_dt() -> datetime:
    return datetime.now(timezone.utc)


def contains(start: datetime, until: datetime | None, at: datetime) -> bool:
    """Return whether ``at`` belongs to half-open ``[start, until)``."""
    if at < start:
        return False
    return until is None or at < until


def overlap_window(
    a_start: datetime,
    a_until: datetime | None,
    b_start: datetime,
    b_until: datetime | None,
) -> tuple[datetime, datetime | None] | None:
    """Return the half-open overlap, or ``None`` for adjacent/disjoint ranges."""
    start = max(a_start, b_start)
    ends = [dt for dt in (a_until, b_until) if dt is not None]
    until = min(ends) if ends else None
    if until is not None and start >= until:
        return None
    return start, until


def resolve_at(value: Any) -> datetime:
    if value is None:
        return now_dt()
    return parse_dt(value, field="at")  # type: ignore[return-value]
