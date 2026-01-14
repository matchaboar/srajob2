"""Timestamp parsing utilities for job scraping.

This module provides functions for parsing and normalizing posted_at timestamps
from various formats including ISO dates, relative times, and unix timestamps.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

# Regex for parsing relative time expressions like "3 days ago"
_RELATIVE_TIME_RE = re.compile(
    r"\b(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>seconds?|secs?|minutes?|mins?|hours?|hrs?|days?|weeks?|months?|years?)\b",
    flags=re.IGNORECASE,
)

# Regex to detect date-only strings (no time component)
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# MST timezone offset (UTC-7)
_MST_OFFSET = timezone(timedelta(hours=-7))

# Default time to use for date-only values (08:00 MST)
_DEFAULT_HOUR_MST = 8

# Minimum days threshold for relative posted times (helps filter noise)
_RELATIVE_POSTED_MIN_DAYS = 30


def _parse_relative_posted_at(value: str, now_ms: int) -> Optional[int]:
    """Parse relative time expressions like "3 days ago" or "yesterday".

    Args:
        value: The string containing the relative time expression
        now_ms: Current timestamp in milliseconds

    Returns:
        Timestamp in milliseconds, or None if the expression couldn't be parsed
    """
    lowered = value.lower()
    if "today" in lowered:
        return now_ms
    if "yesterday" in lowered:
        return now_ms - 86_400_000
    if "ago" not in lowered:
        return None

    match = _RELATIVE_TIME_RE.search(lowered)
    if not match:
        return None

    try:
        amount = float(match.group("value"))
    except ValueError:
        return None
    if amount < 0:
        return None
    if amount == 0:
        return now_ms

    unit = match.group("unit")
    if unit.startswith("day") and amount < _RELATIVE_POSTED_MIN_DAYS:
        # Allow smaller ranges when the value explicitly looks like a posted/updated label.
        if "posted" not in lowered and "updated" not in lowered:
            return None
    if unit.startswith(("second", "sec")):
        multiplier = 1
    elif unit.startswith(("minute", "min")):
        multiplier = 60
    elif unit.startswith(("hour", "hr")):
        multiplier = 3_600
    elif unit.startswith("day"):
        multiplier = 86_400
    elif unit.startswith("week"):
        multiplier = 604_800
    elif unit.startswith("month"):
        multiplier = 2_592_000
    elif unit.startswith("year"):
        multiplier = 31_536_000
    else:
        return None

    delta_ms = int(amount * multiplier * 1000)
    return max(0, now_ms - delta_ms)


def parse_posted_at(value: Any, now_ms: int | None = None) -> int:
    """Parse a posted_at timestamp from various formats.

    Handles:
    - Unix timestamps (milliseconds or seconds)
    - ISO 8601 date strings
    - Relative time expressions ("3 days ago", "yesterday", etc.)

    Args:
        value: The timestamp value to parse
        now_ms: Current timestamp in milliseconds (defaults to current time)

    Returns:
        Timestamp in milliseconds. Returns now_ms if parsing fails.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    if value is None:
        return now_ms

    if isinstance(value, (int, float)):
        if value > 1e12:
            return int(value)
        if value > 1e9:
            return int(value * 1000)
        return now_ms

    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            relative = _parse_relative_posted_at(cleaned, now_ms)
            if relative is not None:
                return relative
        if re.search(r"[+-]\d{4}$", cleaned):
            cleaned = cleaned[:-5] + cleaned[-5:-2] + ":" + cleaned[-2:]
        try:
            # Check if this is a date-only string (no time component)
            if _DATE_ONLY_RE.match(cleaned):
                # Parse as date and set time to 08:00 MST
                dt = datetime.fromisoformat(cleaned)
                dt = dt.replace(hour=_DEFAULT_HOUR_MST, tzinfo=_MST_OFFSET)
            else:
                dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            pass

    return now_ms


# Maximum days in future before a date is considered invalid (1 day for timezone buffer)
_MAX_FUTURE_DAYS = 1


def parse_posted_at_with_unknown(
    value: Any,
    now_ms: int | None = None,
    *,
    max_age_days: int | None = None,
) -> tuple[int, bool]:
    """Parse a posted_at timestamp and indicate if the value was unknown.

    Similar to parse_posted_at but returns a tuple indicating whether the
    parsed value was actually extracted from the input or defaulted to now.

    Args:
        value: The timestamp value to parse
        now_ms: Current timestamp in milliseconds (defaults to current time)
        max_age_days: If set, treats timestamps older than this as unknown

    Returns:
        Tuple of (timestamp_ms, is_unknown). is_unknown is True if the value
        couldn't be parsed, was older than max_age_days, or is in the future.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    max_future_ms = int(_MAX_FUTURE_DAYS) * 86_400_000

    def _is_future(ts: int) -> bool:
        """Check if timestamp is too far in the future."""
        return ts > now_ms + max_future_ms

    if value is None:
        return now_ms, True

    if isinstance(value, (int, float)):
        if value > 1e12:
            ts = int(value)
            if _is_future(ts):
                return now_ms, True
            return ts, False
        if value > 1e9:
            ts = int(value * 1000)
            if _is_future(ts):
                return now_ms, True
            return ts, False
        return now_ms, True

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return now_ms, True
        relative = _parse_relative_posted_at(cleaned, now_ms)
        if relative is not None:
            posted_at = relative
            if _is_future(posted_at):
                return now_ms, True
            if max_age_days is not None:
                max_age_ms = int(max_age_days) * 86_400_000
                if posted_at < now_ms - max_age_ms:
                    return now_ms, True
            return posted_at, False
        if re.search(r"[+-]\d{4}$", cleaned):
            cleaned = cleaned[:-5] + cleaned[-5:-2] + ":" + cleaned[-2:]
        try:
            # Check if this is a date-only string (no time component)
            if _DATE_ONLY_RE.match(cleaned):
                # Parse as date and set time to 08:00 MST
                dt = datetime.fromisoformat(cleaned)
                dt = dt.replace(hour=_DEFAULT_HOUR_MST, tzinfo=_MST_OFFSET)
            else:
                dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            posted_at = int(dt.timestamp() * 1000)
            if _is_future(posted_at):
                return now_ms, True
            if max_age_days is not None:
                max_age_ms = int(max_age_days) * 86_400_000
                if posted_at < now_ms - max_age_ms:
                    return now_ms, True
            return posted_at, False
        except Exception:
            return now_ms, True

    return now_ms, True


__all__ = [
    # Constants
    "_RELATIVE_TIME_RE",
    "_RELATIVE_POSTED_MIN_DAYS",
    # Functions
    "_parse_relative_posted_at",
    "parse_posted_at",
    "parse_posted_at_with_unknown",
]
