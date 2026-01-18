"""DBOS step function for loading schedule interval from Convex."""

from __future__ import annotations

from dbos import DBOS

from ..sqlite import now_ms

DEFAULT_SCHEDULE_INTERVAL_MINUTES = 15
SCHEDULE_CONFIG_REFRESH_SECONDS = 600

_SCHEDULE_CACHE: tuple[int, dict[str, object]] | None = None


def _interval_from_config(config: dict[str, object]) -> int:
    interval = config.get("intervalMinutes")
    if isinstance(interval, (int, float)) and interval > 0:
        return int(interval)
    if config.get("mode") == "daily":
        return 24 * 60
    return DEFAULT_SCHEDULE_INTERVAL_MINUTES


def reset_schedule_cache() -> None:
    """Reset the schedule cache."""
    global _SCHEDULE_CACHE
    _SCHEDULE_CACHE = None


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def load_schedule_interval_minutes() -> int:
    """Load the schedule interval from Convex configuration."""
    from ...services.convex_client import convex_query

    global _SCHEDULE_CACHE
    now = now_ms()
    if _SCHEDULE_CACHE is not None:
        fetched_at, cached = _SCHEDULE_CACHE
        if now - fetched_at < SCHEDULE_CONFIG_REFRESH_SECONDS * 1000:
            return _interval_from_config(cached)

    try:
        config = convex_query("temporal:getScrapeSchedule", {})
    except Exception:
        return DEFAULT_SCHEDULE_INTERVAL_MINUTES
    if isinstance(config, dict):
        _SCHEDULE_CACHE = (now, config)
        return _interval_from_config(config)
    return DEFAULT_SCHEDULE_INTERVAL_MINUTES
