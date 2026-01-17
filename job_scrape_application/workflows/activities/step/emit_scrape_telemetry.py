"""DBOS step function for emitting scrape telemetry events."""

from __future__ import annotations

from typing import Any

from dbos import DBOS

from ....services import telemetry


@DBOS.step(retries_allowed=True, max_attempts=2, interval_seconds=0.5, backoff_rate=1.0)
def emit_scrape_telemetry_step(
    event: str,
    level: str,
    site_url: str,
    data: dict[str, Any] | None = None,
) -> None:
    """Emit a telemetry event to PostHog.

    This step sends scrape-related events to PostHog for monitoring and
    alerting. Failures are logged but do not block workflow execution.

    Args:
        event: Event name (e.g., "scrape.listing.zero_urls")
        level: Log level ("info", "warn", "error")
        site_url: The site URL for context
        data: Additional event data
    """
    payload: dict[str, Any] = {
        "event": event,
        "level": level,
        "siteUrl": site_url,
    }
    if data:
        payload["data"] = data

    try:
        telemetry.emit_posthog_log(payload)
    except Exception:
        # Telemetry failures should not block workflows
        pass


@DBOS.step(retries_allowed=True, max_attempts=2, interval_seconds=0.5, backoff_rate=1.0)
def emit_scrape_exception_step(
    exception: Exception,
    properties: dict[str, Any] | None = None,
) -> None:
    """Emit an exception event to PostHog.

    This step sends exception details to PostHog for error tracking.
    Failures are logged but do not block workflow execution.

    Args:
        exception: The exception to report
        properties: Additional context properties
    """
    try:
        telemetry.emit_posthog_exception(exception, properties=properties)
    except Exception:
        # Telemetry failures should not block workflows
        pass
