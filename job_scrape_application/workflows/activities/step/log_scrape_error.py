"""DBOS step function for logging scrape errors to Convex."""

from __future__ import annotations

from dbos import DBOS

from ....services import telemetry
from ..errors import ScrapeErrorInput, clean_scrape_error_payload


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def log_scrape_error(payload: ScrapeErrorInput) -> None:
    """Persist scrape/HTTP errors to Convex for audit visibility."""
    from ....services.convex_client import convex_mutation

    data = clean_scrape_error_payload(payload)
    try:
        convex_mutation("router:insertScrapeError", data)
    except Exception:
        # Best-effort; do not raise
        pass

    try:
        telemetry.emit_posthog_log(
            {
                "event": "scrape.error",
                "level": "error",
                "siteUrl": data.get("sourceUrl", ""),
                "data": data,
            }
        )
    except Exception:
        # Best-effort; do not raise
        return
