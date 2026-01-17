"""DBOS step function for recording scrape URL attempts."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from dbos import DBOS

logger = logging.getLogger("temporal.worker.activities")


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def record_scrape_url_attempts(entries: List[Dict[str, Any]]) -> None:
    """Record scrape URL attempts to Convex for tracking.

    Fails fast with ConvexFunctionNotFoundError if the function doesn't exist,
    allowing the workflow to move on without unnecessary retry delays.
    """
    from ....services.convex_client import ConvexFunctionNotFoundError, convex_mutation

    if not entries:
        return

    try:
        convex_mutation("router:recordScrapeUrlAttempts", {"entries": entries})
    except ConvexFunctionNotFoundError:
        # Function doesn't exist - log and continue without blocking workflow
        logger.warning("router:recordScrapeUrlAttempts not deployed - skipping telemetry")
        return
    except Exception as exc:
        # Other errors (timeout, network) - log but don't block the workflow
        logger.warning("Failed to record scrape URL attempts: %s", exc)
        return
