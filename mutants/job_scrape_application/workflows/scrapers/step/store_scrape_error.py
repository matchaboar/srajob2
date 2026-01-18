"""DBOS step function for storing scrape errors via Convex action."""

from __future__ import annotations

from typing import Any, Dict, Optional

from dbos import DBOS


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def store_scrape_error_step(
    source_url: str,
    event: str,
    error: str,
    metadata: Optional[Dict[str, Any]] = None,
    raw_response_base64: Optional[str] = None,
) -> None:
    """Store a scrape error to Convex via action.

    This is used for storing invalid SpiderCloud responses for debugging.

    Args:
        source_url: The URL that was being scraped.
        event: The error event type.
        error: The error message.
        metadata: Optional metadata dict.
        raw_response_base64: Optional base64-encoded raw response.
    """
    from ....services.convex_client import convex_action

    payload: Dict[str, Any] = {
        "sourceUrl": source_url,
        "event": event,
        "error": error,
    }
    if metadata is not None:
        payload["metadata"] = metadata
    if raw_response_base64 is not None:
        payload["rawResponseBase64"] = raw_response_base64

    convex_action("router:storeScrapeError", payload)
