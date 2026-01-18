"""DBOS step functions for Firecrawl webhook operations."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from dbos import DBOS


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def fetch_pending_firecrawl_webhooks_step(
    limit: int = 25,
    event: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return unprocessed Firecrawl webhook rows from Convex.

    Args:
        limit: Maximum number of webhooks to return.
        event: Optional event type filter.

    Returns:
        List of pending webhook dictionaries.
    """
    from ....services.convex_client import convex_query

    args: Dict[str, Any] = {"limit": limit}
    if event:
        args["event"] = event
    res = convex_query("router:listPendingFirecrawlWebhooks", args)
    if not isinstance(res, list):
        return []
    return res  # type: ignore[return-value]


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def get_firecrawl_webhook_status_step(job_id: str) -> Dict[str, Any]:
    """Return the current Convex state for a Firecrawl job's webhook rows.

    Args:
        job_id: The Firecrawl job ID.

    Returns:
        Dict containing the webhook status, or empty dict on error.
    """
    from ....services.convex_client import convex_query

    try:
        res = convex_query("router:getFirecrawlWebhookStatus", {"jobId": job_id})
    except Exception:
        return {}
    return res if isinstance(res, dict) else {}


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def mark_firecrawl_webhook_processed_step(
    webhook_id: str,
    error: Optional[str] = None,
) -> None:
    """Mark a webhook row as processed and optionally attach an error.

    Args:
        webhook_id: The Convex webhook document ID.
        error: Optional error message to attach.
    """
    from ....services.convex_client import convex_mutation

    payload: Dict[str, Any] = {"id": webhook_id}
    if error is not None:
        payload["error"] = error

    convex_mutation(
        "router:markFirecrawlWebhookProcessed",
        payload,
    )
