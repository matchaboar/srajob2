"""DBOS step function for recording pending Firecrawl webhooks to Convex."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

from dbos import DBOS

from ...helpers.firecrawl import should_mock_convex_webhooks
from ..constants import FirecrawlJobKind

if TYPE_CHECKING:
    from ..types import Site

logger = logging.getLogger("dbos.step.firecrawl")


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=1.0, backoff_rate=2.0)
def record_pending_webhook_step(
    job: Dict[str, Any],
    site: "Site",
    webhook: Dict[str, Any],
    kind: FirecrawlJobKind | str,
) -> Optional[str]:
    """Insert a placeholder webhook row so missing callbacks can be recovered later.

    This step records a pending Firecrawl webhook event to Convex before the
    async scrape job completes. This allows recovery of webhooks that fail to
    arrive.

    Args:
        job: The Firecrawl job response containing jobId and statusUrl.
        site: The site configuration being scraped.
        webhook: The webhook configuration used for the scrape.
        kind: The type of Firecrawl job (GREENHOUSE_LISTING or SITE_CRAWL).

    Returns:
        The Convex document ID of the created webhook record, or None on error.
    """
    if should_mock_convex_webhooks():
        return f"mock-webhook-{int(time.time() * 1000)}"

    from ....services.convex_client import convex_mutation

    now_ms = int(time.time() * 1000)
    metadata = webhook.get("metadata") if isinstance(webhook.get("metadata"), dict) else {}
    payload: Dict[str, Any] = {
        "jobId": str(job.get("jobId") or job.get("id") or ""),
        "event": "pending",
        "status": "pending",
        "sourceUrl": site.get("url"),
        "siteId": site.get("_id"),
        "metadata": metadata,
        "payload": {"queuedAt": now_ms, "kind": kind},
        "receivedAt": now_ms,
    }

    # Add statusUrl if present (Convex optional strings must be omitted when null)
    status_url = job.get("statusUrl")
    if status_url is not None:
        payload["statusUrl"] = status_url

    try:
        res = convex_mutation("router:insertFirecrawlWebhookEvent", payload)
        if isinstance(res, str):
            return res
    except Exception as exc:
        logger.warning(
            "Failed to record pending Firecrawl webhook job_id=%s error=%s",
            payload.get("jobId"),
            exc,
        )
    return None
