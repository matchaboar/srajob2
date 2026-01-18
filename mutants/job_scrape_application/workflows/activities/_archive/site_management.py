"""Site management activities for leasing, completing, and failing sites."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from temporalio import activity

from ..step.site_management import (
    complete_site_step,
    fail_site_step,
    fetch_sites_step,
    lease_site_step,
)
from ..types import Site

logger = logging.getLogger("temporal.worker.activities")
scheduling_logger = logging.getLogger("temporal.scheduler")


def _strip_none_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys whose values are None so Convex does not receive nulls."""
    return {k: v for k, v in payload.items() if v is not None}


def _looks_like_convex_id(value: str) -> bool:
    """Check if a string looks like a valid Convex document ID."""
    return isinstance(value, str) and len(value) >= 26 and value.isalnum()


@activity.defn
def fetch_sites() -> List[Site]:
    """Fetch all enabled sites from Convex."""
    return fetch_sites_step()


@activity.defn
def lease_site(
    worker_id: str,
    lock_seconds: int = 300,
    site_type: Optional[str] = None,
    scrape_provider: Optional[str] = None,
) -> Optional[Site]:
    """Lease a site for scraping, locking it for the specified duration."""
    res = lease_site_step(
        worker_id=worker_id,
        lock_seconds=lock_seconds,
        site_type=site_type,
        scrape_provider=scrape_provider,
    )
    if res is None:
        return None
    try:
        scheduling_logger.info(
            "lease_site leased site_id=%s url=%s provider=%s manual_trigger_at=%s last_run_at=%s completed=%s failed=%s lock_expires_at=%s locked_by=%s",
            res.get("_id"),
            res.get("url"),
            res.get("scrapeProvider"),
            res.get("manualTriggerAt"),
            res.get("lastRunAt"),
            res.get("completed"),
            res.get("failed"),
            res.get("lockExpiresAt"),
            res.get("lockedBy"),
        )
    except Exception:
        # logging should not break leasing
        pass
    return res


@activity.defn
def complete_site(site_id: str) -> None:
    """Mark a site as completed after successful scraping."""
    complete_site_step(site_id)


@activity.defn
def fail_site(payload: Dict[str, Any]) -> None:
    """Mark a site as failed with an error message."""
    fail_site_step(payload)
