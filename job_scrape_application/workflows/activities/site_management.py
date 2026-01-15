"""Site management activities for leasing, completing, and failing sites."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from temporalio import activity

from .types import Site

logger = logging.getLogger("temporal.worker.activities")
scheduling_logger = logging.getLogger("temporal.scheduler")


def _strip_none_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys whose values are None so Convex does not receive nulls."""
    return {k: v for k, v in payload.items() if v is not None}


def _looks_like_convex_id(value: str) -> bool:
    """Check if a string looks like a valid Convex document ID."""
    return isinstance(value, str) and len(value) >= 26 and value.isalnum()


@activity.defn
async def fetch_sites() -> List[Site]:
    """Fetch all enabled sites from Convex."""
    from ...services.convex_client import convex_query

    res = await convex_query("router:listSites", {"enabledOnly": True})
    if not isinstance(res, list):
        raise RuntimeError(f"Unexpected sites payload: {res!r}")
    return res  # type: ignore[return-value]


@activity.defn
async def lease_site(
    worker_id: str,
    lock_seconds: int = 300,
    site_type: Optional[str] = None,
    scrape_provider: Optional[str] = None,
) -> Optional[Site]:
    """Lease a site for scraping, locking it for the specified duration."""
    from ...services.convex_client import convex_mutation

    payload = _strip_none_values(
        {
            "workerId": worker_id,
            "lockSeconds": lock_seconds,
            "siteType": site_type,
            "scrapeProvider": scrape_provider,
        }
    )
    res = await convex_mutation("router:leaseSite", payload)
    if res is None:
        return None
    if not isinstance(res, dict):
        raise RuntimeError(f"Unexpected lease payload: {res!r}")
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
    return res  # type: ignore[return-value]


@activity.defn
async def complete_site(site_id: str) -> None:
    """Mark a site as completed after successful scraping."""
    from ...services.convex_client import ArgumentValidationError, convex_mutation

    if not _looks_like_convex_id(site_id):
        # Skip best-effort if id is not a Convex document id
        return

    try:
        await convex_mutation("router:completeSite", {"id": site_id})
    except ArgumentValidationError as exc:
        # Swallow validator errors for .id field so workflows continue
        if ".id" in str(exc):
            return
        raise


@activity.defn
async def fail_site(payload: Dict[str, Any]) -> None:
    """Mark a site as failed with an error message."""
    from ...services.convex_client import ArgumentValidationError, convex_mutation

    site_id = payload.get("id")
    if not isinstance(site_id, str) or not _looks_like_convex_id(site_id):
        return

    try:
        await convex_mutation("router:failSite", {"id": site_id, "error": payload.get("error")})
    except ArgumentValidationError as exc:
        # Swallow validator errors for .id field so workflows continue
        if ".id" in str(exc):
            return
        raise
