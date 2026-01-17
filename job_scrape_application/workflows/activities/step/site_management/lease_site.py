"""DBOS step function for leasing a site from Convex."""

from __future__ import annotations

from typing import Any, Dict, Optional

from dbos import DBOS

from ...types import Site


def _strip_none_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys whose values are None so Convex does not receive nulls."""
    return {k: v for k, v in payload.items() if v is not None}


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def lease_site_step(
    worker_id: str,
    lock_seconds: int = 300,
    site_type: Optional[str] = None,
    scrape_provider: Optional[str] = None,
) -> Optional[Site]:
    """Lease a site for scraping, locking it for the specified duration.

    Args:
        worker_id: Identifier for the worker leasing the site.
        lock_seconds: Duration to lock the site (default 300s).
        site_type: Optional site type filter.
        scrape_provider: Optional provider filter.

    Returns:
        The leased site, or None if no site is available.

    Raises:
        RuntimeError: If the response is an unexpected type.
    """
    from .....services.convex_client import convex_mutation

    payload = _strip_none_values(
        {
            "workerId": worker_id,
            "lockSeconds": lock_seconds,
            "siteType": site_type,
            "scrapeProvider": scrape_provider,
        }
    )
    res = convex_mutation("router:leaseSite", payload)
    if res is None:
        return None
    if not isinstance(res, dict):
        raise RuntimeError(f"Unexpected lease payload: {res!r}")
    return res  # type: ignore[return-value]
