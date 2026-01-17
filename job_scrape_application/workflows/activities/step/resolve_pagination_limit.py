"""DBOS step function for resolving pagination limit from Convex."""

from __future__ import annotations

from typing import Optional

from dbos import DBOS


def _looks_like_convex_id(value: str) -> bool:
    """Check if a string looks like a valid Convex document ID."""
    return isinstance(value, str) and len(value) >= 26 and value.isalnum()


def _convex_site_id(value) -> Optional[str]:
    """Return a Convex document id if the value looks valid, else None."""
    candidate = value.get("_id") if isinstance(value, dict) else value
    if isinstance(candidate, str) and _looks_like_convex_id(candidate):
        return candidate
    return None


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def resolve_pagination_limit_step(site_id: str) -> Optional[int]:
    """Resolve pagination limit for a site from Convex.

    Args:
        site_id: The Convex document ID of the site.

    Returns:
        The pagination limit if set, 0 if explicitly disabled, or None if not found.
    """
    from ....services.convex_client import convex_query

    try:
        site = convex_query("router:getSiteById", {"id": site_id})
    except Exception:
        return None

    if isinstance(site, dict):
        site_limit = site.get("paginationLimit")
        if isinstance(site_limit, (int, float)):
            site_limit_int = int(site_limit)
            if site_limit_int <= 0:
                return 0
            return site_limit_int

    return None
