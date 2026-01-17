"""DBOS step function for fetching enabled sites from Convex."""

from __future__ import annotations

from dbos import DBOS

from ....services.convex_client import convex_query


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=1.0, backoff_rate=2.0)
def fetch_enabled_sites_step() -> list[dict[str, object]]:
    """Fetch all enabled sites from Convex.

    This step queries Convex to get the list of enabled sites that should
    be scraped. Used by the scheduled listing workflow.

    Returns:
        List of site dicts containing url, type, _id, paginationLimit, etc.
    """
    fetched = convex_query("router:listSites", {"enabledOnly": True})

    if not isinstance(fetched, list):
        return []

    return [site for site in fetched if isinstance(site, dict)]
