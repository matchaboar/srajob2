"""DBOS step function for fetching sites from Convex."""

from __future__ import annotations

from typing import List

from dbos import DBOS

from ...types import Site


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def fetch_sites_step() -> List[Site]:
    """Fetch all enabled sites from Convex.

    Returns:
        List of enabled sites from the router:listSites query.

    Raises:
        RuntimeError: If the response is not a list.
    """
    from .....services.convex_client import convex_query

    res = convex_query("router:listSites", {"enabledOnly": True})
    if not isinstance(res, list):
        raise RuntimeError(f"Unexpected sites payload: {res!r}")
    return res  # type: ignore[return-value]
