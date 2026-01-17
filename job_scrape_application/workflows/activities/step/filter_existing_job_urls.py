"""DBOS step function for filtering existing job URLs."""

from __future__ import annotations

from typing import List

from dbos import DBOS


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def filter_existing_job_urls_step(urls: List[str]) -> List[str]:
    """Return the subset of URLs that already exist in Convex jobs table.

    Args:
        urls: List of job URLs to check.

    Returns:
        List of URLs that already exist in the database.
    """
    from ....services.convex_client import convex_query

    cleaned = [u for u in urls if isinstance(u, str) and u.strip()]
    if not cleaned:
        return []

    try:
        data = convex_query("router:findExistingJobUrls", {"urls": cleaned})
    except Exception:
        return []

    existing = data.get("existing", []) if isinstance(data, dict) else []
    if not isinstance(existing, list):
        return []

    return [u for u in existing if isinstance(u, str)]
