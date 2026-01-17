"""DBOS step function for filtering new job URLs."""

from __future__ import annotations

from typing import List

from dbos import DBOS


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def filter_new_job_urls(urls: List[str]) -> List[str]:
    """
    Return only URLs that do NOT exist in Convex jobs table.

    Returns:
        List[str]: New URLs that don't exist in DB
    """
    from ....services.convex_client import convex_query

    cleaned = [u for u in urls if isinstance(u, str) and u.strip()]
    if not cleaned:
        return []

    data = convex_query("router:filterNewJobUrls", {"urls": cleaned})

    new_urls = data.get("new", []) if isinstance(data, dict) else []
    if not isinstance(new_urls, list):
        return []

    return [u for u in new_urls if isinstance(u, str)]
