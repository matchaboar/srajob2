"""Convex database operations for filtering and looking up job URLs."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from temporalio import activity

from ....config import settings
from ..step import filter_existing_job_urls_step, filter_new_job_urls, lookup_job_id_for_url

logger = logging.getLogger("temporal.worker.activities")

# Re-export step functions for backwards compatibility
__all__ = [
    "filter_new_job_urls",
    "lookup_job_id_for_url",
    "filter_existing_job_urls",
    "compute_urls_to_scrape",
    "_looks_like_convex_id",
    "_convex_site_id",
    "_convex_http_base_url",
]


def _looks_like_convex_id(value: str) -> bool:
    """Check if a string looks like a valid Convex document ID."""
    return isinstance(value, str) and len(value) >= 26 and value.isalnum()


def _convex_site_id(value: Any) -> Optional[str]:
    """Return a Convex document id if the value looks valid, else None."""
    candidate = value.get("_id") if isinstance(value, dict) else value
    if isinstance(candidate, str) and _looks_like_convex_id(candidate):
        return candidate
    return None


def _convex_http_base_url() -> Optional[str]:
    """Get the Convex HTTP base URL from settings."""
    if settings.convex_http_url:
        return settings.convex_http_url.rstrip("/")
    return None


@activity.defn
def filter_existing_job_urls(urls: List[str]) -> List[str]:
    """Return the subset of URLs that already exist in Convex jobs table."""
    return filter_existing_job_urls_step(urls)


@activity.defn
async def compute_urls_to_scrape(
    job_urls: List[Any],
    existing_urls: List[str] | None = None,
) -> Dict[str, Any]:
    """Filter and diff URL lists to keep workflow CPU usage minimal."""
    cleaned = [u for u in job_urls if isinstance(u, str) and u.strip()]
    existing_list = [u for u in (existing_urls or []) if isinstance(u, str)]
    existing_set = set(existing_list)
    urls_to_scrape = [u for u in cleaned if u not in existing_set]

    return {
        "totalCount": len(cleaned),
        "existingCount": len(existing_set),
        "urlsToScrape": urls_to_scrape,
    }
