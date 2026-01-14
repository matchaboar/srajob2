"""Convex database operations for filtering and looking up job URLs."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from temporalio import activity

from ...config import settings
from ..helpers.link_extractors import normalize_url
from ..site_handlers import get_site_handler

logger = logging.getLogger("temporal.worker.activities")


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


def _to_greenhouse_marketing_url(url: str) -> Optional[str]:
    """Convert Greenhouse API detail URL to the public marketing page."""
    from urllib.parse import urlparse

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    host = (parsed.hostname or "").lower()
    if "greenhouse.io" not in host:
        return None

    parts = [p for p in parsed.path.split("/") if p]
    # Expected API shape: /v1/boards/{slug}/jobs/{id}
    if len(parts) >= 5 and parts[0] == "v1" and parts[1] == "boards" and parts[3] == "jobs":
        slug = parts[2]
        job_id = parts[4]
        return f"https://boards.greenhouse.io/{slug}/jobs/{job_id}"

    return None


async def _lookup_job_id_for_url(url: str) -> Optional[str]:
    """Look up a job ID in Convex by URL, trying various URL formats."""
    from ...services.convex_client import convex_query

    candidates: List[str] = []
    seen: set[str] = set()

    def _add_candidate(value: str | None) -> None:
        if not isinstance(value, str) or not value.strip():
            return
        normalized = normalize_url(value) or value.strip()
        if normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    _add_candidate(url)

    handler = get_site_handler(url)
    if handler and handler.name == "greenhouse":
        _add_candidate(handler.get_api_uri(url))
        _add_candidate(_to_greenhouse_marketing_url(url))

    last_exc: Exception | None = None
    for candidate in candidates:
        try:
            result = await convex_query("jobs:getJobIdByUrl", {"url": candidate})
        except Exception as exc:
            logger.debug(
                "Job ID lookup failed for candidate %s: %s",
                candidate,
                exc,
                exc_info=exc,
            )
            last_exc = exc
            continue
        if result:
            return result

    if last_exc is not None:
        logger.warning(
            "Failed to lookup job id for %s (tried %d candidates): %s",
            url,
            len(candidates),
            last_exc,
            exc_info=last_exc,
        )
    return None


@activity.defn
async def filter_existing_job_urls(urls: List[str]) -> List[str]:
    """Return the subset of URLs that already exist in Convex jobs table."""
    cleaned = [u for u in urls if isinstance(u, str) and u.strip()]
    if not cleaned:
        return []
    from ...services.convex_client import convex_query

    try:
        data = await convex_query("router:findExistingJobUrls", {"urls": cleaned})
    except Exception:
        return []

    existing = data.get("existing", []) if isinstance(data, dict) else []
    if not isinstance(existing, list):
        return []

    return [u for u in existing if isinstance(u, str)]


async def filter_new_job_urls(urls: List[str]) -> List[str]:
    """
    Return only URLs that do NOT exist in Convex jobs table.

    More efficient than filter_existing_job_urls when most URLs already exist,
    as it returns only the new URLs (less network transfer).
    """
    cleaned = [u for u in urls if isinstance(u, str) and u.strip()]
    if not cleaned:
        return []
    from ...services.convex_client import convex_query

    try:
        data = await convex_query("router:filterNewJobUrls", {"urls": cleaned})
    except Exception:
        # Re-raise so caller can use fallback logic (assume all URLs are new)
        raise

    new_urls = data.get("new", []) if isinstance(data, dict) else []
    if not isinstance(new_urls, list):
        return []

    return [u for u in new_urls if isinstance(u, str)]


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
