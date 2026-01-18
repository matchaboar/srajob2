"""DBOS step function for scraping job detail URLs via SpiderCloud."""

from __future__ import annotations

from typing import Any

from dbos import DBOS

from ....config import runtime_config
from ...scrapers import SpiderCloudScraper
from ..factories import build_spidercloud_scraper
from ...helpers.provider import (
    build_request_snapshot,
    log_provider_dispatch,
    log_sync_response,
    mask_secret,
    sanitize_headers,
)
from ...helpers.scrape_utils import trim_scrape_for_convex


def _make_scraper() -> SpiderCloudScraper:
    """Create a configured SpiderCloud scraper instance."""
    return build_spidercloud_scraper(
        mask_secret=mask_secret,
        sanitize_headers=sanitize_headers,
        build_request_snapshot=build_request_snapshot,
        log_provider_dispatch=log_provider_dispatch,
        log_sync_response=log_sync_response,
        trim_scrape_for_convex=trim_scrape_for_convex,
    )


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=2.0, backoff_rate=2.0)
async def scrape_job_details(
    urls: list[str],
    source_url: str,
    pattern: str | None = None,
    posted_at_by_url: dict[str, int] | None = None,
    site_id: str | None = None,
) -> dict[str, Any]:
    """Scrape job detail URLs via SpiderCloud API.

    This step calls the SpiderCloud API to scrape job detail pages and extract
    normalized job data. It handles retries automatically via DBOS.

    Args:
        urls: List of job detail URLs to scrape
        source_url: The original site URL (for context)
        pattern: Optional URL pattern for filtering
        posted_at_by_url: Optional mapping of URLs to posted timestamps
        site_id: Optional site ID for tracking

    Returns:
        Dict containing the scrape response with normalized job data.
        Structure includes:
        - items.normalized: List of extracted job data
        - items.raw: Raw scrape response
        - costMilliCents: API cost

    Raises:
        Exception: On non-retryable errors after retry attempts exhausted.
    """
    if not urls:
        return {
            "scrape": None,
            "items": {"normalized": [], "raw": []},
            "urls": [],
        }

    scraper = _make_scraper()
    payload: dict[str, Any] = {
        "urls": urls,
        "source_url": source_url,
        "pattern": pattern,
    }
    if posted_at_by_url:
        payload["posted_at_by_url"] = posted_at_by_url

    result = await scraper.scrape_greenhouse_jobs(payload)

    # Augment result with site_id for downstream storage
    if result and isinstance(result, dict) and site_id:
        scrape = result.get("scrape")
        if isinstance(scrape, dict):
            scrape.setdefault("siteId", site_id)
        else:
            result.setdefault("siteId", site_id)

    return result or {
        "scrape": None,
        "items": {"normalized": [], "raw": []},
        "urls": urls,
    }
