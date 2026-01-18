"""DBOS workflow function for enqueuing scrape URLs."""

from __future__ import annotations

from typing import Any

from dbos import DBOS

from ..queue import enqueue_scrape_urls as _enqueue_scrape_urls


@DBOS.workflow()
def enqueue_scrape_urls_step(
    urls: list[str],
    source_url: str,
    provider: str = "spidercloud",
    site_id: str | None = None,
    pattern: str | None = None,
    url_types: list[str] | None = None,
    posted_ats: list[int | None] | None = None,
    delays_ms: list[int] | None = None,
) -> dict[str, Any]:
    """Enqueue URLs for scraping.

    This workflow enqueues DBOS workflows via DBOS queues with basic deduplication
    for completed detail URLs. Used by listing workflows to enqueue discovered
    job URLs.

    Note: This must be a workflow (not a step) because queue.enqueue() can only
    be called from a workflow context.

    Args:
        urls: List of URLs to enqueue
        source_url: The original site URL
        provider: Scrape provider (default: "spidercloud")
        site_id: Optional site ID for tracking
        pattern: Optional URL pattern
        url_types: List of URL types ("listing" or "detail") for each URL
        posted_ats: List of posted timestamps for each URL
        delays_ms: List of delay times in milliseconds for each URL

    Returns:
        Dict with "queued" count and other metadata
    """
    payload: dict[str, Any] = {
        "urls": urls,
        "sourceUrl": source_url,
        "provider": provider,
    }
    if site_id:
        payload["siteId"] = site_id
    if pattern:
        payload["pattern"] = pattern
    if url_types:
        payload["urlTypes"] = url_types
    if posted_ats:
        payload["postedAts"] = posted_ats
    if delays_ms:
        payload["delaysMs"] = delays_ms

    return _enqueue_scrape_urls(payload)
