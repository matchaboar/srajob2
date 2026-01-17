"""Queue management activities for leasing and completing scrape URL batches.

DEPRECATED: These @activity.defn functions are deprecated in favor of @DBOS.step functions.
Use the step functions from job_scrape_application.dbos_runtime.step instead:
- lease_scrape_url_batch -> lease_scrape_url_batch_step
- complete_scrape_urls -> complete_scrape_urls_step
- fail_listing_batch_urls -> fail_listing_batch_urls_step
"""

from __future__ import annotations

import logging
import time
import warnings
from typing import Any, Dict, List, Optional

from temporalio import activity

from job_scrape_application.config import runtime_config
from job_scrape_application.dbos_runtime import queue as dbos_queue
from job_scrape_application.workflows.helpers.step import fetch_seen_urls_for_site
from job_scrape_application.workflows.site_handlers import get_site_handler
from job_scrape_application.workflows.activities.step import record_scrape_url_attempts

# Re-export step function for backwards compatibility
__all__ = [
    "record_scrape_url_attempts",
    "complete_scrape_urls",
    "lease_scrape_url_batch",
    "fail_listing_batch_urls",
]

logger = logging.getLogger("temporal.worker.activities")

SPIDERCLOUD_BATCH_SIZE = runtime_config.spidercloud_job_details_batch_size


def _is_spidercloud_listing_url(url: str, source_url: str | None = None) -> bool:
    """Check if URL is a listing URL for SpiderCloud scraping."""
    from urllib.parse import urlparse

    handler = get_site_handler(url) or (get_site_handler(source_url) if source_url else None)
    if handler and handler.is_listing_url(url):
        return True
    if handler and handler.name == "greenhouse":
        # Allow explicit detail endpoints, skip listing/board URLs.
        if handler.is_api_detail_url(url):
            return False
        if handler.get_api_uri(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower().rstrip("/")
        parts = [p for p in path.split("/") if p]
        if host.endswith("greenhouse.io") and path.endswith("/jobs"):
            if len(parts) == 4 and parts[0] == "v1" and parts[1] == "boards" and parts[3] == "jobs":
                return True
            if host.endswith("boards.greenhouse.io") and len(parts) <= 2:
                return True
            return True
        if host.endswith("boards.greenhouse.io") and len(parts) == 1 and parts[0]:
            return True
    return False


@activity.defn
def complete_scrape_urls(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Mark queued scrape URLs as completed/failed in DBOS queue.

    DEPRECATED: Use complete_scrape_urls_step from dbos_runtime.step instead.
    """
    warnings.warn(
        "complete_scrape_urls is deprecated. Use complete_scrape_urls_step from "
        "job_scrape_application.dbos_runtime.step instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    res = dbos_queue.complete_scrape_urls(payload)
    return res if isinstance(res, dict) else {"updated": 0}


@activity.defn
def lease_scrape_url_batch(
    provider: Optional[str] = None,
    limit: int = SPIDERCLOUD_BATCH_SIZE,
    url_type: str | None = None,
) -> Dict[str, Any]:
    """Lease a batch of queued URLs from DBOS.

    DEPRECATED: Use lease_scrape_url_batch_step from dbos_runtime.step instead.
    """
    warnings.warn(
        "lease_scrape_url_batch is deprecated. Use lease_scrape_url_batch_step from "
        "job_scrape_application.dbos_runtime.step instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    skipped_urls: List[str] = []
    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
        attempts += 1
        lease = dbos_queue.lease_scrape_url_batch(provider=provider, limit=limit, url_type=url_type)
        queued = lease.urls if lease else []
        skipped_urls.extend(getattr(lease, "skipped_urls", []) if lease else [])
        if not queued:
            return {"urls": [], "skippedUrls": skipped_urls}

        grouped: Dict[tuple[str, str | None], List[Dict[str, Any]]] = {}
        retained: List[Dict[str, Any]] = []
        for row in queued:
            if not isinstance(row, dict):
                continue
            url_val = row.get("url")
            if not isinstance(url_val, str) or not url_val.strip():
                continue
            source_val = row.get("sourceUrl") if isinstance(row.get("sourceUrl"), str) else ""
            pattern_val = row.get("pattern") if isinstance(row.get("pattern"), str) else None
            url_type_val = row.get("urlType") if isinstance(row.get("urlType"), str) else None
            is_listing = url_type_val == "listing" or _is_spidercloud_listing_url(url_val, source_val or None)
            if is_listing or not source_val:
                retained.append(row)
                continue
            grouped.setdefault((source_val, pattern_val), []).append(row)

        to_skip: List[Dict[str, Any]] = []
        for (source_url, pattern), rows in grouped.items():
            try:
                seen_urls = fetch_seen_urls_for_site(source_url, pattern)
            except Exception:
                seen_urls = []
            seen_set = {u for u in seen_urls if isinstance(u, str)}
            for row in rows:
                url_val = row.get("url")
                if isinstance(url_val, str) and url_val in seen_set:
                    to_skip.append(row)
                    skipped_urls.append(url_val)
                else:
                    retained.append(row)

        if to_skip:
            items: List[Dict[str, Any]] = []
            for row in to_skip:
                url_val = row.get("url")
                if not isinstance(url_val, str):
                    continue
                item = {"url": url_val}
                row_id = row.get("_id") or row.get("id")
                if isinstance(row_id, str):
                    item["id"] = row_id
                items.append(item)
            if items:
                try:
                    dbos_queue.complete_scrape_urls(
                        {"items": items, "status": "failed", "error": "skip_listed_url"}
                    )
                except Exception:
                    pass

        if retained:
            return {"urls": retained, "skippedUrls": skipped_urls}

    return {"urls": [], "skippedUrls": skipped_urls}


@activity.defn
def fail_listing_batch_urls(
    batch: Dict[str, Any],
    error: str = "batch_failed",
) -> Dict[str, Any]:
    """Mark a leased listing batch as failed without touching workflow code.

    DEPRECATED: Use fail_listing_batch_urls_step from dbos_runtime.step instead.
    """
    warnings.warn(
        "fail_listing_batch_urls is deprecated. Use fail_listing_batch_urls_step from "
        "job_scrape_application.dbos_runtime.step instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    items: List[Dict[str, Any]] = []
    for entry in batch.get("urls", []):
        if not isinstance(entry, dict):
            continue
        url_val = entry.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            continue
        item: Dict[str, Any] = {"url": url_val, "isListingUrl": True}
        row_id = entry.get("_id")
        if isinstance(row_id, str):
            item["id"] = row_id
        source_val = entry.get("sourceUrl")
        if isinstance(source_val, str):
            item["sourceUrl"] = source_val
        provider_val = entry.get("provider")
        if isinstance(provider_val, str):
            item["provider"] = provider_val
        site_val = entry.get("siteId")
        if isinstance(site_val, str):
            item["siteId"] = site_val
        attempts_val = entry.get("attempts")
        if isinstance(attempts_val, (int, float)):
            item["attempts"] = int(attempts_val)
        items.append(item)

    if not items:
        return {"updated": 0}

    res = dbos_queue.complete_scrape_urls(
        {"items": items, "status": "failed", "error": error}
    )
    return res if isinstance(res, dict) else {"updated": 0}
