"""Store scrape function using step-based architecture.

This module provides the store_scrape function that stores scrape records,
ingests jobs, and enqueues discovered URLs using DBOS step functions.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict

from ..activities.step import (
    ingest_jobs_from_scrape_step,
    insert_ignored_job_step,
    insert_scrape_record_step,
    store_job_descriptions_step,
)
from ..activities.url_processing import _is_probable_listing_url
from ..helpers.job_url_extractor import extract_job_urls_from_scrape
from ..site_handlers import get_site_handler

logger = logging.getLogger(__name__)


def _resolve_source_url(data: Dict[str, Any]) -> str:
    """Best-effort source URL extraction that tolerates missing fields."""
    for key in ("sourceUrl", "sourceURL", "source_url", "siteUrl", "url"):
        val = data.get(key)
        if isinstance(val, str) and val.strip():
            return val

    request_block = data.get("request")
    if isinstance(request_block, dict):
        req_url = request_block.get("url")
        if isinstance(req_url, str) and req_url.strip():
            return req_url

    return ""


def _is_http_404_entry(entry: Dict[str, Any]) -> bool:
    """Check if an ignored entry is due to HTTP 404."""
    status = entry.get("status") or entry.get("httpStatus")
    if isinstance(status, (int, float)) and int(status) == 404:
        return True
    reason = entry.get("reason")
    if isinstance(reason, str) and "404" in reason.lower():
        return True
    error_type = entry.get("errorType")
    return isinstance(error_type, str) and "404" in error_type.lower()


def _build_base_payload(
    data: Dict[str, Any],
    now: int,
) -> Dict[str, Any]:
    """Build the base payload for storing a scrape record."""
    source_url = _resolve_source_url(data)
    body: Dict[str, Any] = {
        "sourceUrl": source_url,
        "startedAt": data.get("startedAt", now),
        "completedAt": data.get("completedAt", now),
        "items": data.get("items"),
    }
    if data.get("siteId") is not None:
        body["siteId"] = data.get("siteId")

    provider_value = data.get("provider")
    if provider_value is None and isinstance(data.get("items"), dict):
        provider_value = data["items"].get("provider")
    if provider_value is not None:
        body["provider"] = str(provider_value)

    workflow_value = data.get("workflowName")
    if workflow_value is not None:
        body["workflowName"] = str(workflow_value)

    if data.get("pattern") is not None:
        body["pattern"] = data.get("pattern")
    if data.get("request") is not None:
        body["request"] = data.get("request")
    if data.get("workflowId") is not None:
        body["workflowId"] = data.get("workflowId")

    return body


def _trim_scrape_for_fallback(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Create a trimmed fallback payload for retries."""
    trimmed = dict(payload)
    items = trimmed.get("items")
    if isinstance(items, dict):
        trimmed_items = dict(items)
        # Keep only essential fields and mark as truncated
        if "normalized" in trimmed_items:
            normalized = trimmed_items.get("normalized")
            if isinstance(normalized, list):
                trimmed_items["normalized"] = normalized[:100]
        trimmed_items["truncated"] = True
        trimmed["items"] = trimmed_items
    return trimmed


def store_scrape(scrape: Dict[str, Any]) -> str:  # noqa: DBOS001
    """Store a scrape record and ingest its jobs.

    This function:
    1. Inserts the scrape record into Convex (with retry and truncation fallback)
    2. Ingests normalized jobs if present
    3. Records ignored jobs (skipping http_404)
    4. Enqueues extracted job URLs for further processing

    Args:
        scrape: The scrape payload containing items, sourceUrl, etc.

    Returns:
        The scrape record ID.
    """
    now = int(time.time() * 1000)
    payload = _build_base_payload(scrape, now)
    source_url = payload.get("sourceUrl") or scrape.get("pattern") or ""
    provider = payload.get("provider")
    workflow_name = payload.get("workflowName")

    # Try to insert the scrape record
    scrape_id: str | None = None
    try:
        scrape_id = insert_scrape_record_step(payload)
    except Exception as exc:
        logger.warning("insertScrapeRecord failed; retrying with trimmed payload: %s", exc)
        # Fallback: trim the payload and retry
        fallback = _trim_scrape_for_fallback(payload)
        scrape_id = insert_scrape_record_step(fallback)

    # Ingest normalized jobs
    items_block = scrape.get("items")
    if isinstance(items_block, dict):
        normalized = items_block.get("normalized")
        if isinstance(normalized, list) and normalized:
            site_id = scrape.get("siteId")
            ingest_jobs_from_scrape_step(normalized, site_id)
            store_job_descriptions_step(
                normalized,
                source_url=source_url or None,
                provider=provider if isinstance(provider, str) else None,
                workflow_name=workflow_name if isinstance(workflow_name, str) else None,
            )

    # Record ignored jobs (skipping http_404)
    if isinstance(items_block, dict):
        ignored_entries = items_block.get("ignored") or []
        if isinstance(ignored_entries, list):
            for entry in ignored_entries:
                if not isinstance(entry, dict):
                    continue
                # Skip http_404 entries
                reason = entry.get("reason")
                if isinstance(reason, str) and reason == "http_404":
                    continue
                if _is_http_404_entry(entry):
                    continue

                url_val = entry.get("url")
                if not isinstance(url_val, str) or not url_val.strip():
                    continue

                title_val = entry.get("title")
                desc_val = entry.get("description")
                if not isinstance(title_val, str) or not title_val.strip():
                    title_val = "Unknown"
                if isinstance(desc_val, str) and len(desc_val) > 4000:
                    desc_val = desc_val[:4000]

                insert_ignored_job_step(
                    url=url_val.strip(),
                    source_url=source_url,
                    reason=reason or "filtered",
                    provider=provider,
                    workflow_name=workflow_name,
                    title=title_val,
                    description=desc_val if isinstance(desc_val, str) else None,
                )

    # Enqueue extracted job URLs
    try:
        urls = extract_job_urls_from_scrape(scrape)
        if urls:
            source_url = scrape.get("sourceUrl") or _resolve_source_url(scrape)
            handler = get_site_handler(source_url) if source_url else None

            # Filter out listing/pagination URLs, keep everything else
            def is_not_listing_url(url: str) -> bool:
                """Check if URL is NOT a listing/pagination page."""
                # Use handler's detection if available
                if handler and handler.is_listing_url(url):
                    return False
                # Fall back to heuristics for listing URLs
                if _is_probable_listing_url(url):
                    return False
                return True

            filtered_urls = [url for url in urls if is_not_listing_url(url)]

            if filtered_urls:
                from ...dbos_runtime.queue import enqueue_scrape_urls

                site_id = scrape.get("siteId")
                enqueue_scrape_urls({
                    "urls": filtered_urls,
                    "sourceUrl": source_url,
                    "siteId": site_id,
                })
    except Exception as exc:
        # URL enqueuing is best-effort
        logger.warning("Failed to enqueue extracted URLs: %s", exc)

    return scrape_id or ""


__all__ = ["store_scrape"]
