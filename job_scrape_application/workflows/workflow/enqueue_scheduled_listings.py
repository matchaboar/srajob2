"""DBOS workflow for scheduled listing URL enqueuing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbos import DBOS

from ..activities.step import (
    emit_scrape_telemetry_step,
    fetch_enabled_sites_step,
)
from ...dbos_runtime.step import (
    check_detail_queue_pending_step,
    enqueue_scrape_urls_step,
)
from ...dbos_runtime.step.load_schedule_interval_minutes import (
    load_schedule_interval_minutes,
)
from ..site_handlers import get_site_handler
from ..result import Result, Success


@dataclass
class ScheduleResult:
    """Result of a scheduled listing enqueue."""

    queued: int
    sites_processed: int
    skipped_pending_details: bool


def _dedupe_urls(values: list[str]) -> list[str]:
    """Deduplicate URLs while preserving order (deterministic)."""
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _limit_listing_urls(urls: list[str], limit: int | None) -> list[str]:
    """Limit listing URLs based on pagination limit (deterministic).

    Args:
        urls: List of URLs to filter
        limit: Maximum number of pages to include (0 = no limit)

    Returns:
        Filtered list of URLs
    """
    from urllib.parse import parse_qs, urlparse

    if not urls:
        return []
    deduped = _dedupe_urls(urls)
    if not limit or limit <= 0:
        return deduped

    filtered: list[str] = []
    for url in deduped:
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
        except Exception:
            params = {}

        page_val = None
        for key in ("page", "from", "start", "offset"):
            value = params.get(key, [None])[0]
            if value is None:
                continue
            try:
                page_val = int(value)
            except Exception:
                page_val = None
            if page_val is not None:
                break

        if page_val is None or page_val <= limit:
            filtered.append(url)

    if len(filtered) <= limit:
        return filtered
    return filtered[:limit]


def _generate_listing_urls_for_site(site: dict[str, Any]) -> list[str]:
    """Generate listing URLs for a site including pagination (deterministic).

    Args:
        site: Site configuration dict

    Returns:
        List of listing URLs to scrape
    """
    url = site.get("url")
    if not isinstance(url, str) or not url.strip():
        return []

    site_type_raw = site.get("type")
    site_type: str | None = site_type_raw if isinstance(site_type_raw, str) else None

    pagination_limit = site.get("paginationLimit")
    if isinstance(pagination_limit, (int, float)):
        pagination_limit = max(0, int(pagination_limit))
    else:
        pagination_limit = 0

    handler = get_site_handler(url.strip(), site_type)
    listing_urls = [url.strip()]

    if handler:
        pagination_urls = handler.get_pagination_urls_from_listing(url)
        if pagination_urls:
            listing_urls.extend(pagination_urls)

    listing_urls = _limit_listing_urls(listing_urls, pagination_limit)
    return listing_urls


@DBOS.workflow()
def enqueue_scheduled_listings() -> Result[ScheduleResult]:
    """Enqueue listing URLs for scheduled scraping.

    This workflow:
    1. Loads schedule configuration from Convex
    2. Checks if detail queue has pending work (skip if so)
    3. Fetches enabled sites from Convex
    4. Generates pagination URLs for each site
    5. Enqueues listing URLs to SQLite queue
    6. Records workflow run metrics
    7. Emits telemetry

    Returns:
        Success[ScheduleResult] with queuing statistics
    """
    # Step: Load schedule interval (for future use with DBOS scheduled decorator)
    try:
        _interval = load_schedule_interval_minutes()
    except Exception:
        _interval = 15  # Default to 15 minutes

    # Step: Check if detail queue has pending work
    try:
        has_pending = check_detail_queue_pending_step(include_processing=True)
    except Exception:
        has_pending = False

    if has_pending:
        # Skip listing enqueue if detail queue is backed up
        try:
            emit_scrape_telemetry_step(
                event="schedule.skipped.pending_details",
                level="info",
                site_url="",
                data={"reason": "detail_queue_has_pending"},
            )
        except Exception:
            pass

        return Success(ScheduleResult(
            queued=0,
            sites_processed=0,
            skipped_pending_details=True,
        ))

    # Step: Fetch enabled sites
    try:
        sites = fetch_enabled_sites_step()
    except Exception as exc:
        try:
            emit_scrape_telemetry_step(
                event="schedule.error.fetch_sites",
                level="error",
                site_url="",
                data={"error": str(exc)[:200]},
            )
        except Exception:
            pass
        return Success(ScheduleResult(
            queued=0,
            sites_processed=0,
            skipped_pending_details=False,
        ))

    if not sites:
        return Success(ScheduleResult(
            queued=0,
            sites_processed=0,
            skipped_pending_details=False,
        ))

    queued_total = 0
    sites_processed = 0

    # Process each site
    for site in sites:
        if not isinstance(site, dict):
            continue

        url = site.get("url")
        if not isinstance(url, str) or not url.strip():
            continue

        # Deterministic: Generate listing URLs
        listing_urls = _generate_listing_urls_for_site(site)
        if not listing_urls:
            continue

        # Step: Enqueue listing URLs
        try:
            result = enqueue_scrape_urls_step(
                urls=listing_urls,
                source_url=url.strip(),
                provider=str(site.get("scrapeProvider") or "spidercloud"),
                site_id=str(site.get("_id")) if site.get("_id") else None,
                pattern=str(site.get("pattern")) if site.get("pattern") else None,
                url_types=["listing"] * len(listing_urls),
            )
            if isinstance(result, dict) and isinstance(result.get("queued"), int):
                queued_total += int(result["queued"])
            sites_processed += 1
        except Exception:
            # Individual site failure should not stop other sites
            pass

    # Step: Emit telemetry
    try:
        emit_scrape_telemetry_step(
            event="schedule.completed",
            level="info",
            site_url="",
            data={
                "queued": queued_total,
                "sites_processed": sites_processed,
            },
        )
    except Exception:
        pass

    return Success(ScheduleResult(
        queued=queued_total,
        sites_processed=sites_processed,
        skipped_pending_details=False,
    ))
