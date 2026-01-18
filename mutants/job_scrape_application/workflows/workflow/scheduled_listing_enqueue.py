"""DBOS scheduled workflow for listing URL enqueuing.

This workflow runs on a schedule (default: every 15 minutes) and enqueues
listing URLs for enabled sites. It uses DBOS's native scheduling for
exactly-once execution guarantees.

Usage:
    Set USE_DBOS_SCHEDULED=1 to enable this scheduled workflow instead of
    the polling-based schedule loop.
"""

from __future__ import annotations

from datetime import datetime

from dbos import DBOS

from ..activities.step import (
    emit_scrape_telemetry_step,
    fetch_enabled_sites_step,
)
from ...dbos_runtime.step import (
    check_detail_queue_pending_step,
    enqueue_scrape_urls_step,
)
from ..site_handlers import get_site_handler
from ..result import Result, Success
from .enqueue_scheduled_listings import (
    ScheduleResult,
    _limit_listing_urls,
)


@DBOS.pure_func
def _generate_listing_urls_for_site(site: dict) -> list[str]:
    """Generate listing URLs for a site including pagination (deterministic)."""
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
    pagination_urls = handler.get_pagination_urls_from_listing(url) if handler else []
    pagination_list = list(pagination_urls) if pagination_urls else []
    listing_urls = _limit_listing_urls([url.strip()] + pagination_list, pagination_limit)
    return listing_urls


@DBOS.workflow()
def scheduled_listing_enqueue(
    scheduled_time: datetime,
    actual_time: datetime,
) -> Result[ScheduleResult]:
    """Scheduled workflow to enqueue listing URLs.

    This workflow runs every 15 minutes via DBOS scheduling. It provides
    exactly-once execution guarantees - if the worker crashes mid-execution,
    DBOS will automatically recover and resume from the last completed step.

    Args:
        scheduled_time: The time this execution was scheduled for
        actual_time: The actual time when execution started

    Returns:
        Success[ScheduleResult] with queuing statistics
    """
    DBOS.logger.info(
        "Scheduled listing enqueue starting (scheduled=%s, actual=%s)",
        scheduled_time.isoformat(),
        actual_time.isoformat(),
    )

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

        DBOS.logger.info("Skipping listing schedule; detail queue has pending items.")
        return Success(ScheduleResult(
            queued=0,
            sites_processed=0,
            skipped_pending_details=True,
        ))

    # Step: Fetch enabled sites
    try:
        sites = fetch_enabled_sites_step()
    except Exception as exc:
        DBOS.logger.error("Failed to fetch enabled sites: %s", exc)
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
        DBOS.logger.info("No enabled sites found.")
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
        except Exception as exc:
            DBOS.logger.warning("Failed to enqueue for site %s: %s", url, exc)
            # Individual site failure should not stop other sites

    # Step: Emit telemetry
    try:
        emit_scrape_telemetry_step(
            event="schedule.completed",
            level="info",
            site_url="",
            data={
                "queued": queued_total,
                "sites_processed": sites_processed,
                "scheduled_time": scheduled_time.isoformat(),
            },
        )
    except Exception:
        pass

    DBOS.logger.info(
        "Scheduled listing enqueue completed: queued=%d sites=%d",
        queued_total,
        sites_processed,
    )

    return Success(ScheduleResult(
        queued=queued_total,
        sites_processed=sites_processed,
        skipped_pending_details=False,
    ))
