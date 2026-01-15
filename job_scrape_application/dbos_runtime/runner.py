from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import logging
import threading
import time
from typing import Awaitable, Callable
from urllib.parse import parse_qs, urlparse

from .api import serve as serve_api
from .queue import (
    LeaseResult,
    complete_scrape_urls,
    detail_queue_has_pending,
    enqueue_scrape_urls,
    lease_scrape_url_batch,
    queue_status,
    recover_stale_processing_items,
)
from .runs import last_completed_at, record_run
from .sqlite import initialize_schema, now_ms
from ..services import telemetry
from ..services.convex_client import convex_query
from ..workflows import activities as workflow_activities
from ..workflows.helpers.spidercloud_error_strategy import decision_for_exception
from ..workflows.site_handlers import get_site_handler

logger = logging.getLogger("dbos.runner")

# Thread pool for blocking DB operations (avoid blocking the event loop)
_DB_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None


def _get_db_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _DB_EXECUTOR
    if _DB_EXECUTOR is None:
        _DB_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="db")
    return _DB_EXECUTOR


async def _async_lease_batch(
    provider: str | None, limit: int, url_type: str | None
) -> LeaseResult:
    """Run lease_scrape_url_batch in executor to avoid blocking event loop."""
    loop = asyncio.get_running_loop()
    start = time.monotonic()
    result = await loop.run_in_executor(
        _get_db_executor(),
        lambda: lease_scrape_url_batch(provider=provider, limit=limit, url_type=url_type),
    )
    elapsed = time.monotonic() - start
    if elapsed > 1.0:
        logger.warning("Slow lease_scrape_url_batch: %.3fs url_type=%s", elapsed, url_type)
    return result


async def _async_detail_queue_pending() -> bool:
    """Run detail_queue_has_pending in executor to avoid blocking event loop."""
    global _LAST_DETAIL_QUEUE_STATUS_ERROR_MS
    loop = asyncio.get_running_loop()
    start = time.monotonic()
    try:
        result = await loop.run_in_executor(
            _get_db_executor(),
            detail_queue_has_pending,
        )
        elapsed = time.monotonic() - start
        if elapsed > 0.5:
            logger.warning("Slow detail_queue_has_pending: %.3fs", elapsed)
        return result
    except Exception as exc:
        now = now_ms()
        if (
            _LAST_DETAIL_QUEUE_STATUS_ERROR_MS is None
            or now - _LAST_DETAIL_QUEUE_STATUS_ERROR_MS > _DETAIL_QUEUE_STATUS_ERROR_LOG_MS
        ):
            _LAST_DETAIL_QUEUE_STATUS_ERROR_MS = now
            logger.warning(
                "Failed to read detail queue status; listing priority disabled. error=%s",
                exc,
            )
        return False

ActivityHandler = Callable[..., Awaitable[object]]


def _load_activity_handler(name: str) -> ActivityHandler:
    handler = getattr(workflow_activities, name, None)
    if handler is None:
        available = [
            attr
            for attr in (
                "process_spidercloud_job_batch",
                "process_spidercloud_listing_batch",
            )
            if hasattr(workflow_activities, attr)
        ]
        message = (
            f"Missing workflow activity '{name}'. "
            f"Available: {', '.join(available) if available else 'none'}"
        )
        logger.error(message)
        try:
            telemetry.emit_posthog_exception(
                RuntimeError(message),
                properties={"activityName": name},
            )
        except Exception:
            pass
        raise RuntimeError(message)
    return handler


def _setup_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    posthog_handler = telemetry.build_posthog_log_handler(level=logging.INFO)
    if posthog_handler is not None:
        handlers.append(posthog_handler)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)


SCHEDULE_WORKFLOW_NAME = "listing-schedule"
SCHEDULE_POLL_SECONDS = 60
DEFAULT_SCHEDULE_INTERVAL_MINUTES = 15
SCHEDULE_CONFIG_REFRESH_SECONDS = 600
SITES_REFRESH_SECONDS = 300

_SCHEDULE_CACHE: tuple[int, dict[str, object]] | None = None
_SITES_CACHE: tuple[int, list[dict[str, object]]] | None = None
_DETAIL_QUEUE_STATUS_ERROR_LOG_MS = 60_000
_LAST_DETAIL_QUEUE_STATUS_ERROR_MS: int | None = None


def _dedupe_urls(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _limit_listing_urls(urls: list[str], limit: int | None) -> list[str]:
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


def _reset_cache() -> None:
    global _SCHEDULE_CACHE, _SITES_CACHE
    _SCHEDULE_CACHE = None
    _SITES_CACHE = None


async def _process_listing_batch(batch: LeaseResult) -> None:
    if not batch.urls:
        return
    try:
        handler = _load_activity_handler("process_spidercloud_listing_batch")
        await handler({"urls": batch.urls})
    except Exception as exc:
        decision = decision_for_exception(exc, source="spidercloud_api")
        logger.exception(
            "Listing batch failed action=%s reason=%s",
            decision.action,
            decision.error,
        )
        if batch.urls:
            status = "pending" if decision.action == "retry" else "failed"
            payload = {
                "items": [{"id": row.get("_id"), "url": row.get("url")} for row in batch.urls],
                "status": status,
                "error": decision.error,
            }
            if status == "pending" and decision.retry_after_seconds is not None:
                payload["runAfterMs"] = int(decision.retry_after_seconds * 1000)
            complete_scrape_urls(payload)


async def _process_detail_batch(batch: LeaseResult) -> None:
    if not batch.urls:
        return
    try:
        handler = _load_activity_handler("process_spidercloud_job_batch")
        await handler({"urls": batch.urls}, persist_scrapes=True)
    except Exception as exc:
        decision = decision_for_exception(exc, source="spidercloud_api")
        logger.exception(
            "Detail batch failed action=%s reason=%s",
            decision.action,
            decision.error,
        )
        payload = {
            "items": [{"id": row.get("_id"), "url": row.get("url")} for row in batch.urls],
            "status": "pending" if decision.action == "retry" else "failed",
            "error": decision.error,
        }
        if payload["status"] == "pending" and decision.retry_after_seconds is not None:
            payload["runAfterMs"] = int(decision.retry_after_seconds * 1000)
        complete_scrape_urls(payload)


async def _run_queue_loop(
    *,
    url_type: str,
    limit: int,
    poll_interval: float,
    max_in_flight: int,
    handler: Callable[[LeaseResult], Awaitable[None]],
    pause_when_async: Callable[[], Awaitable[bool]] | None = None,
) -> None:
    semaphore = asyncio.Semaphore(max(1, max_in_flight))
    queue_label = url_type or "unknown"

    async def _run_batch(batch: LeaseResult) -> None:
        try:
            await handler(batch)
        finally:
            semaphore.release()

    logger.info("Queue loop starting: %s (concurrency=%d)", queue_label, max_in_flight)

    while True:
        try:
            # Check if we should pause (async to not block event loop)
            if pause_when_async:
                should_pause = await pause_when_async()
                if should_pause:
                    await asyncio.sleep(poll_interval)
                    continue

            # Check semaphore without blocking
            if semaphore.locked():
                await asyncio.sleep(poll_interval)
                continue

            # Lease batch using async wrapper (runs in thread pool)
            batch = await _async_lease_batch(provider=None, limit=limit, url_type=url_type)

            if batch.urls:
                await semaphore.acquire()
                asyncio.create_task(_run_batch(batch))
                # Yield to allow other tasks to run
                await asyncio.sleep(0)
                continue

            # No items available, sleep before polling again
            await asyncio.sleep(poll_interval)

        except Exception as exc:
            logger.exception("Queue loop error (%s): %s", queue_label, exc)
            await asyncio.sleep(poll_interval)


async def _load_schedule_interval_minutes() -> int:
    global _SCHEDULE_CACHE
    now = now_ms()
    if _SCHEDULE_CACHE is not None:
        fetched_at, cached = _SCHEDULE_CACHE
        if now - fetched_at < SCHEDULE_CONFIG_REFRESH_SECONDS * 1000:
            return _interval_from_config(cached)

    try:
        config = await convex_query("temporal:getScrapeSchedule", {})
    except Exception:
        return DEFAULT_SCHEDULE_INTERVAL_MINUTES
    if isinstance(config, dict):
        _SCHEDULE_CACHE = (now, config)
        return _interval_from_config(config)
    return DEFAULT_SCHEDULE_INTERVAL_MINUTES


def _interval_from_config(config: dict[str, object]) -> int:
    interval = config.get("intervalMinutes")
    if isinstance(interval, (int, float)) and interval > 0:
        return int(interval)
    if config.get("mode") == "daily":
        return 24 * 60
    return DEFAULT_SCHEDULE_INTERVAL_MINUTES


async def _enqueue_listing_sites() -> int:
    global _SITES_CACHE
    now = now_ms()
    sites: list[dict[str, object]] | None = None
    if _SITES_CACHE is not None:
        fetched_at, cached_sites = _SITES_CACHE
        if now - fetched_at < SITES_REFRESH_SECONDS * 1000:
            sites = cached_sites
    if sites is None:
        fetched = await convex_query("router:listSites", {"enabledOnly": True})
        if not isinstance(fetched, list):
            return 0
        sites = [site for site in fetched if isinstance(site, dict)]
        _SITES_CACHE = (now, sites)
    queued = 0
    for site in sites:
        url = site.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        site_type = site.get("type") if isinstance(site.get("type"), str) else None
        pagination_limit = site.get("paginationLimit")
        if isinstance(pagination_limit, (int, float)):
            pagination_limit = max(0, int(pagination_limit))
        else:
            pagination_limit = 0
        handler = get_site_handler(url, site_type)
        listing_urls = [url.strip()]
        if handler:
            pagination_urls = handler.get_pagination_urls_from_listing(url)
            if pagination_urls:
                listing_urls.extend(pagination_urls)
        listing_urls = _limit_listing_urls(listing_urls, pagination_limit)
        if not listing_urls:
            continue
        payload = {
            "urls": listing_urls,
            "sourceUrl": url,
            "provider": site.get("scrapeProvider") or "spidercloud",
            "siteId": site.get("_id"),
            "pattern": site.get("pattern"),
            "urlTypes": ["listing"] * len(listing_urls),
        }
        result = enqueue_scrape_urls(payload)
        if isinstance(result, dict) and isinstance(result.get("queued"), int):
            queued += int(result["queued"])
    return queued


async def _run_schedule_loop() -> None:
    while True:
        try:
            interval_minutes = await _load_schedule_interval_minutes()
            last_run = last_completed_at(SCHEDULE_WORKFLOW_NAME) or 0
            interval_ms = interval_minutes * 60 * 1000
            now = now_ms()
            if now - last_run >= interval_ms:
                started_at = now
                # Use async version to not block event loop
                if await _async_detail_queue_pending():
                    logger.info(
                        "Skipping listing schedule; detail queue has pending items.",
                    )
                    await asyncio.sleep(SCHEDULE_POLL_SECONDS)
                    continue
                try:
                    queued = await _enqueue_listing_sites()
                    record_run(
                        workflow_name=SCHEDULE_WORKFLOW_NAME,
                        queue_name="listing",
                        status="completed",
                        started_at=started_at,
                        completed_at=now_ms(),
                    )
                    logger.info("Scheduled listing enqueue queued=%s", queued)
                except Exception as exc:
                    record_run(
                        workflow_name=SCHEDULE_WORKFLOW_NAME,
                        queue_name="listing",
                        status="failed",
                        error=str(exc),
                        started_at=started_at,
                        completed_at=now_ms(),
                    )
                    logger.exception("Scheduled listing enqueue failed")
        except Exception as exc:
            logger.exception("Schedule loop error: %s", exc)
        await asyncio.sleep(SCHEDULE_POLL_SECONDS)


async def run_worker(
    *,
    listing_batch: int,
    detail_batch: int,
    listing_poll: float,
    detail_poll: float,
    listing_concurrency: int,
    detail_concurrency: int,
) -> None:
    initialize_schema()
    # Recover items stuck in 'processing' for more than 10 minutes (likely from a previous crash)
    recovered = recover_stale_processing_items(stale_threshold_ms=10 * 60 * 1000)
    if recovered > 0:
        logger.info("Recovered %d stale processing items (>10min)", recovered)
    logger.info(
        "Starting worker: listing(batch=%d, poll=%.1fs, concurrency=%d) "
        "detail(batch=%d, poll=%.1fs, concurrency=%d)",
        listing_batch, listing_poll, listing_concurrency,
        detail_batch, detail_poll, detail_concurrency,
    )
    await asyncio.gather(
        _run_queue_loop(
            url_type="listing",
            limit=listing_batch,
            poll_interval=listing_poll,
            max_in_flight=listing_concurrency,
            handler=_process_listing_batch,
            pause_when_async=_async_detail_queue_pending,
        ),
        _run_queue_loop(
            url_type="detail",
            limit=detail_batch,
            poll_interval=detail_poll,
            max_in_flight=detail_concurrency,
            handler=_process_detail_batch,
        ),
        _run_schedule_loop(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="DBOS workflow runner")
    parser.add_argument("--listing-batch", type=int, default=25)
    parser.add_argument("--detail-batch", type=int, default=1)  # Single URL per request in SINGLE_REQUEST_MODE
    parser.add_argument("--listing-poll", type=float, default=1.0)
    parser.add_argument("--detail-poll", type=float, default=0.5)
    parser.add_argument("--listing-concurrency", type=int, default=2)
    parser.add_argument("--detail-concurrency", type=int, default=10)  # Increased for SINGLE_REQUEST_MODE
    parser.add_argument("--with-api", action="store_true")
    parser.add_argument("--api-host", default="0.0.0.0")
    parser.add_argument("--api-port", type=int, default=8080)
    args = parser.parse_args()

    _setup_logging()

    if args.with_api:
        logger.info("Starting DBOS API at %s:%s", args.api_host, args.api_port)
        api_thread = threading.Thread(
            target=serve_api, args=(args.api_host, args.api_port), daemon=True
        )
        api_thread.start()

    logger.info("DBOS queues starting: %s", queue_status())
    asyncio.run(
        run_worker(
            listing_batch=args.listing_batch,
            detail_batch=args.detail_batch,
            listing_poll=args.listing_poll,
            detail_poll=args.detail_poll,
            listing_concurrency=args.listing_concurrency,
            detail_concurrency=args.detail_concurrency,
        )
    )


if __name__ == "__main__":
    main()
