from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import logging
import threading
from typing import Awaitable, Callable

from dbos import DBOS, DBOSConfig

from .api import serve as serve_api
from .queue import (
    LeaseResult,
    complete_scrape_urls,
    detail_queue_has_pending,
    lease_scrape_url_batch,
    queue_status,
    recover_stale_processing_items,
)
from .runs import last_completed_at, record_run
from .sqlite import initialize_schema, now_ms
from .step import (
    load_schedule_interval_minutes,
    reset_schedule_cache,
    reset_sites_cache,
)
from ..services import telemetry
from ..workflows.result import Failure, Success
from ..workflows.workflow import (
    scrape_listing_batch,
    scrape_job_detail_batch,
    enqueue_scheduled_listings,
)

logger = logging.getLogger("dbos.runner")

# DBOS workflows are now the only processing path (activities removed)

# Track if DBOS has been initialized
_DBOS_INITIALIZED = False

# Thread pool for blocking DB operations (avoid blocking the event loop)
_DB_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None


def _initialize_dbos() -> None:
    """Initialize DBOS for workflow support."""
    global _DBOS_INITIALIZED
    if _DBOS_INITIALIZED:
        return

    try:
        # Use a file-based SQLite database for DBOS system tables.
        # Note: in-memory databases (sqlite:///:memory:) don't work reliably with DBOS
        # because different connections/threads get separate databases, causing the
        # "no such table: workflow_status" error.
        from .sqlite import _resolve_db_path
        dbos_db_path = _resolve_db_path().parent / "dbos_system.sqlite"
        config = DBOSConfig(
            name="job-scrape-worker",
            database_url=f"sqlite:///{dbos_db_path}",
        )
        DBOS(config=config)
        DBOS.launch()
        _DBOS_INITIALIZED = True
        logger.info("DBOS initialized successfully")
    except Exception as exc:
        logger.warning("Failed to initialize DBOS: %s. Falling back to activities.", exc)
        _DBOS_INITIALIZED = False


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
    return await loop.run_in_executor(
        _get_db_executor(),
        lambda: lease_scrape_url_batch(provider=provider, limit=limit, url_type=url_type),
    )


async def _async_detail_queue_pending() -> bool:
    """Run detail_queue_has_pending in executor to avoid blocking event loop."""
    global _LAST_DETAIL_QUEUE_STATUS_ERROR_MS
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(
            _get_db_executor(),
            detail_queue_has_pending,
        )
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


def _setup_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    posthog_handler = telemetry.build_posthog_log_handler(level=logging.INFO)
    if posthog_handler is not None:
        handlers.append(posthog_handler)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)


SCHEDULE_WORKFLOW_NAME = "listing-schedule"
SCHEDULE_POLL_SECONDS = 60

_DETAIL_QUEUE_STATUS_ERROR_LOG_MS = 60_000
_LAST_DETAIL_QUEUE_STATUS_ERROR_MS: int | None = None


def _reset_cache() -> None:
    """Reset schedule and sites caches."""
    reset_schedule_cache()
    reset_sites_cache()


async def _process_listing_batch(batch: LeaseResult) -> None:  # noqa: DBOS004 - event loop function
    if not batch.urls:
        return

    if not _DBOS_INITIALIZED:
        logger.error("DBOS not initialized, cannot process listing batch")
        return

    try:
        result = await scrape_listing_batch(batch={"urls": batch.urls})
        match result:
            case Success(value=data):
                logger.info(
                    "Listing batch completed: queued=%d completed=%d source=%s",
                    data.queued,
                    data.completed,
                    data.source_url,
                )
            case Failure(error_type=error_type, message=message):
                logger.error(
                    "Listing batch failed (non-retryable) [%s]: %s",
                    error_type,
                    message,
                )
    except Exception as exc:
        logger.exception("Listing workflow failed: %s", exc)
        # Mark batch as failed
        if batch.urls:
            complete_scrape_urls({
                "items": [{"id": row.get("_id"), "url": row.get("url")} for row in batch.urls],
                "status": "failed",
                "error": f"workflow_error: {str(exc)[:100]}",
            })


async def _process_detail_batch(batch: LeaseResult) -> None:  # noqa: DBOS004 - event loop function
    if not batch.urls:
        return

    if not _DBOS_INITIALIZED:
        logger.error("DBOS not initialized, cannot process detail batch")
        return

    try:
        result = await scrape_job_detail_batch(
            batch={"urls": batch.urls},
            persist_scrapes=True,
        )
        match result:
            case Success(value=data):
                logger.info(
                    "Detail batch completed: stored=%d invalid=%d failed=%d source=%s",
                    data.stored,
                    data.invalid,
                    data.failed,
                    data.source_url,
                )
            case Failure(error_type=error_type, message=message):
                logger.error(
                    "Detail batch failed (non-retryable) [%s]: %s",
                    error_type,
                    message,
                )
    except Exception as exc:
        logger.exception("Detail workflow failed: %s", exc)
        # Mark batch as failed
        complete_scrape_urls({
            "items": [{"id": row.get("_id"), "url": row.get("url")} for row in batch.urls],
            "status": "failed",
            "error": f"workflow_error: {str(exc)[:100]}",
        })


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
                continue

            # No items available, sleep before polling again
            await asyncio.sleep(poll_interval)

        except Exception as exc:
            logger.exception("Queue loop error (%s): %s", queue_label, exc)
            await asyncio.sleep(poll_interval)


async def _run_schedule_loop() -> None:  # noqa: DBOS004 - event loop function
    while True:
        try:
            if not _DBOS_INITIALIZED:
                logger.warning("DBOS not initialized, skipping schedule loop iteration")
                await asyncio.sleep(SCHEDULE_POLL_SECONDS)
                continue

            interval_minutes = load_schedule_interval_minutes()
            last_run = last_completed_at(SCHEDULE_WORKFLOW_NAME) or 0
            interval_ms = interval_minutes * 60 * 1000
            now = now_ms()
            if now - last_run >= interval_ms:
                started_at = now

                try:
                    result = enqueue_scheduled_listings()
                    match result:
                        case Success(value=data):
                            if data.skipped_pending_details:
                                logger.info(
                                    "Skipping listing schedule; detail queue has pending items.",
                                )
                            else:
                                record_run(
                                    workflow_name=SCHEDULE_WORKFLOW_NAME,
                                    queue_name="listing",
                                    status="completed",
                                    started_at=started_at,
                                    completed_at=now_ms(),
                                )
                                logger.info(
                                    "Scheduled listing enqueue: queued=%d sites=%d",
                                    data.queued,
                                    data.sites_processed,
                                )
                        case Failure(error_type=error_type, message=message):
                            logger.error(
                                "Scheduled listing enqueue failed (non-retryable) [%s]: %s",
                                error_type,
                                message,
                            )
                            record_run(
                                workflow_name=SCHEDULE_WORKFLOW_NAME,
                                queue_name="listing",
                                status="failed",
                                error=f"[{error_type}] {message}",
                                started_at=started_at,
                                completed_at=now_ms(),
                            )
                except Exception as exc:
                    record_run(
                        workflow_name=SCHEDULE_WORKFLOW_NAME,
                        queue_name="listing",
                        status="failed",
                        error=str(exc),
                        started_at=started_at,
                        completed_at=now_ms(),
                    )
                    logger.exception("Scheduled listing enqueue failed: %s", exc)
        except Exception as exc:
            logger.exception("Schedule loop error: %s", exc)
        await asyncio.sleep(SCHEDULE_POLL_SECONDS)


async def run_worker(  # noqa: DBOS004 - event loop function
    *,
    listing_batch: int,
    detail_batch: int,
    listing_poll: float,
    detail_poll: float,
    listing_concurrency: int,
    detail_concurrency: int,
) -> None:
    initialize_schema()

    # Initialize DBOS (required for workflow processing)
    logger.info("Initializing DBOS workflows")
    _initialize_dbos()

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
