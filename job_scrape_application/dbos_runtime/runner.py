from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from dbos import DBOS, DBOSConfig

# Cross-platform file locking
if sys.platform == "win32":
    import msvcrt

    @contextmanager
    def _file_lock(lock_path: Path) -> Generator[None, None, None]:
        """Windows file lock using msvcrt."""
        lock_path.touch(exist_ok=True)
        with open(lock_path, "w") as lock_file:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    @contextmanager
    def _file_lock(lock_path: Path) -> Generator[None, None, None]:
        """Unix file lock using fcntl."""
        lock_path.touch(exist_ok=True)
        with open(lock_path, "w") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

from .api import serve as serve_api
from .queue import queue_status
from .runs import last_completed_at, record_run
from .sqlite import _resolve_db_path, initialize_schema, now_ms
from .step import (
    load_schedule_interval_minutes,
    reset_schedule_cache,
    reset_sites_cache,
)
from .workflow_queues import DETAIL_QUEUE, LISTING_QUEUE
from ..services import telemetry
from ..workflows.result import Failure, Success
from ..workflows.workflow import enqueue_scheduled_listings

logger = logging.getLogger("dbos.runner")

_DBOS_INITIALIZED = False

SCHEDULE_WORKFLOW_NAME = "listing-schedule"
SCHEDULE_POLL_SECONDS = 60


def _setup_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    posthog_handler = telemetry.build_posthog_log_handler(level=logging.INFO)
    if posthog_handler is not None:
        handlers.append(posthog_handler)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)


def _reset_cache() -> None:
    """Reset schedule and sites caches."""
    reset_schedule_cache()
    reset_sites_cache()


def _resolve_executor_id(queue_name: str | None, override: str | None) -> str:
    if override:
        return override
    env_executor = os.getenv("DBOS_EXECUTOR_ID")
    if env_executor:
        return env_executor
    suffix = uuid.uuid4().hex[:8]
    name = queue_name or "worker"
    return f"{name}-{os.getpid()}-{suffix}"


def _resolve_listening_queues(queue_name: str) -> list:
    if queue_name == "listing":
        return [LISTING_QUEUE]
    if queue_name == "detail":
        return [DETAIL_QUEUE]
    if queue_name == "schedule":
        return []
    return [LISTING_QUEUE, DETAIL_QUEUE]


def _initialize_dbos(*, executor_id: str | None, listen_queues: list) -> None:
    """Initialize DBOS for workflow support.

    Uses a file lock to ensure only one process runs DBOS migrations at a time,
    preventing "table dbos_migrations already exists" race condition when
    multiple workers start simultaneously.
    """
    global _DBOS_INITIALIZED
    if _DBOS_INITIALIZED:
        return

    db_dir = _resolve_db_path().parent
    dbos_db_path = db_dir / "dbos_system.sqlite"
    lock_path = db_dir / ".dbos_init.lock"

    try:
        # Acquire exclusive lock to serialize DBOS initialization across processes
        logger.info("Acquiring DBOS initialization lock...")
        with _file_lock(lock_path):
            logger.info("Lock acquired, initializing DBOS...")
            config = DBOSConfig(
                name="job-scrape-worker",
                system_database_url=f"sqlite:///{dbos_db_path}",
                executor_id=executor_id,
            )
            DBOS(config=config)
            DBOS.listen_queues(listen_queues)
            DBOS.launch()
            _DBOS_INITIALIZED = True
            logger.info("DBOS initialized successfully")
    except Exception as exc:
        logger.warning("Failed to initialize DBOS: %s.", exc)
        _DBOS_INITIALIZED = False


def _run_schedule_loop() -> None:
    while True:
        try:
            if not _DBOS_INITIALIZED:
                logger.warning("DBOS not initialized, skipping schedule loop iteration")
                time.sleep(SCHEDULE_POLL_SECONDS)
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
        time.sleep(SCHEDULE_POLL_SECONDS)


def run_worker(*, queue_name: str, executor_id: str | None, run_schedule: bool) -> None:
    initialize_schema()

    listen_queues = _resolve_listening_queues(queue_name)
    resolved_executor_id = _resolve_executor_id(queue_name, executor_id)
    logger.info("Initializing DBOS workflows (executor_id=%s)", resolved_executor_id)
    _initialize_dbos(executor_id=resolved_executor_id, listen_queues=listen_queues)

    logger.info("DBOS queues starting: %s", queue_status())
    if run_schedule:
        _run_schedule_loop()
        return

    threading.Event().wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="DBOS workflow runner")
    parser.add_argument(
        "--queue",
        choices=["listing", "detail", "schedule", "all"],
        default="all",
        help="Queue this worker listens to.",
    )
    parser.add_argument("--executor-id", default=None)
    parser.add_argument("--with-schedule", action="store_true")
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

    run_schedule = args.with_schedule or args.queue in {"all", "schedule"}
    run_worker(queue_name=args.queue, executor_id=args.executor_id, run_schedule=run_schedule)


if __name__ == "__main__":
    main()
