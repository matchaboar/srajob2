from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from temporalio import workflow

from .scrape_workflow import ScrapeSummary

# Disable workflow sandbox for this module to allow activity imports that pull in HTTP clients.
__temporal_disable_workflow_sandbox__ = True
ACTIVITY_NAME = "process_pending_job_details_batch"
MAX_RUN_DURATION = timedelta(hours=1)
DEFAULT_TASK_DURATION = timedelta(seconds=30)
SAFETY_MARGIN = timedelta(seconds=5)
BATCH_LIMIT_DEFAULT = 100
BATCH_LIMIT_MAX = 200
BATCH_LIMIT_MIN = 25


class AssignmentAwareIterator:
    """Tracks observed task durations and decides if another iteration fits in the remaining window."""

    def __init__(
        self,
        max_duration: timedelta,
        *,
        default_task_duration: timedelta = DEFAULT_TASK_DURATION,
        safety_margin: timedelta = SAFETY_MARGIN,
    ):
        self.max_duration = max_duration
        self.default_task_duration = default_task_duration
        self.safety_margin = safety_margin
        self._started_at: datetime | None = None
        self._total_duration = timedelta()
        self._count = 0

    def mark_start(self, now: datetime) -> None:
        if self._started_at is None:
            self._started_at = now

    def record_task_duration(self, duration: timedelta) -> None:
        self._total_duration += duration
        self._count += 1

    def average_task_duration(self) -> timedelta:
        if self._count == 0:
            return self.default_task_duration
        return self._total_duration / self._count

    def remaining_time(self, now: datetime) -> timedelta:
        if self._started_at is None:
            return self.max_duration
        elapsed = now - self._started_at
        return max(timedelta(), self.max_duration - elapsed)

    def can_start_next(self, now: datetime) -> bool:
        remaining = self.remaining_time(now) - self.safety_margin
        if remaining <= timedelta():
            return False
        return remaining >= self.average_task_duration()

if TYPE_CHECKING:  # pragma: no cover
    from .activities import process_pending_job_details_batch  # noqa: F401


@workflow.defn(name="HeuristicJobDetails")
class HeuristicJobDetailsWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        processed_total = 0
        iterator = AssignmentAwareIterator(MAX_RUN_DURATION)
        workflow_start = workflow.now()
        iterator.mark_start(workflow_start)
        batch_limit = BATCH_LIMIT_DEFAULT
        # workflow.logger is provided by Temporal; fallback to stdlib in tests or if unavailable.
        try:
            logger = workflow.logger  # type: ignore[attr-defined]
        except Exception:
            logger = logging.getLogger("workflow.HeuristicJobDetails")
        try:
            while True:
                now = workflow.now()
                if not iterator.can_start_next(now):
                    break

                remaining = iterator.remaining_time(now)
                activity_timeout = min(remaining, MAX_RUN_DURATION)
                if activity_timeout <= iterator.safety_margin:
                    break

                task_start = workflow.now()
                res = await workflow.execute_activity(
                    ACTIVITY_NAME,
                    args=[25],
                    # Allow a long-running batch but cap at the remaining runtime window.
                    start_to_close_timeout=activity_timeout,
                )
                task_duration = workflow.now() - task_start
                iterator.record_task_duration(task_duration)

                count = res.get("processed") if isinstance(res, dict) else 0
                remaining = res.get("remaining") if isinstance(res, dict) else None
                fetched = res.get("fetched") if isinstance(res, dict) else None
                processed_total += count or 0
                if remaining is not None:
                    logger.info(
                        "heuristic.remaining rows=%s processed_total=%s fetched=%s",
                        remaining,
                        processed_total,
                        fetched,
                    )
                # Continue pulling batches while we still have time and there appears to be backlog.
                if iterator.can_start_next(workflow.now()):
                    if remaining is not None and remaining > 0:
                        batch_limit = max(BATCH_LIMIT_MIN, min(BATCH_LIMIT_MAX, int(remaining) if isinstance(remaining, int) else BATCH_LIMIT_DEFAULT))
                        continue
                    if fetched:
                        batch_limit = max(BATCH_LIMIT_MIN, min(BATCH_LIMIT_MAX, int(fetched)))
                        continue
                    if count:
                        # No remaining info but we updated rows; try another batch.
                        batch_limit = max(BATCH_LIMIT_MIN, min(BATCH_LIMIT_MAX, batch_limit))
                        continue
                # Nothing left or out of time.
                break
        except Exception:
            # Best-effort; avoid surfacing heuristic failures as workflow failures.
            pass

        return ScrapeSummary(site_count=0, scrape_ids=[])
