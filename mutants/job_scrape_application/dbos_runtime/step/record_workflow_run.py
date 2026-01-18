"""DBOS step function for recording workflow run metrics."""

from __future__ import annotations

from dbos import DBOS

from ..runs import record_run


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=0.5, backoff_rate=2.0)
def record_workflow_run_step(
    workflow_name: str,
    queue_name: str,
    status: str,
    started_at: int,
    completed_at: int,
    error: str | None = None,
) -> None:
    """Record a workflow run for metrics and observability.

    This step records workflow execution metadata to the SQLite runs table
    for tracking schedule adherence and debugging.

    Args:
        workflow_name: Name of the workflow (e.g., "listing-schedule")
        queue_name: Name of the queue ("listing" or "detail")
        status: Run status ("completed", "failed")
        started_at: Timestamp in milliseconds when run started
        completed_at: Timestamp in milliseconds when run completed
        error: Optional error message for failed runs
    """
    record_run(
        workflow_name=workflow_name,
        queue_name=queue_name,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        error=error,
    )
