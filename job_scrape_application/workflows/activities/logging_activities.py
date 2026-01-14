"""Logging and metrics activities for workflow tracking."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

from temporalio import activity

from ...dbos_runtime import queue as dbos_queue
from ...services import telemetry

logger = logging.getLogger("temporal.worker.activities")


def _coerce_workflow_id(entry: Dict[str, Any]) -> str:
    """Best-effort extraction of a workflow id for logging/filtering."""
    candidates = [
        entry.get("workflowId"),
        entry.get("workflow_id"),
        (entry.get("data") or {}).get("workflowId") if isinstance(entry.get("data"), dict) else None,
        (entry.get("data") or {}).get("workflow_id") if isinstance(entry.get("data"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return "unknown"


def _short_preview(value: Any) -> str:
    """Return a concise preview for message strings."""
    if value is None:
        return "none"
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, str):
        return value[:120]
    if isinstance(value, list):
        return f"len={len(value)}"
    if isinstance(value, dict):
        return ", ".join(
            f"{k}={_short_preview(v)}"
            for k, v in list(value.items())[:4]
            if v is not None
        )
    return str(value)[:120]


def _build_log_message(payload: Dict[str, Any]) -> str:
    """Compose a descriptive message that always includes workflow id."""
    event = payload.get("event")
    site_url = payload.get("siteUrl") or payload.get("sourceUrl")
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    workflow_id = _coerce_workflow_id(payload)

    base = payload.get("message")
    parts: List[str] = []
    if base:
        parts.append(str(base))
    elif event:
        parts.append(event.replace("_", " "))

    if site_url:
        parts.append(f"site={site_url}")

    interesting_keys = (
        "jobId",
        "jobsScraped",
        "jobUrls",
        "itemsCount",
        "normalizedCount",
        "urls",
        "count",
        "sitesProcessed",
        "stored",
        "failed",
        "remaining",
        "toScrape",
        "status",
        "provider",
        "pattern",
    )
    details: List[str] = []
    for key in interesting_keys:
        if key in data and data[key] is not None:
            details.append(f"{key}={_short_preview(data[key])}")

    if data.get("sample"):
        sample_title = None
        if isinstance(data["sample"], list):
            for entry in data["sample"]:
                if isinstance(entry, dict):
                    sample_title = entry.get("title") or entry.get("job_title")
                    if sample_title:
                        break
        if sample_title:
            details.append(f"sample_title={_short_preview(sample_title)}")

    if details:
        parts.append(", ".join(details))

    if workflow_id:
        parts.append(f"workflow_id={workflow_id}")

    if not parts:
        return f"{event or 'workflow.log'} | workflow_id={workflow_id}"

    return " | ".join(parts)


@activity.defn
async def record_workflow_run(run: Dict[str, Any]) -> None:
    """Record a workflow run completion to DBOS."""
    from ...dbos_runtime.runs import record_run

    try:
        workflow_name = run.get("workflowName") or "unknown"
        queue_name = run.get("taskQueue") or run.get("queue") or "dbos"
        status = run.get("status") or "completed"
        started_at = run.get("startedAt") if isinstance(run.get("startedAt"), (int, float)) else None
        completed_at = run.get("completedAt") if isinstance(run.get("completedAt"), (int, float)) else None
        error = run.get("error") if isinstance(run.get("error"), str) else None
        record_run(
            workflow_name=str(workflow_name),
            queue_name=str(queue_name),
            status=str(status),
            started_at=int(started_at) if started_at is not None else None,
            completed_at=int(completed_at) if completed_at is not None else None,
            error=error,
        )
    except asyncio.CancelledError:
        return None
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to record workflow run: {e}") from e


@activity.defn(name="record_workflow_checkpoint")
async def record_workflow_checkpoint(payload: Dict[str, Any]) -> None:
    """Record a workflow checkpoint for debugging."""
    from ..deadlock_logging import update_run_metadata

    if not isinstance(payload, dict):
        return None

    run_id = payload.get("runId")
    fields = {k: v for k, v in payload.items() if k != "runId"}
    update_run_metadata(run_id, **fields)


@activity.defn
async def record_throughput_metrics(window_seconds: int = 60) -> Dict[str, Any]:
    """
    Calculate and log throughput metrics for job detail scraping.

    This activity tracks:
    - URLs processed per minute (throughput)
    - Queue depth (pending, processing)
    - Failed URLs in window
    - Current system utilization

    Args:
        window_seconds: Time window in seconds to calculate metrics over (default: 60)

    Returns:
        Dictionary containing throughput metrics and queue status
    """
    from ...dbos_runtime.sqlite import transaction, now_ms

    log = activity.logger

    now = now_ms()
    window_start = now - (window_seconds * 1000)

    with transaction() as conn:
        # URLs completed in window
        completed_row = conn.execute(
            """
            SELECT COUNT(*) as count FROM queue_items
            WHERE queue_name = ? AND status = ? AND completed_at >= ?
            """,
            (dbos_queue.QUEUE_DETAIL, dbos_queue.STATUS_COMPLETED, window_start)
        ).fetchone()
        completed = completed_row["count"] if completed_row else 0

        # Current queue state
        processing_row = conn.execute(
            """
            SELECT COUNT(*) as count FROM queue_items
            WHERE queue_name = ? AND status = ?
            """,
            (dbos_queue.QUEUE_DETAIL, dbos_queue.STATUS_PROCESSING)
        ).fetchone()
        processing = processing_row["count"] if processing_row else 0

        pending_row = conn.execute(
            """
            SELECT COUNT(*) as count FROM queue_items
            WHERE queue_name = ? AND status = ?
            """,
            (dbos_queue.QUEUE_DETAIL, dbos_queue.STATUS_PENDING)
        ).fetchone()
        pending = pending_row["count"] if pending_row else 0

        # Failed in window
        failed_row = conn.execute(
            """
            SELECT COUNT(*) as count FROM queue_items
            WHERE queue_name = ? AND status = ? AND completed_at >= ?
            """,
            (dbos_queue.QUEUE_DETAIL, dbos_queue.STATUS_FAILED, window_start)
        ).fetchone()
        failed = failed_row["count"] if failed_row else 0

    # Calculate throughput
    throughput_per_min = (completed / window_seconds) * 60 if window_seconds > 0 else 0

    metrics = {
        "throughputPerMinute": round(throughput_per_min, 2),
        "completedInWindow": completed,
        "failedInWindow": failed,
        "currentlyProcessing": processing,
        "pending": pending,
        "windowSeconds": window_seconds,
        "timestamp": now,
    }

    # Emit to telemetry
    telemetry.emit_posthog_log({
        "event": "throughput.metrics",
        "level": "info",
        **metrics
    })

    # Log summary
    log.info(
        f"Throughput: {throughput_per_min:.1f} URLs/min | "
        f"Completed: {completed} | Failed: {failed} | "
        f"Queue: {pending} pending, {processing} processing"
    )

    return metrics
