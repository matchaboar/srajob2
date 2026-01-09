from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict

from temporalio import workflow


async def workflow_checkpoint(
    label: str,
    *,
    location: str | None = None,
    data: Dict[str, Any] | None = None,
) -> None:
    """Record a workflow-side checkpoint to aid deadlock diagnostics."""

    try:
        info = workflow.info()
        run_id = info.run_id
        workflow_id = info.workflow_id
        workflow_type = info.workflow_type
        task_queue = info.task_queue
    except Exception:
        run_id = None
        workflow_id = None
        workflow_type = None
        task_queue = None

    try:
        now = workflow.now().isoformat()
    except Exception:
        now = None

    payload = {
        "runId": run_id,
        "lastCheckpoint": label,
        "lastCheckpointLocation": location,
        "lastCheckpointData": data,
        "lastCheckpointAt": now,
        "workflowId": workflow_id,
        "workflowType": workflow_type,
        "taskQueue": task_queue,
    }

    try:
        await workflow.execute_local_activity(
            "record_workflow_checkpoint",
            args=[payload],
            start_to_close_timeout=timedelta(seconds=5),
        )
    except Exception:
        return
