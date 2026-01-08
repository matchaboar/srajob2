from __future__ import annotations

import inspect
import os

from temporalio import workflow

from ..deadlock_logging import update_run_metadata


def workflow_checkpoint(label: str) -> None:
    """Record a workflow-side checkpoint to aid deadlock diagnostics."""

    try:
        run_id = workflow.info().run_id
    except Exception:
        run_id = None

    location = None
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            caller = frame.f_back
            filename = caller.f_code.co_filename
            basename = os.path.basename(filename)
            location = f"{basename}:{caller.f_lineno}"
    except Exception:
        location = None

    try:
        now = workflow.now().isoformat()
    except Exception:
        now = None

    try:
        with workflow.unsafe.sandbox_unrestricted():
            update_run_metadata(
                run_id,
                lastCheckpoint=label,
                lastCheckpointLocation=location,
                lastCheckpointAt=now,
            )
    except Exception:
        return
