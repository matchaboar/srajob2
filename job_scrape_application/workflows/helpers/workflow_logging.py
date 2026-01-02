from __future__ import annotations

import logging

from temporalio import workflow


def get_workflow_logger() -> logging.Logger:
    """Return a workflow-aware logger, falling back outside a workflow loop."""

    logger = workflow.logger  # type: ignore[attr-defined]
    try:
        is_enabled_for = getattr(logger, "isEnabledFor", None)
        if callable(is_enabled_for):
            is_enabled_for(logging.INFO)
    except Exception:
        return logging.getLogger("temporalio.workflow")
    return logger
