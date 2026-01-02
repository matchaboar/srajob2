from __future__ import annotations

import logging
import re
import threading
import time
import traceback

from ..services import telemetry

_DEADLOCK_SIGNATURES = ("TMPRL1101", "Potential deadlock detected", "_DeadlockError")
_DEADLOCK_RUN_ID_RE = re.compile(r"run ID ([0-9a-f-]{8,})", re.IGNORECASE)
_DEADLOCK_WORKFLOW_ID_RE = re.compile(r"workflow ID ([0-9a-zA-Z_-]{4,})", re.IGNORECASE)
_DEADLOCK_YIELD_TIMEOUT_RE = re.compile(r"yield within (\d+) second", re.IGNORECASE)
_DEADLOCK_HINTS = [
    "Long synchronous workflow loops without await/workflow.sleep",
    "Large list/set/dict comprehensions or JSON parsing inside workflow code",
    "CPU-heavy work running in workflow instead of an activity",
    "Blocking I/O (requests, time.sleep, file reads) executed in workflow code",
]
_RUN_METADATA: dict[str, dict[str, object]] = {}
_RUN_METADATA_LOCK = threading.Lock()
_RUN_METADATA_MAX = 2000
_RUN_METADATA_PRUNE_TARGET = 1500


def _shrink_text(value: str | None, max_len: int = 2000) -> str | None:
    if value is None:
        return None
    if len(value) <= max_len:
        return value
    return f"{value[:max_len]}... (+{len(value) - max_len} chars)"


def _extract_ids(message: str) -> tuple[str | None, str | None]:
    run_id = None
    workflow_id = None
    run_match = _DEADLOCK_RUN_ID_RE.search(message)
    if run_match:
        run_id = run_match.group(1)
    workflow_match = _DEADLOCK_WORKFLOW_ID_RE.search(message)
    if workflow_match:
        workflow_id = workflow_match.group(1)
    return run_id, workflow_id


def _extract_timeout_seconds(message: str) -> int | None:
    match = _DEADLOCK_YIELD_TIMEOUT_RE.search(message)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def update_run_metadata(run_id: str | None, **fields: object) -> None:
    if not run_id:
        return
    with _RUN_METADATA_LOCK:
        payload = dict(_RUN_METADATA.get(run_id, {}))
        payload.update(fields)
        payload["recordedAt"] = time.time()
        _RUN_METADATA[run_id] = payload
        if len(_RUN_METADATA) > _RUN_METADATA_MAX:
            ordered = sorted(
                _RUN_METADATA.items(),
                key=lambda item: item[1].get("recordedAt", 0),
            )
            prune_count = len(_RUN_METADATA) - _RUN_METADATA_PRUNE_TARGET
            for key, _value in ordered[:prune_count]:
                _RUN_METADATA.pop(key, None)


def record_run_metadata(
    run_id: str | None,
    workflow_id: str | None,
    workflow_type: str | None,
    task_queue: str | None,
) -> None:
    update_run_metadata(
        run_id,
        workflowId=workflow_id,
        workflowType=workflow_type,
        taskQueue=task_queue,
    )


def _get_run_metadata(run_id: str | None) -> dict[str, object] | None:
    if not run_id:
        return None
    with _RUN_METADATA_LOCK:
        data = _RUN_METADATA.get(run_id)
        if not data:
            return None
        return dict(data)


class DeadlockLogHandler(logging.Handler):
    def __init__(self, context: dict[str, object]) -> None:
        super().__init__(level=logging.ERROR)
        self._context = context

    def _is_deadlock_record(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if any(sig in message for sig in _DEADLOCK_SIGNATURES):
            return True
        exc = record.exc_info[1] if record.exc_info else None
        if exc is None:
            return False
        if type(exc).__name__ == "_DeadlockError":
            return True
        exc_str = str(exc)
        return any(sig in exc_str for sig in _DEADLOCK_SIGNATURES)

    def emit(self, record: logging.LogRecord) -> None:
        if not self._is_deadlock_record(record):
            return

        message = record.getMessage()
        run_id, workflow_id = _extract_ids(message)
        run_metadata = _get_run_metadata(run_id)
        if not workflow_id and run_metadata:
            workflow_id = run_metadata.get("workflowId") if run_metadata else None
        exc_type = None
        exc_message = None
        stack = None
        if record.exc_info:
            exc_type = record.exc_info[0].__name__ if record.exc_info[0] else None
            exc_message = str(record.exc_info[1]) if record.exc_info[1] else None
            stack = _shrink_text("".join(traceback.format_exception(*record.exc_info)))

        payload = {
            "event": "temporal.deadlock",
            "level": "error",
            "message": message,
            "workflowId": workflow_id,
            "data": {
                "runId": run_id,
                "workflowId": workflow_id,
                "workflowType": run_metadata.get("workflowType") if run_metadata else None,
                "taskQueue": run_metadata.get("taskQueue") if run_metadata else None,
                "registryHit": bool(run_metadata),
                "lastActivity": run_metadata.get("lastActivity") if run_metadata else None,
                "lastActivityId": run_metadata.get("lastActivityId") if run_metadata else None,
                "lastActivityQueue": run_metadata.get("lastActivityQueue") if run_metadata else None,
                "lastActivityAt": run_metadata.get("lastActivityAt") if run_metadata else None,
                "lastChildWorkflow": run_metadata.get("lastChildWorkflow") if run_metadata else None,
                "lastChildWorkflowId": run_metadata.get("lastChildWorkflowId") if run_metadata else None,
                "lastChildWorkflowQueue": run_metadata.get("lastChildWorkflowQueue") if run_metadata else None,
                "lastChildWorkflowAt": run_metadata.get("lastChildWorkflowAt") if run_metadata else None,
                "logger": record.name,
                "level": record.levelname,
                "errorType": exc_type,
                "errorMessage": exc_message,
                "stack": stack,
                "yieldTimeoutSeconds": _extract_timeout_seconds(message),
                "possibleCauses": list(_DEADLOCK_HINTS),
                "processId": record.process,
                "threadName": record.threadName,
                "created": record.created,
                **self._context,
            },
        }

        try:
            telemetry.emit_posthog_log(payload)
        except Exception:
            return


def install_deadlock_posthog_handler(context: dict[str, object]) -> None:
    logger = logging.getLogger("temporalio.worker._workflow")
    for handler in logger.handlers:
        if isinstance(handler, DeadlockLogHandler):
            return
    logger.addHandler(DeadlockLogHandler(context))
