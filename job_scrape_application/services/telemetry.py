from __future__ import annotations

import logging
import importlib
from typing import Any, Dict

from opentelemetry import _logs as logs
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

from ..config import settings

DEFAULT_POSTHOG_ENDPOINT = "https://us.i.posthog.com/i/v1/logs"

_logger_provider: LoggerProvider | None = None
_logger: logging.Logger | None = None


def _resolve_endpoint() -> str:
    if settings.posthog_logs_endpoint:
        return settings.posthog_logs_endpoint.rstrip("/")

    region = (settings.posthog_region or "").lower()
    if region.startswith("eu"):
        return "https://eu.i.posthog.com/i/v1/logs"

    return DEFAULT_POSTHOG_ENDPOINT


def _build_otlp_exporter(endpoint: str, token: str) -> OTLPLogExporter:
    return OTLPLogExporter(endpoint=endpoint, headers={"Authorization": f"Bearer {token}"})


def _infer_workflow_id() -> str | None:
    """Best-effort: pull workflow_id from Temporal workflow/activity context if present."""

    candidates = [
        ("temporalio.workflow", "info", "workflow_id"),
        ("temporalio.activity", "info", "workflow_id"),
    ]
    for module_name, func_name, attr in candidates:
        try:
            mod = importlib.import_module(module_name)
            info_fn = getattr(mod, func_name, None)
            if not callable(info_fn):
                continue
            run_info = info_fn()
            wf_id = getattr(run_info, attr, None) or getattr(run_info, "workflowId", None)
            if isinstance(wf_id, str) and wf_id.strip():
                return wf_id
        except Exception:
            continue
    return None


def _ensure_logger() -> logging.Logger:
    global _logger, _logger_provider

    if _logger:
        return _logger

    token = settings.posthog_project_api_key
    if not token:
        raise RuntimeError("POSTHOG_PROJECT_API_KEY is not configured")

    endpoint = _resolve_endpoint()

    provider = LoggerProvider()
    logs.set_logger_provider(provider)

    exporter = _build_otlp_exporter(endpoint, token)
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

    handler = LoggingHandler(level=logging.INFO, logger_provider=provider)
    logger = logging.getLogger("scratchpad")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Avoid duplicating OTLP handlers when the function is called multiple times.
    logger.handlers = [h for h in logger.handlers if not isinstance(h, LoggingHandler)]
    logger.addHandler(handler)

    _logger_provider = provider
    _logger = logger
    return logger


def _normalize_log_level(level: Any) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        normalized = level.strip().lower()
        if normalized in {"warn", "warning"}:
            return logging.WARNING
        if normalized == "error":
            return logging.ERROR
        if normalized == "debug":
            return logging.DEBUG
        if normalized == "critical":
            return logging.CRITICAL
    return logging.INFO


def emit_posthog_log(payload: Dict[str, Any]) -> None:
    """Send a structured log entry to PostHog via OTLP."""

    logger = _ensure_logger()

    workflow_id = (
        payload.get("workflowId")
        or payload.get("workflow_id")
        or (payload.get("data") or {}).get("workflowId")
        or (payload.get("data") or {}).get("workflow_id")
        or _infer_workflow_id()
    )

    message = payload.get("message") or payload.get("event") or "scratchpad"
    if workflow_id and f"workflow_id={workflow_id}" not in str(message):
        message = f"{message} | workflow_id={workflow_id}"

    attributes = {k: v for k, v in payload.items() if k != "message"}
    if workflow_id and "workflowId" not in attributes:
        attributes["workflowId"] = workflow_id
    if "message" in payload:
        attributes["scratchpad_message"] = payload["message"]

    level = _normalize_log_level(payload.get("level"))
    # Use stacklevel so OTLP location fields point to the caller of emit_posthog_log,
    # not this helper module.
    if hasattr(logger, "log"):
        logger.log(level, message, extra=attributes, stacklevel=2)
    else:
        logger.info(message, extra=attributes)


def force_flush_posthog_logs(timeout_ms: int = 30000) -> bool:
    if _logger_provider:
        return _logger_provider.force_flush(timeout_ms)
    return True
