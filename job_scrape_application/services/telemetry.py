from __future__ import annotations

import importlib
import logging
import os
import sys
from typing import Any, Dict
from urllib.parse import urlparse

from opentelemetry import _logs as logs
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

from ..config import settings

try:
    from posthog import Posthog
except Exception:  # pragma: no cover - optional dependency in constrained environments
    Posthog = None  # type: ignore[assignment]

DEFAULT_POSTHOG_ENDPOINT = "https://us.i.posthog.com/i/v1/logs"

_logger_provider: LoggerProvider | None = None
_logger: logging.Logger | None = None
_posthog_client: Posthog | None = None  # type: ignore[valid-type]
_posthog_log_handler: LoggingHandler | None = None
_posthog_log_configured: bool = False


def _resolve_endpoint() -> str:
    if settings.posthog_logs_endpoint:
        return settings.posthog_logs_endpoint.rstrip("/")

    region = (settings.posthog_region or "").lower()
    if region.startswith("eu"):
        return "https://eu.i.posthog.com/i/v1/logs"

    return DEFAULT_POSTHOG_ENDPOINT


def _resolve_posthog_host() -> str:
    endpoint = _resolve_endpoint()
    parsed = urlparse(endpoint)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    region = (settings.posthog_region or "").lower()
    if region.startswith("eu"):
        return "https://eu.i.posthog.com"
    return "https://us.i.posthog.com"


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


def _posthog_sdk_disabled() -> bool:
    if settings.posthog_disabled:
        return True
    if os.getenv("PYTEST_CURRENT_TEST") or "pytest" in sys.modules:
        return True
    return False


def _ensure_posthog_client() -> Posthog | None:  # type: ignore[valid-type]
    global _posthog_client

    if _posthog_client is not None:
        return _posthog_client

    if _posthog_sdk_disabled():
        return None
    if not Posthog:
        return None
    token = settings.posthog_project_api_key
    if not token:
        return None

    host = _resolve_posthog_host()
    _posthog_client = Posthog(
        token,
        host=host,
        enable_exception_autocapture=settings.posthog_exception_autocapture,
        capture_exception_code_variables=settings.posthog_capture_exception_code_variables,
    )
    return _posthog_client


def _configure_posthog_logger(token: str) -> LoggerProvider:
    global _logger_provider, _posthog_log_configured

    if _logger_provider is None:
        _logger_provider = LoggerProvider()
        logs.set_logger_provider(_logger_provider)

    if not _posthog_log_configured:
        endpoint = _resolve_endpoint()
        exporter = _build_otlp_exporter(endpoint, token)
        _logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
        _posthog_log_configured = True

    return _logger_provider


def build_posthog_log_handler(level: int = logging.INFO) -> LoggingHandler | None:
    """Return a LoggingHandler that ships records to PostHog via OTLP."""
    global _posthog_log_handler

    if _posthog_log_handler is not None:
        return _posthog_log_handler

    token = settings.posthog_project_api_key
    if not token:
        return None

    provider = _configure_posthog_logger(token)
    _posthog_log_handler = LoggingHandler(level=level, logger_provider=provider)
    return _posthog_log_handler


def _ensure_logger() -> logging.Logger:
    global _logger

    if _logger:
        return _logger

    token = settings.posthog_project_api_key
    if not token:
        raise RuntimeError("POSTHOG_PROJECT_API_KEY is not configured")

    provider = _configure_posthog_logger(token)
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


def emit_posthog_exception(
    exc: BaseException,
    *,
    distinct_id: str | None = None,
    properties: Dict[str, Any] | None = None,
) -> None:
    client = _ensure_posthog_client()
    if not client:
        return

    workflow_id = distinct_id or _infer_workflow_id() or "scraper-worker"
    payload: Dict[str, Any] = {"level": "error", "workflowId": workflow_id}
    if properties:
        payload.update(properties)
    payload.setdefault("exceptionType", type(exc).__name__)
    payload.setdefault("exceptionMessage", str(exc))

    try:
        client.capture_exception(exc, distinct_id=workflow_id, properties=payload)
    except Exception:
        # best-effort; never fail the caller on telemetry issues
        return


def force_flush_posthog_logs(timeout_ms: int = 30000) -> bool:
    if _logger_provider:
        return _logger_provider.force_flush(timeout_ms)
    return True


def initialize_posthog_exception_tracking() -> bool:
    """Initialize PostHog client for exception autocapture; returns True when enabled."""

    client = _ensure_posthog_client()
    return client is not None
