from __future__ import annotations

import logging
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


def emit_posthog_log(payload: Dict[str, Any]) -> None:
    """Send a structured log entry to PostHog via OTLP."""

    logger = _ensure_logger()

    message = payload.get("message") or payload.get("event") or "scratchpad"
    attributes = {k: v for k, v in payload.items() if k != "message"}
    if "message" in payload:
        attributes["scratchpad_message"] = payload["message"]

    logger.info(message, extra=attributes)


def force_flush_posthog_logs(timeout_ms: int = 30000) -> bool:
    if _logger_provider:
        return _logger_provider.force_flush(timeout_ms)
    return True

