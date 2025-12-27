from __future__ import annotations

import os
import sys
import types
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

if "convex" not in sys.modules:
    class _FakeConvexClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def query(self, *_args, **_kwargs):
            return None

        def mutation(self, *_args, **_kwargs):
            return None

    sys.modules["convex"] = types.SimpleNamespace(ConvexClient=_FakeConvexClient)
if "yaml" not in sys.modules:
    sys.modules["yaml"] = types.SimpleNamespace(safe_load=lambda *_args, **_kwargs: {})
if "opentelemetry" not in sys.modules:
    otel_mod = types.ModuleType("opentelemetry")
    otel_logs = types.SimpleNamespace(set_logger_provider=lambda *_args, **_kwargs: None)
    otel_mod._logs = otel_logs

    class _FakeLoggerProvider:
        def __init__(self, *_args, **_kwargs):
            self.processors = []

        def add_log_record_processor(self, proc):
            self.processors.append(proc)

        def force_flush(self, timeout_ms: int):
            return True

    class _FakeLoggingHandler:
        def __init__(self, level=None, logger_provider=None):
            self.logger_provider = logger_provider

    class _FakeBatchLogRecordProcessor:
        def __init__(self, exporter):
            self.exporter = exporter

        def force_flush(self, timeout_millis: int | None = None):
            return True

    class _FakeOTLPExporter:
        def __init__(self, endpoint: str | None = None, headers: Dict[str, str] | None = None):
            self.endpoint = endpoint
            self.headers = headers or {}

    sys.modules["opentelemetry"] = otel_mod
    sys.modules["opentelemetry._logs"] = types.SimpleNamespace(set_logger_provider=lambda *_a, **_k: None)
    sys.modules["opentelemetry.exporter.otlp.proto.http._log_exporter"] = types.SimpleNamespace(
        OTLPLogExporter=_FakeOTLPExporter
    )
    sys.modules["opentelemetry.sdk._logs"] = types.SimpleNamespace(
        LoggerProvider=_FakeLoggerProvider, LoggingHandler=_FakeLoggingHandler
    )
    sys.modules["opentelemetry.sdk._logs.export"] = types.SimpleNamespace(
        BatchLogRecordProcessor=_FakeBatchLogRecordProcessor
    )
if "posthog" not in sys.modules:
    class _FakePosthog:
        def __init__(self, api_key: str, host: str | None = None, enable_exception_autocapture: bool = False):
            self.api_key = api_key
            self.host = host
            self.enable_exception_autocapture = enable_exception_autocapture
            self.captured: List[Dict[str, Any]] = []

        def capture_exception(self, exc, distinct_id=None, properties=None):
            self.captured.append(
                {
                    "exc": exc,
                    "distinct_id": distinct_id,
                    "properties": properties or {},
                }
            )

    sys.modules["posthog"] = types.SimpleNamespace(Posthog=_FakePosthog)

from job_scrape_application.services import telemetry  # noqa: E402


@pytest.fixture(autouse=True)
def reset_logger_state(monkeypatch):
    """Ensure module-level logger state does not leak between tests."""

    monkeypatch.setattr(telemetry, "_logger", None)
    monkeypatch.setattr(telemetry, "_logger_provider", None)
    monkeypatch.setattr(telemetry, "_posthog_client", None)


def test_resolve_endpoint_prefers_explicit_override(monkeypatch):
    monkeypatch.setattr(telemetry.settings, "posthog_logs_endpoint", "https://custom.example.com/logs/")
    monkeypatch.setattr(telemetry.settings, "posthog_region", "EU Cloud")

    assert telemetry._resolve_endpoint() == "https://custom.example.com/logs"


def test_resolve_endpoint_falls_back_to_region(monkeypatch):
    monkeypatch.setattr(telemetry.settings, "posthog_logs_endpoint", None)
    monkeypatch.setattr(telemetry.settings, "posthog_region", "EU Cloud")

    assert telemetry._resolve_endpoint() == "https://eu.i.posthog.com/i/v1/logs"


def test_resolve_endpoint_defaults_to_us(monkeypatch):
    monkeypatch.setattr(telemetry.settings, "posthog_logs_endpoint", None)
    monkeypatch.setattr(telemetry.settings, "posthog_region", None)

    assert telemetry._resolve_endpoint() == telemetry.DEFAULT_POSTHOG_ENDPOINT


def test_resolve_posthog_host_from_logs_endpoint(monkeypatch):
    monkeypatch.setattr(telemetry.settings, "posthog_logs_endpoint", "https://eu.i.posthog.com/i/v1/logs")
    monkeypatch.setattr(telemetry.settings, "posthog_region", "US")

    assert telemetry._resolve_posthog_host() == "https://eu.i.posthog.com"


def test_ensure_logger_requires_api_key(monkeypatch):
    monkeypatch.setattr(telemetry.settings, "posthog_project_api_key", None)

    with pytest.raises(RuntimeError, match="POSTHOG_PROJECT_API_KEY is not configured"):
        telemetry._ensure_logger()


def test_ensure_logger_builds_exporter_with_auth_header(monkeypatch):
    captured: Dict[str, Any] = {}

    class FakeExporter:
        def __init__(self, endpoint: str | None = None, headers: Dict[str, str] | None = None):
            captured["endpoint"] = endpoint
            captured["headers"] = headers or {}

        def export(self, batch: Any):  # pragma: no cover - unused
            return None

        def shutdown(self):  # pragma: no cover - unused
            return None

        def force_flush(self, timeout_millis: int | None = None):  # pragma: no cover - unused
            return True

    class FakeProcessor:
        def __init__(self, exporter: FakeExporter):
            captured["processor_exporter"] = exporter

        def force_flush(self, timeout_millis: int | None = None):  # pragma: no cover - unused
            captured["processor_force_flush"] = timeout_millis
            return True

        def shutdown(self):  # pragma: no cover - unused
            return None

    monkeypatch.setattr(telemetry, "OTLPLogExporter", FakeExporter)
    monkeypatch.setattr(telemetry, "BatchLogRecordProcessor", FakeProcessor)
    monkeypatch.setattr(telemetry.settings, "posthog_project_api_key", "token-abc")
    monkeypatch.setattr(telemetry.settings, "posthog_logs_endpoint", "https://logs.example.com")

    logger = telemetry._ensure_logger()

    assert logger is telemetry._logger
    assert captured["endpoint"] == "https://logs.example.com"
    assert captured["headers"] == {"Authorization": "Bearer token-abc"}
    assert isinstance(captured["processor_exporter"], FakeExporter)


def test_emit_posthog_log_formats_message_and_attributes(monkeypatch):
    records: List[Dict[str, Any]] = []

    class FakeLogger:
        def info(self, msg: str, extra: Dict[str, Any] | None = None, **kwargs):
            records.append({"msg": msg, "extra": extra or {}})

    monkeypatch.setattr(telemetry, "_ensure_logger", lambda: FakeLogger())
    monkeypatch.setattr(telemetry, "_infer_workflow_id", lambda: None)

    payload = {"message": "hello", "event": "job.scraped", "count": 1}
    telemetry.emit_posthog_log(payload)

    assert len(records) == 1
    assert records[0]["msg"] == "hello"
    assert records[0]["extra"]["event"] == "job.scraped"
    assert records[0]["extra"]["count"] == 1
    assert records[0]["extra"]["scratchpad_message"] == "hello"


def test_emit_posthog_log_appends_workflow_id(monkeypatch):
    records: List[Dict[str, Any]] = []

    class FakeLogger:
        def info(self, msg: str, extra: Dict[str, Any] | None = None, **kwargs):
            records.append({"msg": msg, "extra": extra or {}})

    monkeypatch.setattr(telemetry, "_ensure_logger", lambda: FakeLogger())

    payload = {"event": "job.scraped", "workflowId": "wf-789"}
    telemetry.emit_posthog_log(payload)

    assert "workflow_id=wf-789" in records[0]["msg"]
    assert records[0]["extra"]["workflowId"] == "wf-789"


def test_emit_posthog_log_infers_workflow_id(monkeypatch):
    records: List[Dict[str, Any]] = []

    class FakeLogger:
        def info(self, msg: str, extra: Dict[str, Any] | None = None, **kwargs):
            records.append({"msg": msg, "extra": extra or {}})

    monkeypatch.setattr(telemetry, "_ensure_logger", lambda: FakeLogger())
    monkeypatch.setattr(telemetry, "_infer_workflow_id", lambda: "wf-auto")

    telemetry.emit_posthog_log({"event": "job.scraped"})

    assert "workflow_id=wf-auto" in records[0]["msg"]
    assert records[0]["extra"]["workflowId"] == "wf-auto"


def test_emit_posthog_log_preserves_caller_location(monkeypatch):
    import logging
    records: List[Any] = []

    class ListHandler(logging.Handler):
        def emit(self, record):  # type: ignore[override]
            records.append(record)

    logger = logging.getLogger("telemetry-location-test")
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(ListHandler())

    monkeypatch.setattr(telemetry, "_ensure_logger", lambda: logger)

    telemetry.emit_posthog_log({"event": "unit.test.location"})

    assert records, "expected a log record"
    record = records[0]
    assert record.pathname.endswith("test_telemetry.py")
    assert record.funcName == "test_emit_posthog_log_preserves_caller_location"
    assert record.lineno > 0


def test_emit_posthog_exception_uses_client(monkeypatch):
    captured: Dict[str, Any] = {}

    class FakeClient:
        def capture_exception(self, exc, distinct_id=None, properties=None):
            captured["exc"] = exc
            captured["distinct_id"] = distinct_id
            captured["properties"] = properties or {}

    monkeypatch.setattr(telemetry, "_ensure_posthog_client", lambda: FakeClient())
    monkeypatch.setattr(telemetry, "_infer_workflow_id", lambda: "wf-123")

    telemetry.emit_posthog_exception(ValueError("boom"))

    assert isinstance(captured["exc"], ValueError)
    assert captured["distinct_id"] == "wf-123"
    assert captured["properties"]["workflowId"] == "wf-123"


def test_force_flush_uses_provider(monkeypatch):
    class FakeProvider:
        def __init__(self):
            self.calls: List[int | None] = []

        def force_flush(self, timeout_ms: int):
            self.calls.append(timeout_ms)
            return True

    provider = FakeProvider()
    monkeypatch.setattr(telemetry, "_logger_provider", provider)

    assert telemetry.force_flush_posthog_logs(timeout_ms=1234) is True
    assert provider.calls == [1234]


def test_force_flush_is_noop_without_provider(monkeypatch):
    monkeypatch.setattr(telemetry, "_logger_provider", None)

    assert telemetry.force_flush_posthog_logs(timeout_ms=500) is True
