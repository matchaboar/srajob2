from __future__ import annotations

import logging
import os
import sys
import types
from typing import Any, Dict, List

if "opentelemetry" not in sys.modules:
    otel_mod = types.ModuleType("opentelemetry")
    otel_logs = types.SimpleNamespace(set_logger_provider=lambda *_args, **_kwargs: None)
    otel_mod._logs = otel_logs

    class _FakeLoggerProvider:
        def __init__(self, *_args, **_kwargs):
            pass

        def add_log_record_processor(self, *_args, **_kwargs):
            return None

    class _FakeLoggingHandler:
        def __init__(self, level=None, logger_provider=None):
            self.logger_provider = logger_provider

    class _FakeBatchLogRecordProcessor:
        def __init__(self, exporter):
            self.exporter = exporter

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

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import deadlock_logging


def _make_record(message: str, *, exc_info=None) -> logging.LogRecord:
    return logging.LogRecord(
        name="temporalio.worker._workflow",
        level=logging.ERROR,
        pathname=__file__,
        lineno=42,
        msg=message,
        args=(),
        exc_info=exc_info,
    )


def test_deadlock_handler_emits_payload_with_ids_and_timeout(monkeypatch):
    recorded: List[Dict[str, Any]] = []

    def fake_emit(payload: Dict[str, Any]) -> None:
        recorded.append(payload)

    monkeypatch.setattr(deadlock_logging.telemetry, "emit_posthog_log", fake_emit)

    handler = deadlock_logging.DeadlockLogHandler({"workerId": "worker-1"})
    message = (
        "Failed handling activation on workflow with run ID 019b7612-7612-7629-bd10-cf95eec8d698 "
        "workflow ID wf-scraper-123: [TMPRL1101] Potential deadlock detected: workflow didn't yield within 2 second(s)."
    )
    handler.emit(_make_record(message))

    assert recorded, "expected a PostHog payload"
    payload = recorded[0]
    assert payload["event"] == "temporal.deadlock"
    assert payload["data"]["runId"] == "019b7612-7612-7629-bd10-cf95eec8d698"
    assert payload["data"]["workflowId"] == "wf-scraper-123"
    assert payload["data"]["yieldTimeoutSeconds"] == 2
    assert payload["data"]["workerId"] == "worker-1"
    assert payload["data"]["possibleCauses"]


def test_deadlock_handler_enriches_from_registry(monkeypatch):
    recorded: List[Dict[str, Any]] = []

    def fake_emit(payload: Dict[str, Any]) -> None:
        recorded.append(payload)

    monkeypatch.setattr(deadlock_logging.telemetry, "emit_posthog_log", fake_emit)

    run_id = "019b7612-7612-7629-bd10-cf95eec8d698"
    deadlock_logging.record_run_metadata(
        run_id,
        "wf-enriched",
        "ScraperSpidercloud",
        "scraper-task-queue",
    )

    handler = deadlock_logging.DeadlockLogHandler({})
    message = (
        "Failed handling activation on workflow with run ID 019b7612-7612-7629-bd10-cf95eec8d698: "
        "[TMPRL1101] Potential deadlock detected: workflow didn't yield within 2 second(s)."
    )
    handler.emit(_make_record(message))

    assert recorded, "expected a PostHog payload"
    payload = recorded[0]
    assert payload["workflowId"] == "wf-enriched"
    assert payload["data"]["workflowType"] == "ScraperSpidercloud"
    assert payload["data"]["taskQueue"] == "scraper-task-queue"
    assert payload["data"]["registryHit"] is True


def test_deadlock_handler_emits_when_exception_type_matches(monkeypatch):
    recorded: List[Dict[str, Any]] = []

    def fake_emit(payload: Dict[str, Any]) -> None:
        recorded.append(payload)

    monkeypatch.setattr(deadlock_logging.telemetry, "emit_posthog_log", fake_emit)

    class _DeadlockError(Exception):
        pass

    try:
        raise _DeadlockError("Potential deadlock detected")
    except _DeadlockError:
        exc_info = sys.exc_info()

    handler = deadlock_logging.DeadlockLogHandler({"workerRole": "all"})
    handler.emit(_make_record("Workflow task failed", exc_info=exc_info))

    assert recorded, "expected a PostHog payload"
    payload = recorded[0]
    assert payload["data"]["errorType"] == "_DeadlockError"
    assert "Potential deadlock detected" in (payload["data"]["errorMessage"] or "")
    assert payload["data"]["workerRole"] == "all"


def test_deadlock_handler_skips_non_deadlock(monkeypatch):
    recorded: List[Dict[str, Any]] = []

    def fake_emit(payload: Dict[str, Any]) -> None:
        recorded.append(payload)

    monkeypatch.setattr(deadlock_logging.telemetry, "emit_posthog_log", fake_emit)

    handler = deadlock_logging.DeadlockLogHandler({})
    handler.emit(_make_record("Something else happened"))

    assert recorded == []
