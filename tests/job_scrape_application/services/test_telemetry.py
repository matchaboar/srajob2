from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.services import telemetry  # noqa: E402


@pytest.fixture(autouse=True)
def reset_logger_state(monkeypatch):
    """Ensure module-level logger state does not leak between tests."""

    monkeypatch.setattr(telemetry, "_logger", None)
    monkeypatch.setattr(telemetry, "_logger_provider", None)


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
        def info(self, msg: str, extra: Dict[str, Any] | None = None):
            records.append({"msg": msg, "extra": extra or {}})

    monkeypatch.setattr(telemetry, "_ensure_logger", lambda: FakeLogger())

    payload = {"message": "hello", "event": "job.scraped", "count": 1}
    telemetry.emit_posthog_log(payload)

    assert len(records) == 1
    assert records[0]["msg"] == "hello"
    assert records[0]["extra"]["event"] == "job.scraped"
    assert records[0]["extra"]["count"] == 1
    assert records[0]["extra"]["scratchpad_message"] == "hello"


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
