from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest


def _disable_fetchfox_firecrawl_env() -> None:
    os.environ.setdefault("ENABLE_FIRECRAWL", "false")
    os.environ.setdefault("ENABLE_FETCHFOX", "false")


def _ensure_fetchfox_stub() -> None:
    if "fetchfox_sdk" in sys.modules:
        return
    fetchfox_mod = types.ModuleType("fetchfox_sdk")

    class _FakeFetchFox:  # minimal stub for import compatibility
        def __init__(self, *args, **kwargs):
            pass

    fetchfox_mod.FetchFox = _FakeFetchFox
    sys.modules["fetchfox_sdk"] = fetchfox_mod


def _ensure_firecrawl_stub() -> None:
    if "firecrawl" in sys.modules:
        return

    firecrawl_mod = types.ModuleType("firecrawl")

    class _FakeFirecrawl:  # minimal stub for import compatibility
        def __init__(self, *args, **kwargs):
            pass

    firecrawl_mod.Firecrawl = _FakeFirecrawl

    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")

    class _FakePaginationConfig:  # noqa: D401
        """Stub class for firecrawl.v2.types.PaginationConfig."""

    class _FakeScrapeOptions:  # noqa: D401
        """Stub class for firecrawl.v2.types.ScrapeOptions."""

    firecrawl_v2_types.PaginationConfig = _FakePaginationConfig
    firecrawl_v2_types.ScrapeOptions = _FakeScrapeOptions

    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")

    class _PaymentRequiredError(Exception):
        pass

    class _RequestTimeoutError(Exception):
        pass

    firecrawl_v2_utils_error.PaymentRequiredError = _PaymentRequiredError
    firecrawl_v2_utils_error.RequestTimeoutError = _RequestTimeoutError
    firecrawl_v2_utils.error_handler = firecrawl_v2_utils_error

    firecrawl_v2.types = firecrawl_v2_types
    firecrawl_v2.utils = firecrawl_v2_utils
    firecrawl_mod.v2 = firecrawl_v2

    sys.modules["firecrawl"] = firecrawl_mod
    sys.modules["firecrawl.v2"] = firecrawl_v2
    sys.modules["firecrawl.v2.types"] = firecrawl_v2_types
    sys.modules["firecrawl.v2.utils"] = firecrawl_v2_utils
    sys.modules["firecrawl.v2.utils.error_handler"] = firecrawl_v2_utils_error


def _ensure_opentelemetry_stub() -> None:
    if "opentelemetry" in sys.modules:
        return

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
        def __init__(self, endpoint: str | None = None, headers: dict | None = None):
            self.endpoint = endpoint
            self.headers = headers or {}

    sys.modules["opentelemetry"] = otel_mod
    sys.modules["opentelemetry._logs"] = types.SimpleNamespace(
        set_logger_provider=lambda *_a, **_k: None
    )
    sys.modules["opentelemetry.exporter.otlp.proto.common"] = types.SimpleNamespace()
    sys.modules["opentelemetry.exporter.otlp.proto.http._log_exporter"] = types.SimpleNamespace(
        OTLPLogExporter=_FakeOTLPExporter
    )
    sys.modules["opentelemetry.sdk._logs"] = types.SimpleNamespace(
        LoggerProvider=_FakeLoggerProvider, LoggingHandler=_FakeLoggingHandler
    )
    sys.modules["opentelemetry.sdk._logs.export"] = types.SimpleNamespace(
        BatchLogRecordProcessor=_FakeBatchLogRecordProcessor
    )


def _sync_settings_flags() -> None:
    config_mod = sys.modules.get("job_scrape_application.config")
    if not config_mod:
        return
    settings = getattr(config_mod, "settings", None)
    if not settings:
        return
    settings.enable_firecrawl = False
    settings.enable_fetchfox = False


_disable_fetchfox_firecrawl_env()
_ensure_fetchfox_stub()
_ensure_firecrawl_stub()
_ensure_opentelemetry_stub()
_sync_settings_flags()


def pytest_collection_modifyitems(config, items) -> None:  # noqa: ARG001
    skip_marker = pytest.mark.skip(reason="firecrawl/fetchfox workers are disabled")
    skip_tokens = ("firecrawl", "fetchfox")

    for item in items:
        path = Path(str(item.fspath))
        if path.suffix != ".py":
            continue
        lowered_parts = [part.lower() for part in path.parts]
        if any(token in part for part in lowered_parts for token in skip_tokens):
            item.add_marker(skip_marker)
