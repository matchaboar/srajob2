from __future__ import annotations

import asyncio
import os
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List

# Ensure repo root importable for module imports
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

if "firecrawl" not in sys.modules:
    class _FakeFirecrawl:
        def __init__(self, *_args, **_kwargs):
            pass

    firecrawl_pkg = types.ModuleType("firecrawl")
    firecrawl_pkg.Firecrawl = _FakeFirecrawl
    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")

    class _FakePagination:
        def __init__(self, *_args, **_kwargs):
            pass

    class _FakeScrapeOptions:
        def __init__(self, *_args, **_kwargs):
            pass

    class _FakePaymentRequiredError(Exception):
        pass

    class _FakeRequestTimeoutError(Exception):
        pass

    firecrawl_v2_types.PaginationConfig = _FakePagination
    firecrawl_v2_types.ScrapeOptions = _FakeScrapeOptions
    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
    firecrawl_v2_utils_error.PaymentRequiredError = _FakePaymentRequiredError
    firecrawl_v2_utils_error.RequestTimeoutError = _FakeRequestTimeoutError
    firecrawl_v2_utils.error_handler = firecrawl_v2_utils_error
    firecrawl_pkg.v2 = firecrawl_v2
    sys.modules["firecrawl"] = firecrawl_pkg
    sys.modules["firecrawl.v2"] = firecrawl_v2
    sys.modules["firecrawl.v2.types"] = firecrawl_v2_types
    sys.modules["firecrawl.v2.utils"] = firecrawl_v2_utils
    sys.modules["firecrawl.v2.utils.error_handler"] = firecrawl_v2_utils_error

if "yaml" not in sys.modules:
    sys.modules["yaml"] = types.SimpleNamespace(safe_load=lambda *_args, **_kwargs: {})
if "opentelemetry" not in sys.modules:
    otel_mod = types.ModuleType("opentelemetry")
    otel_logs = types.SimpleNamespace(set_logger_provider=lambda *_args, **_kwargs: None)
    otel_mod._logs = otel_logs

    class _FakeLoggerProvider:
        def __init__(self, *_args, **_kwargs):
            pass

        def add_log_record_processor(self, *_args, **_kwargs):
            return None

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

if "fetchfox_sdk" not in sys.modules:
    class _FakeFetchFox:
        def __init__(self, *_args, **_kwargs):
            pass

    sys.modules["fetchfox_sdk"] = types.SimpleNamespace(FetchFox=_FakeFetchFox)
if "temporalio" not in sys.modules:
    def _noop(*_args, **_kwargs):
        return None

    def _defn(fn=None, **_kwargs):
        if fn is None:
            return lambda f: f
        return fn

    class _DummyCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeActivityError(Exception):
        def __init__(self, cause=None):
            super().__init__("activity error")
            self.cause = cause

    class _FakeApplicationError(Exception):
        def __init__(self, message="", non_retryable: bool = False):
            super().__init__(message)
            self.non_retryable = non_retryable

    workflow_mod = types.SimpleNamespace(
        defn=_defn,
        run=_defn,
        execute_activity=_noop,
        start_activity=_noop,
        start_child_workflow=_noop,
        now=lambda: datetime.now(timezone.utc),
        info=lambda: SimpleNamespace(run_id="run-stub", workflow_id="wf-stub", task_queue="default"),
        unsafe=types.SimpleNamespace(imports_passed_through=lambda: _DummyCtx()),
    )
    activity_mod = types.SimpleNamespace(defn=_defn, heartbeat=_noop)
    exceptions_mod = types.SimpleNamespace(ActivityError=_FakeActivityError, ApplicationError=_FakeApplicationError)
    temporal_pkg = types.ModuleType("temporalio")
    temporal_pkg.workflow = workflow_mod
    temporal_pkg.activity = activity_mod
    temporal_pkg.exceptions = exceptions_mod
    sys.modules["temporalio"] = temporal_pkg
    sys.modules["temporalio.workflow"] = workflow_mod
    sys.modules["temporalio.activity"] = activity_mod
    sys.modules["temporalio.exceptions"] = exceptions_mod
if "pydantic" not in sys.modules:
    class _FakeBaseModel:
        def __init__(self, *_args, **_kwargs):
            pass

    class _FakeConfigDict(dict):
        def __init__(self, *_args, **_kwargs):
            super().__init__()

    def _fake_field(*_args, **_kwargs):
        return None

    def _fake_field_validator(*_args, **_kwargs):
        def decorator(fn):
            return fn

        return decorator

    sys.modules["pydantic"] = types.SimpleNamespace(
        BaseModel=_FakeBaseModel,
        Field=_fake_field,
        field_validator=_fake_field_validator,
        ConfigDict=_FakeConfigDict,
    )
if "httpx" not in sys.modules:
    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, *_args, **_kwargs):
            return types.SimpleNamespace(status_code=200, text="")

        async def post(self, *_args, **_kwargs):
            return types.SimpleNamespace(status_code=200, text="")

    sys.modules["httpx"] = types.SimpleNamespace(AsyncClient=_FakeAsyncClient, Response=object)
if "spider" not in sys.modules:
    class _FakeAsyncSpider:
        async def crawl(self, *_args, **_kwargs):
            return []

    sys.modules["spider"] = types.SimpleNamespace(AsyncSpider=_FakeAsyncSpider)

from job_scrape_application.workflows import activities as activities_mod
from job_scrape_application.workflows import greenhouse_workflow as gh
from job_scrape_application.workflows import scrape_workflow as sw


def test_record_scratchpad_enriches_message_and_workflow_id(monkeypatch):
    recorded: List[Dict[str, Any]] = []

    def fake_emit(payload: Dict[str, Any]) -> None:
        recorded.append(payload)

    monkeypatch.setattr(activities_mod.telemetry, "emit_posthog_log", fake_emit)

    async def _run() -> None:
        entry = {
            "event": "site.leased",
            "workflow_id": "wf-record-1",
            "siteUrl": "https://careers.robinhood.com",
            "data": {"siteId": "site-1", "jobsScraped": 4},
        }

        await activities_mod.record_scratchpad(entry)

    asyncio.run(_run())

    assert recorded, "expected a payload to be emitted"
    payload = recorded[0]
    assert payload["workflowId"] == "wf-record-1"
    assert "workflow_id=wf-record-1" in payload["message"]
    assert "jobsScraped=4" in payload["message"]


def test_scrape_workflow_records_scratchpad_events(monkeypatch):
    site = {"_id": "s1", "url": "https://example.com/jobs"}
    leases: List[Any] = [site, None]
    scratchpad: List[Dict[str, Any]] = []

    async def fake_execute_activity(func, args=None, **kwargs):  # type: ignore[override]
        name = getattr(func, "__name__", "")

        if name == "lease_site":
            return leases.pop(0)

        if name == "fake_scrape_site":
            return {
                "sourceUrl": site["url"],
                "items": {
                    "normalized": [{"title": "Engineer"}],
                    "provider": "firecrawl",
                    "queued": True,
                    "jobId": "job-123",
                    "statusUrl": "https://status/job-123",
                },
                "provider": "firecrawl",
                "workflowName": "ScraperFirecrawl",
            }

        if name == "store_scrape":
            return "scr-1"

        if name in {"complete_site", "fail_site"}:
            return None

        if name == "record_scratchpad":
            payload = args[0] if isinstance(args, list) else args
            scratchpad.append(payload)
            return None

        if name == "record_workflow_run":
            return None

        raise AssertionError(f"Unexpected activity {name}")

    async def fake_scrape_site(*_args, **_kwargs):
        return await fake_execute_activity(fake_scrape_site)

    async def _run() -> None:
        monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
        monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_000))
        monkeypatch.setattr(sw.workflow, "info", lambda: SimpleNamespace(run_id="run-123", workflow_id="wf-abc"))
        monkeypatch.setattr(sw.workflow, "logger", types.SimpleNamespace(info=lambda *_a, **_k: None, warning=lambda *_a, **_k: None, error=lambda *_a, **_k: None), raising=False)

        summary = await sw._run_scrape_workflow(fake_scrape_site, "ScraperFirecrawl")

        assert summary.site_count == 1
        events = [entry.get("event") for entry in scratchpad]
        assert "workflow.start" in events
        assert "site.leased" in events
        assert "scrape.result" in events
        assert "workflow.complete" in events

        scrape_entry = next(e for e in scratchpad if e.get("event") == "scrape.result")
        assert scrape_entry["runId"] == "run-123"
        assert scrape_entry["workflowName"] == "ScraperFirecrawl"
        assert scrape_entry["data"]["jobId"] == "job-123"

    asyncio.run(_run())


def test_greenhouse_workflow_records_scratchpad_events(monkeypatch):
    site = {"_id": "s-gh", "url": "https://example.com/gh", "type": "greenhouse"}
    leases: List[Any] = [site, None]
    scratchpad: List[Dict[str, Any]] = []

    async def fake_execute_activity(func, args=None, **kwargs):  # type: ignore[override]
        name = getattr(func, "__name__", "")

        if name == "lease_site":
            return leases.pop(0)

        if name == "fetch_greenhouse_listing":
            return {
                "job_urls": ["https://example.com/gh/1", "https://example.com/gh/2"],
                "startedAt": 1,
                "completedAt": 2,
            }

        if name == "filter_existing_job_urls":
            return []
        if name == "compute_urls_to_scrape":
            return {
                "urlsToScrape": ["https://example.com/gh/1", "https://example.com/gh/2"],
                "existingCount": 0,
                "totalCount": 2,
            }

        if name == "scrape_greenhouse_jobs":
            return {
                "jobsScraped": 2,
                "scrape": {
                    "sourceUrl": site["url"],
                    "items": {"normalized": [{"title": "A"}, {"title": "B"}]},
                },
            }

        if name == "store_scrape":
            return "scr-gh"

        if name in {"complete_site", "fail_site"}:
            return None

        if name == "record_scratchpad":
            payload = args[0] if isinstance(args, list) else args
            scratchpad.append(payload)
            return None

        if name == "record_workflow_run":
            return None

        raise AssertionError(f"Unexpected activity {name}")

    async def _run() -> None:
        monkeypatch.setattr(gh.workflow, "execute_activity", fake_execute_activity)
        monkeypatch.setattr(gh.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_100))
        monkeypatch.setattr(gh.workflow, "info", lambda: SimpleNamespace(run_id="run-gh", workflow_id="wf-gh"))

        wf = gh.GreenhouseScraperWorkflow()
        summary = await wf.run()

        assert summary.site_count == 1
        assert summary.jobs_scraped == 2
        events = [entry.get("event") for entry in scratchpad]
        assert {"workflow.start", "site.leased", "greenhouse.listing", "greenhouse.scrape", "workflow.complete"}.issubset(events)

        listing = next(e for e in scratchpad if e.get("event") == "greenhouse.listing")
        assert listing["data"]["jobUrls"] == 2
        assert listing["runId"] == "run-gh"

    asyncio.run(_run())


def test_firecrawl_key_suffix_added_to_firecrawl_events(monkeypatch):
    monkeypatch.setattr(activities_mod.settings, "firecrawl_api_key", "fc-abc12345")
    payload = {"event": "firecrawl.job.started", "data": {"jobId": "job-1"}}

    updated = activities_mod._with_firecrawl_suffix(dict(payload))

    assert updated["data"]["jobId"] == "job-1"
    assert updated["data"]["firecrawlKeySuffix"] == "2345"


def test_firecrawl_key_suffix_not_added_when_unrelated(monkeypatch):
    monkeypatch.setattr(activities_mod.settings, "firecrawl_api_key", "fc-abc12345")
    payload = {"event": "workflow.start", "data": {"provider": "fetchfox"}}

    updated = activities_mod._with_firecrawl_suffix(dict(payload))

    assert "firecrawlKeySuffix" not in (updated.get("data") or {})
