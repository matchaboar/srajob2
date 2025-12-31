from __future__ import annotations

import asyncio
import os
import re
import sys
import types
from pathlib import Path
from typing import Any, Dict

import pytest

ROUTER_ARG_NAME_PATTERN = r"\s*(\w+):"

try:
    import firecrawl  # noqa: F401
    import firecrawl.v2.types  # noqa: F401
    import firecrawl.v2.utils.error_handler  # noqa: F401
except Exception:
    firecrawl_mod = types.ModuleType("firecrawl")
    firecrawl_mod.Firecrawl = type("Firecrawl", (), {})  # dummy class
    sys.modules.setdefault("firecrawl", firecrawl_mod)
    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
    firecrawl_v2_types.PaginationConfig = type("PaginationConfig", (), {})
    firecrawl_v2_types.ScrapeOptions = type("ScrapeOptions", (), {})
    sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
    sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)
    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
    firecrawl_v2_utils_error.PaymentRequiredError = type("PaymentRequiredError", (Exception,), {})
    firecrawl_v2_utils_error.RequestTimeoutError = type("RequestTimeoutError", (Exception,), {})
    sys.modules.setdefault("firecrawl.v2.utils", firecrawl_v2_utils)
    sys.modules.setdefault("firecrawl.v2.utils.error_handler", firecrawl_v2_utils_error)
    firecrawl_v2_utils.error_handler = firecrawl_v2_utils_error
fetchfox_mod = types.ModuleType("fetchfox_sdk")
fetchfox_mod.FetchFox = type("FetchFox", (), {})
sys.modules.setdefault("fetchfox_sdk", fetchfox_mod)

try:
    import temporalio  # noqa: F401
except ImportError:  # pragma: no cover
    temporalio = types.ModuleType("temporalio")
    sys.modules.setdefault("temporalio", temporalio)

    class _Activity:
        def defn(self, fn=None, **kwargs):
            if fn is None:
                def wrapper(func):
                    return func

                return wrapper
            return fn

    temporalio.activity = _Activity()
    sys.modules.setdefault("temporalio.activity", temporalio)

    temporalio_exceptions = types.ModuleType("temporalio.exceptions")
    temporalio_exceptions.ApplicationError = type("ApplicationError", (Exception,), {})
    sys.modules.setdefault("temporalio.exceptions", temporalio_exceptions)

sys.path.insert(0, os.path.abspath("."))

from temporalio.exceptions import ApplicationError  # noqa: E402
from job_scrape_application.workflows import activities as acts  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ROUTER_PATH = PROJECT_ROOT / "job_board_application/convex/router.ts"


def _router_insert_scrape_arg_names() -> set[str]:
    content = ROUTER_PATH.read_text()
    collecting = False
    depth = 0
    names: set[str] = set()
    for line in content.splitlines():
        if not collecting:
            if "export const insertScrapeRecord" in line:
                collecting = True
            continue
        if depth == 0:
            if "args:" not in line:
                continue
            depth += line.count("{") - line.count("}")
            continue
        depth += line.count("{") - line.count("}")
        match = re.match(ROUTER_ARG_NAME_PATTERN, line)
        if match:
            names.add(match.group(1))
        if depth <= 0:
            break
    return names


@pytest.mark.asyncio
async def test_store_scrape_omits_null_cost(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": []},
    }

    calls: Dict[str, Any] = {}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls["name"] = name
        calls["args"] = args
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert "costMilliCents" not in calls["args"]


@pytest.mark.asyncio
async def test_store_scrape_omits_null_pattern(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "pattern": None,
        "items": {"normalized": []},
    }

    calls: Dict[str, Any] = {}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls["name"] = name
        calls["args"] = args
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert "pattern" not in calls["args"]


@pytest.mark.asyncio
async def test_store_scrape_marks_spidercloud_captcha_failed(monkeypatch):
    payload = {
        "sourceUrl": "https://api.greenhouse.io/v1/boards/axon/jobs",
        "workflowName": "SpidercloudJobDetails",
        "items": {
            "normalized": [],
            "failed": [{"url": "https://boards-api.greenhouse.io/v1/boards/axon/jobs/1", "reason": "captcha_failed"}],
        },
    }

    async def fake_mutation(name: str, args: Dict[str, Any]):
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts.telemetry, "emit_posthog_log", lambda *_args, **_kwargs: None)

    with pytest.raises(ApplicationError) as excinfo:
        await acts.store_scrape(payload)

    assert excinfo.value.type == "captcha_failed"


@pytest.mark.asyncio
async def test_store_scrape_retries_on_failure(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": []},
        "costMilliCents": 1500,
    }

    calls: list[str] = []
    first_insert_failed: Dict[str, bool] = {"value": False}
    emitted: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append(name)
        if name == "router:insertScrapeRecord" and not first_insert_failed["value"]:
            first_insert_failed["value"] = True
            raise RuntimeError("first failure")
        return "scrape-id"

    def fake_emit_exception(
        exc: BaseException,
        *,
        distinct_id: str | None = None,  # noqa: ARG001
        properties: Dict[str, Any] | None = None,
    ) -> None:
        emitted.append(
            {
                "exc": exc,
                "properties": properties or {},
            }
        )

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts.telemetry, "emit_posthog_exception", fake_emit_exception)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert first_insert_failed["value"] is True
    assert calls.count("router:insertScrapeRecord") == 2
    assert emitted
    assert isinstance(emitted[0]["exc"], RuntimeError)
    assert emitted[0]["properties"].get("event") == "scrape.persist_failed"


@pytest.mark.asyncio
async def test_store_scrape_ingest_jobs_failure_is_nonfatal(monkeypatch):
    now = 1_700_000_000_000
    payload = {
        "sourceUrl": "https://example.com",
        "completedAt": now,
        "items": {"normalized": [{"url": "https://example.com/job"}]},
    }

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:ingestJobsFromScrape":
            raise RuntimeError("ingest failed")
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"


@pytest.mark.asyncio
async def test_store_scrape_raises_after_double_failure(monkeypatch, caplog):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": [{"url": "https://example.com/job"}]},
    }

    attempts: list[str] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        attempts.append(name)
        raise RuntimeError("convex down")

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    with caplog.at_level("ERROR", logger="temporal.worker.activities"):
        with pytest.raises(ApplicationError) as excinfo:
            await acts.store_scrape(payload)

    assert excinfo.value.type == "store_scrape_failed"
    assert attempts == ["router:insertScrapeRecord", "router:insertScrapeRecord"]
    assert any("Failed to persist scrape after fallback" in msg for msg in caplog.messages)


@pytest.mark.asyncio
async def test_store_scrape_payload_matches_router_contract(monkeypatch):
    allowed_args = _router_insert_scrape_arg_names()
    assert allowed_args, "insertScrapeRecord args were not parsed"
    assert "siteId" in allowed_args

    payload = {
        "sourceUrl": "https://example.com",
        "items": {"normalized": []},
        "siteId": "kd7a81q9r5xsvfy4aqc96k2qts7wjaes",
    }

    insert_calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            insert_calls.append(args)
            assert set(args.keys()) <= allowed_args
        return "scrape-id"

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert insert_calls, "insertScrapeRecord was not invoked"
    assert insert_calls[0]["siteId"] == payload["siteId"]


@pytest.mark.asyncio
async def test_store_scrape_applies_heuristics_before_ingest(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {
            "normalized": [
                {
                    "title": "Heuristic Engineer",
                    "company": "ExampleCo",
                    "description": "Location: Austin, TX\n$150k",
                    "url": "https://example.com/jobs/123",
                    "compensation_unknown": True,
                    "total_compensation": 0,
                }
            ]
        },
    }

    ingest_calls: list[dict[str, Any]] = []
    recorded: list[dict[str, Any]] = []

    async def fake_query(name: str, args: Dict[str, Any] | None = None):
        if name == "router:listJobDetailConfigs":
            return []
        raise AssertionError(f"unexpected query {name}")

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:recordJobDetailHeuristic":
            recorded.append(args)
            return {"created": True}
        if name == "router:ingestJobsFromScrape":
            ingest_calls.append(args)
            return {"inserted": len(args.get("jobs") or [])}
        return None

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"
    assert ingest_calls, "jobs were not ingested"
    job = ingest_calls[0]["jobs"][0]
    assert job["totalCompensation"] == 150000
    assert job.get("compensationUnknown") is False
    assert "parsed" in (job.get("compensationReason") or "")
    assert job.get("heuristicVersion") == acts.HEURISTIC_VERSION
    assert job.get("heuristicAttempts") == 1
    assert any(rec.get("field") == "compensation" for rec in recorded)


@pytest.mark.asyncio
async def test_store_scrape_heuristic_cancellation_logs_and_continues(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "workflowName": "SpidercloudJobDetails",
        "workflowId": "wf-123",
        "runId": "run-456",
        "items": {
            "normalized": [
                {
                    "title": "Heuristic Engineer",
                    "company": "ExampleCo",
                    "description": "Location: Austin, TX\n$150k",
                    "url": "https://example.com/jobs/123",
                }
            ]
        },
    }

    emitted: list[Dict[str, Any]] = []

    async def fake_query(name: str, args: Dict[str, Any] | None = None):
        if name == "router:listJobDetailConfigs":
            raise asyncio.CancelledError()
        return []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:recordJobDetailHeuristic":
            raise asyncio.CancelledError()
        if name == "router:ingestJobsFromScrape":
            return {"inserted": len(args.get("jobs") or [])}
        return None

    def fake_emit_posthog_log(payload: Dict[str, Any]) -> None:
        emitted.append(payload)

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)
    monkeypatch.setattr(acts.telemetry, "emit_posthog_log", fake_emit_posthog_log)

    res = await acts.store_scrape(payload)

    assert res == "scrape-id"

    events = {entry.get("event") for entry in emitted}
    assert "heuristic.list_configs_cancelled" in events
    assert "heuristic.record_cancelled" in events

    for event in ("heuristic.list_configs_cancelled", "heuristic.record_cancelled"):
        entry = next(item for item in emitted if item.get("event") == event)
        assert entry.get("workflowId") == payload["workflowId"]
        assert entry.get("workflowName") == payload["workflowName"]
        assert entry.get("runId") == payload["runId"]
        assert entry.get("siteUrl") == payload["sourceUrl"]
