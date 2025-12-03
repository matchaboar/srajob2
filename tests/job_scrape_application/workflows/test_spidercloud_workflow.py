from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pytest
import types

# Stub firecrawl dependency for tests that don't exercise it
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
firecrawl_v2_utils.error_handler = types.SimpleNamespace(
    PaymentRequiredError=type("PaymentRequiredError", (Exception,), {}),
    RequestTimeoutError=type("RequestTimeoutError", (Exception,), {}),
)
sys.modules.setdefault("firecrawl.v2.utils", firecrawl_v2_utils)
firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
firecrawl_v2_utils_error.PaymentRequiredError = firecrawl_v2_utils.error_handler.PaymentRequiredError
firecrawl_v2_utils_error.RequestTimeoutError = firecrawl_v2_utils.error_handler.RequestTimeoutError
sys.modules.setdefault("firecrawl.v2.utils.error_handler", firecrawl_v2_utils_error)

# Ensure repo root importable
sys.path.insert(0, os.path.abspath("."))

try:
    import temporalio  # noqa: F401
except ImportError:  # pragma: no cover
    pytest.skip("temporalio not installed", allow_module_level=True)

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows import scrape_workflow as sw  # noqa: E402
from job_scrape_application.workflows.activities import factories  # noqa: E402
from job_scrape_application.workflows.create_schedule import (  # noqa: E402
    load_schedule_configs,
)
from job_scrape_application.components.models import (  # noqa: E402
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)
from job_scrape_application.workflows.activities.types import Site  # noqa: E402
from job_scrape_application.workflows.scrapers import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.activities import _extract_job_urls_from_scrape  # type: ignore  # noqa: E402


def test_spidercloud_workflow_has_schedule():
    configs = load_schedule_configs()
    config_map = {cfg.id: cfg for cfg in configs}
    spidercloud = config_map.get("scraper-spidercloud")

    assert spidercloud is not None, "Spidercloud workflow is missing from Temporal schedules"
    assert spidercloud.workflow == "ScraperSpidercloud"
    assert spidercloud.task_queue in (None, "scraper-task-queue")


@pytest.mark.asyncio
async def test_spidercloud_workflow_leases_manual_trigger(monkeypatch):
    now = datetime.fromtimestamp(0)
    lease_payload: Site = {
        "_id": "site-manual",
        "url": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "scrapeProvider": "spidercloud",
        "type": "greenhouse",
        "manualTriggerAt": int(now.timestamp() * 1000),
        "scheduleId": "sched-123",
        "lockExpiresAt": int(now.timestamp() * 1000) + 10_000,
    }

    calls: list[str] = []
    scratchpad_events: list[dict[str, Any]] = []
    log_events: list[tuple[str, str]] = []
    state = {"leased": False}

    async def fake_execute_activity(activity, *args, **kwargs):
        calls.append(activity.__name__ if hasattr(activity, "__name__") else str(activity))
        if activity is acts.lease_site:
            if state["leased"]:
                return None
            state["leased"] = True
            return lease_payload
        if activity is acts.scrape_site:
            return {"items": {"normalized": []}}
        if activity is acts.store_scrape:
            return "scrape-xyz"
        if activity is acts.record_scratchpad:
            payload = None
            if args and args[0]:
                payload = args[0][0] if isinstance(args[0], list) else args[0]
            elif kwargs.get("args"):
                arg_list = kwargs.get("args")
                if isinstance(arg_list, list) and arg_list:
                    payload = arg_list[0]
            payload = payload or {}
            scratchpad_events.append(payload)
            return None
        if activity in (acts.complete_site, acts.record_workflow_run):
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    class _Log:
        def info(self, msg, *args, **kwargs):
            log_events.append(("info", msg % args if args else msg))

        def warning(self, msg, *args, **kwargs):
            log_events.append(("warning", msg % args if args else msg))

        def error(self, msg, *args, **kwargs):
            log_events.append(("error", msg % args if args else msg))

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(sw.workflow, "now", lambda: now)
    monkeypatch.setattr(sw.workflow, "info", lambda: type("Info", (), {"run_id": "r", "workflow_id": "wf"})())
    monkeypatch.setattr(sw, "logger", _Log())

    summary = await sw._run_scrape_workflow(  # noqa: SLF001
        acts.scrape_site,
        "ScraperSpidercloud",
        scrape_provider="spidercloud",
        activity_timeout=timedelta(minutes=25),
    )

    assert summary.site_count == 1
    assert "lease_site" in calls
    assert "scrape_site" in calls
    assert "record_scratchpad" in calls
    assert scratchpad_events
    assert any(isinstance(evt, dict) and evt.get("event") == "workflow.start" for evt in scratchpad_events)
    assert any(isinstance(evt, dict) and evt.get("event") == "scrape.result" for evt in scratchpad_events)
    assert any("event=workflow.start" in msg for _level, msg in log_events)


@pytest.mark.asyncio
async def test_spidercloud_workflow_uses_provider_and_timeout(monkeypatch):
    calls: List[Dict[str, Any]] = []
    state = {"leased_once": False}

    async def fake_execute_activity(activity, *args, **kwargs):
        calls.append({"activity": activity, "kwargs": kwargs})
        if activity is acts.lease_site:
            if state["leased_once"]:
                return None
            state["leased_once"] = True
            return {"_id": "site-123", "url": "https://example.com"}
        if activity is acts.scrape_site:
            return {"items": {"normalized": [{"url": "https://example.com", "title": "Engineer"}]}}
        if activity is acts.store_scrape:
            return "scrape-1"
        if activity in (acts.complete_site, acts.record_workflow_run):
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(0))

    class _Info:
        run_id = "run-1"
        workflow_id = "wf-1"

    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    summary = await sw._run_scrape_workflow(  # noqa: SLF001
        acts.scrape_site,
        "ScraperSpidercloud",
        scrape_provider="spidercloud",
        activity_timeout=timedelta(minutes=25),
    )

    assert summary.site_count == 1
    assert summary.scrape_ids == ["scrape-1"]

    lease_call = next(c for c in calls if c["activity"] is acts.lease_site)
    assert lease_call["kwargs"]["args"] == ["scraper-worker", 300, None, "spidercloud"]

    scrape_call = next(c for c in calls if c["activity"] is acts.scrape_site)
    assert scrape_call["kwargs"]["start_to_close_timeout"] == timedelta(minutes=25)


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_fanout(monkeypatch):
    site: Site = {
        "_id": "01hzconvexsiteid123456789abc",
        "url": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "type": "greenhouse",
        "scrapeProvider": "spidercloud",
    }

    calls: list[str] = []
    scraper = _make_spidercloud_scraper()
    enqueue_calls: list[Dict[str, Any]] = []
    query_calls: list[Dict[str, Any]] = []
    now_ms = 0
    stale_created = now_ms - (acts.SCRAPE_URL_QUEUE_TTL_MS + 1)

    async def fake_listing(site_arg: Site):
        calls.append("listing")
        assert site_arg is site
        return {"job_urls": ["https://example.com/a", "https://example.com/b"]}

    async def fake_select_scraper(site_arg: Site):
        return scraper, None

    async def fake_filter_existing(urls: list[str]):
        return ["https://example.com/a"]

    monkeypatch.setattr(scraper, "fetch_greenhouse_listing", fake_listing)
    monkeypatch.setattr(acts, "select_scraper_for_site", fake_select_scraper)
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)
    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation",
        lambda name, payload: enqueue_calls.append({"name": name, "payload": payload}),
    )
    async def fake_convex_query(name, payload):
        query_calls.append({"name": name, "payload": payload})
        return [
            {"url": "https://example.com/a", "createdAt": stale_created, "status": "pending"},
            {"url": "https://example.com/b", "createdAt": now_ms, "status": "pending"},
        ]

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_query",
        fake_convex_query,
    )

    res = await acts.scrape_site(site)

    assert calls == ["listing"]
    assert any(call["name"] == "router:enqueueScrapeUrls" for call in enqueue_calls)
    assert any(
        call["name"] == "router:completeScrapeUrls"
        and call["payload"].get("status") == "failed"
        and "https://example.com/a" in call["payload"].get("urls", [])
        for call in enqueue_calls
    )
    assert any(call["name"] == "router:listQueuedScrapeUrls" for call in query_calls)
    items = res.get("items", {})
    assert items.get("queued") is True


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_not_ingested(monkeypatch):
    site: Site = {
        "_id": "01hzconvexsiteid123456789abd",
        "url": "https://api.greenhouse.io/v1/boards/example/jobs",
        "type": "greenhouse",
        "scrapeProvider": "spidercloud",
    }

    scraper = _make_spidercloud_scraper()
    calls: list[str] = []

    async def fake_listing(site_arg: Site):
        calls.append("listing")
        assert site_arg is site
        return {"job_urls": []}

    async def fake_scrape_site(site_arg: Site, *, _skip_urls=None):
        calls.append("scrape_site")
        return {
            "items": {"normalized": [{"url": site_arg["url"], "title": "Jobs"}]},
            "provider": "spidercloud",
        }

    monkeypatch.setattr(scraper, "fetch_greenhouse_listing", fake_listing)
    monkeypatch.setattr(scraper, "scrape_site", fake_scrape_site)
    monkeypatch.setattr(acts, "select_scraper_for_site", lambda _site: (scraper, []))

    res = await acts.scrape_site(site)

    assert calls == ["listing"]
    items = res.get("items", {})
    assert items.get("normalized") == []


@pytest.mark.asyncio
async def test_spidercloud_convex_calls_strip_none_fields(monkeypatch):
    site: Site = {
        "_id": "01hzconvexsiteid123456789abe",
        "url": "https://api.greenhouse.io/v1/boards/example/jobs",
        "type": "greenhouse",
        "scrapeProvider": "spidercloud",
        "pattern": None,
    }

    scraper = _make_spidercloud_scraper()
    mutation_calls: list[Dict[str, Any]] = []
    queue_payloads: list[Dict[str, Any]] = []
    now_ms = int(time.time() * 1000)

    async def fake_listing(site_arg: Site):
        return {"job_urls": ["https://example.com/new"]}

    async def fake_scrape_jobs(payload: Dict[str, Any]):
        return {
            "scrape": {"items": {"normalized": [{"url": u} for u in payload.get("urls", [])]}},
            "jobsScraped": len(payload.get("urls", [])),
        }

    async def fake_filter_existing(urls: list[str]):
        return []

    async def fake_convex_query(name, payload):
        if name == "router:findExistingJobUrls":
            return {"existing": []}
        if name == "router:listSeenJobUrlsForSite":
            return {"urls": []}
        if name == "router:listQueuedScrapeUrls":
            queue_payloads.append(payload)
        return [{"url": "https://example.com/new", "createdAt": now_ms, "status": "pending"}]

    async def fake_convex_mutation(name, payload):
        mutation_calls.append({"name": name, "payload": payload})
        return {"queued": payload.get("urls", [])}

    monkeypatch.setattr(scraper, "fetch_greenhouse_listing", fake_listing)
    monkeypatch.setattr(acts, "select_scraper_for_site", lambda _site: (scraper, []))
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_convex_query)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)

    res = await acts.scrape_site(site)

    assert res.get("items", {}).get("queued") is True

    # listQueuedScrapeUrls should not receive explicit None fields (e.g., status or pattern)
    assert queue_payloads
    assert queue_payloads[0] == {"siteId": site["_id"], "provider": "spidercloud", "limit": 500}
    assert all(value is not None for value in queue_payloads[0].values())

    enqueue_payload = next(call["payload"] for call in mutation_calls if call["name"] == "router:enqueueScrapeUrls")
    assert "pattern" not in enqueue_payload
    assert "siteId" in enqueue_payload  # still forwards known identifiers
    assert all(value is not None for value in enqueue_payload.values())


@pytest.mark.asyncio
async def test_spidercloud_skips_invalid_site_ids(monkeypatch):
    site: Site = {
        "_id": "site-1",
        "url": "https://api.greenhouse.io/v1/boards/example/jobs",
        "type": "greenhouse",
        "scrapeProvider": "spidercloud",
    }

    scraper = _make_spidercloud_scraper()
    queue_payloads: list[Dict[str, Any]] = []
    mutation_calls: list[Dict[str, Any]] = []

    async def fake_listing(site_arg: Site):
        return {"job_urls": ["https://example.com/new"]}

    async def fake_filter_existing(urls: list[str]):
        return []

    async def fake_convex_query(name, payload):
        if name == "router:listQueuedScrapeUrls":
            queue_payloads.append(payload)
        return [{"url": "https://example.com/new", "createdAt": 0, "status": "pending"}]

    async def fake_convex_mutation(name, payload):
        mutation_calls.append({"name": name, "payload": payload})
        return {"queued": payload.get("urls", [])}

    monkeypatch.setattr(scraper, "fetch_greenhouse_listing", fake_listing)
    monkeypatch.setattr(acts, "select_scraper_for_site", lambda _site: (scraper, []))
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_convex_query)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)

    res = await acts.scrape_site(site)

    assert res.get("items", {}).get("queued") is True
    assert queue_payloads and "siteId" not in queue_payloads[0]
    enqueue_payload = next(call["payload"] for call in mutation_calls if call["name"] == "router:enqueueScrapeUrls")
    assert "siteId" not in enqueue_payload


def test_strip_none_values_keeps_falsey():
    data = {"a": None, "b": 0, "c": "", "d": False, "e": "ok"}
    cleaned = acts._strip_none_values(data)  # noqa: SLF001
    assert cleaned == {"b": 0, "c": "", "d": False, "e": "ok"}


@pytest.mark.asyncio
async def test_store_scrape_ingests_spidercloud_jobs(monkeypatch):
    """Regression: ensure jobs from spidercloud scrape are sent to Convex ingestion."""

    # Minimal spidercloud scrape payload with two normalized jobs
    scrape_payload = {
        "sourceUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
        "provider": "spidercloud",
        "items": {
            "normalized": [
                {
                    "title": "Software Engineer",
                    "job_title": "Software Engineer",
                    "url": "https://boards.greenhouse.io/example/jobs/1",
                    "company": "Example",
                    "location": "Remote",
                    "remote": True,
                    "level": "mid",
                    "description": "Desc",
                    "posted_at": 0,
                },
                {
                    "title": "Senior Engineer",
                    "job_title": "Senior Engineer",
                    "url": "https://boards.greenhouse.io/example/jobs/2",
                    "company": "Example",
                    "location": "Remote",
                    "remote": True,
                    "level": "senior",
                    "description": "Desc",
                    "posted_at": 0,
                },
            ],
            "provider": "spidercloud",
        },
    }

    ingest_calls: list[dict[str, Any]] = []

    async def fake_convex_mutation(name, payload):
        if name == "router:ingestJobsFromScrape":
            ingest_calls.append(payload)
        return "ok"

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)

    await acts.store_scrape(scrape_payload)

    assert ingest_calls, "Expected ingestJobsFromScrape to be called"
    jobs_payload = ingest_calls[0].get("jobs")
    assert isinstance(jobs_payload, list) and len(jobs_payload) == 2


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_uses_boards_slug(monkeypatch):
    scraper = _make_spidercloud_scraper()
    requested: dict[str, str] = {}

    class FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            requested["url"] = url
            payload = {
                "jobs": [
                    {
                        "absolute_url": "https://example.com/job/123",
                        "title": "Software Engineer",
                        "id": 123,
                        "location": {"name": "Remote"},
                    },
                ]
            }
            return FakeResponse(json.dumps(payload))

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.httpx.AsyncClient",
        FakeClient,
    )

    site: Site = {
        "_id": "01hzconvexsiteid123456789abf",
        "url": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert requested["url"] == "https://boards.greenhouse.io/v1/boards/robinhood/jobs"
    assert listing["job_urls"] == ["https://example.com/job/123"]


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_regex_fallback(monkeypatch):
    scraper = _make_spidercloud_scraper()
    requested: dict[str, str] = {}

    # Use the saved Spidercloud scrape response content (string JSON).
    scrape_fixture = Path("tests/fixtures/spidercloud_robinhood_scrape.json")
    payload = json.loads(scrape_fixture.read_text(encoding="utf-8"))
    content_str = payload[0]["content"]

    class FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            requested["url"] = url
            return FakeResponse(content_str)

    # Force structured parser to yield no URLs so regex is used.
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.load_greenhouse_board",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.extract_greenhouse_job_urls",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.httpx.AsyncClient",
        FakeClient,
    )

    site: Site = {
        "_id": "01hzconvexsiteid123456789abg",
        "url": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert requested["url"] == "https://boards.greenhouse.io/v1/boards/robinhood/jobs"
    assert len(listing["job_urls"]) >= 30
    assert any(u.startswith("https://boards.greenhouse.io/robinhood/jobs/") for u in listing["job_urls"])


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_enqueues_listing_urls(monkeypatch):
    scraper = _make_spidercloud_scraper()
    fixture_path = Path("tests/fixtures/robinhood_greenhouse_board.json")
    board = load_greenhouse_board(fixture_path.read_text(encoding="utf-8"))
    job_urls = extract_greenhouse_job_urls(board)
    assert job_urls, "fixture should contain job urls"

    enqueue_calls: list[Dict[str, Any]] = []
    complete_calls: list[Dict[str, Any]] = []
    query_calls: list[Dict[str, Any]] = []

    async def fake_fetch_greenhouse_listing(site: Site):
        return {"job_urls": job_urls}

    async def fake_filter_existing(urls: list[str]):
        return []

    async def fake_convex_mutation(name: str, payload: Dict[str, Any]):
        if name == "router:enqueueScrapeUrls":
            enqueue_calls.append(payload)
        if name == "router:completeScrapeUrls":
            complete_calls.append(payload)
        return None

    async def fake_convex_query(name: str, payload: Dict[str, Any]):
        query_calls.append({"name": name, "payload": payload})
        # Return queued rows so the scraper processes them (not stale)
        now_ms = 0
        return [{"url": u, "createdAt": now_ms, "status": "pending"} for u in job_urls]

    monkeypatch.setattr(scraper, "fetch_greenhouse_listing", fake_fetch_greenhouse_listing)
    monkeypatch.setattr(acts, "select_scraper_for_site", lambda site: (scraper, None))
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_convex_query)

    site: Site = {
        "_id": "01hzconvexsiteid123456789abh",
        "url": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "type": "greenhouse",
        "scrapeProvider": "spidercloud",
    }

    res = await acts.scrape_site(site)

    assert enqueue_calls, "should enqueue discovered job urls"
    assert len(enqueue_calls[0]["urls"]) == len(job_urls)
    assert not complete_calls  # job details now processed by dedicated workflow
    assert query_calls
    items = res.get("items") if isinstance(res, dict) else {}
    assert items.get("queued") is True
    assert items.get("job_urls")


def test_extract_job_urls_from_spidercloud_scrape_raw():
    scrape_fixture = Path("tests/fixtures/spidercloud_robinhood_scrape.json")
    raw_payload = json.loads(scrape_fixture.read_text(encoding="utf-8"))[0]
    scrape_payload = {
        "sourceUrl": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "normalized": [{"url": "https://api.greenhouse.io/v1/boards/robinhood/jobs"}],
            "raw": raw_payload.get("content"),
        },
    }

    urls = _extract_job_urls_from_scrape(scrape_payload)

    assert len(urls) >= 20
    assert any("boards.greenhouse.io/robinhood/jobs" in u for u in urls)


@pytest.mark.asyncio
async def test_store_scrape_enqueues_urls_from_spidercloud_raw(monkeypatch):
    scrape_fixture = Path("tests/fixtures/spidercloud_robinhood_scrape.json")
    raw_payload = json.loads(scrape_fixture.read_text(encoding="utf-8"))[0]
    scrape_payload = {
        "sourceUrl": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "normalized": [{"url": "https://api.greenhouse.io/v1/boards/robinhood/jobs"}],
            "raw": raw_payload.get("content"),
        },
    }

    calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    await acts.store_scrape(scrape_payload)

    enqueue_calls = [c for c in calls if c["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "store_scrape should enqueue job URLs from raw payload"
    assert len(enqueue_calls[0]["args"]["urls"]) >= 20


@pytest.mark.asyncio
async def test_select_scraper_defaults_greenhouse_to_spidercloud(monkeypatch):
    monkeypatch.setattr(acts.settings, "spider_api_key", "spider-key")
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", None)
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", None)

    scraper, skip_urls = await acts.select_scraper_for_site({"_id": "s1", "url": "https://example.com", "type": "greenhouse"})

    assert isinstance(scraper, SpiderCloudScraper)
    assert skip_urls is None


@pytest.mark.asyncio
async def test_select_scraper_falls_back_to_firecrawl(monkeypatch):
    monkeypatch.setattr(acts.settings, "spider_api_key", None)
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-key")
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", None)

    async def fake_seen(url: str, pattern: str | None):
        return ["https://example.com/seen"]

    monkeypatch.setattr(factories, "fetch_seen_urls_for_site", fake_seen)

    scraper, skip_urls = await acts.select_scraper_for_site({"_id": "s2", "url": "https://example.com", "type": "greenhouse"})

    from job_scrape_application.workflows.scrapers import FirecrawlScraper  # noqa: E402

    assert isinstance(scraper, FirecrawlScraper)
    assert skip_urls == ["https://example.com/seen"]


def _make_spidercloud_scraper() -> SpiderCloudScraper:
    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=types.SimpleNamespace(spider_api_key="key"),
        fetch_seen_urls_for_site=lambda *_args, **_kwargs: [],
    )
    return SpiderCloudScraper(deps)


def test_spidercloud_filters_when_title_missing_keyword():
    scraper = _make_spidercloud_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url="https://example.com/jobs/product-manager",
        markdown="",
        events=[{"title": "Product Manager"}],
        started_at=0,
    )

    assert normalized is None


def test_spidercloud_allows_when_title_unknown():
    scraper = _make_spidercloud_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url="https://example.com/jobs/listing-123",
        markdown="",
        events=[],
        started_at=0,
    )

    assert normalized is not None
    assert normalized["title"] == "Listing 123"


@pytest.mark.asyncio
async def test_spidercloud_scrape_site_skips_seen_urls(monkeypatch):
    seen_url = "https://example.com/jobs/skip-me"
    captured: dict[str, Any] = {}

    async def fake_seen(url: str, pattern: str | None):
        return [seen_url]

    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=types.SimpleNamespace(spider_api_key="key"),
        fetch_seen_urls_for_site=fake_seen,
    )
    scraper = SpiderCloudScraper(deps)

    async def fake_batch(urls: List[str], *, source_url: str, pattern: str | None):
        captured["urls"] = urls
        return {
            "sourceUrl": source_url,
            "pattern": pattern,
            "provider": scraper.provider,
            "items": {"normalized": [], "provider": scraper.provider, "seedUrls": urls},
        }

    monkeypatch.setattr(scraper, "_scrape_urls_batch", fake_batch)

    site = {"_id": "s-skip", "url": seen_url, "pattern": None}
    result = await scraper.scrape_site(site)

    assert captured.get("urls") == []
    assert result["items"]["seedUrls"] == []


@pytest.mark.asyncio
async def test_spidercloud_records_costs_and_ingests(monkeypatch):
    total_cost_usd = 0.015
    expected_cost_mc = int(total_cost_usd * 100000)
    scraper = _make_spidercloud_scraper()

    class FakeAsyncSpider:
        def __init__(self, api_key: str):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def crawl_url(self, url: str, params: Dict[str, Any], stream: bool, content_type: str):
            assert stream is True
            yield {
                "commonmark": "# Software Engineer\nDetails",
                "costs": {"total_cost": total_cost_usd, "compute_cost": total_cost_usd / 3},
            }

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        FakeAsyncSpider,
    )

    site = {"_id": "s-cost", "url": "https://example.com/jobs/eng"}
    scrape_payload = await scraper.scrape_site(site)

    assert scrape_payload["costMilliCents"] == expected_cost_mc
    assert scrape_payload["items"]["costMilliCents"] == expected_cost_mc

    calls: Dict[str, Any] = {}

    async def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            calls["record"] = args
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            calls["jobs"] = args["jobs"]
            return None
        raise AssertionError(f"unexpected mutation {name}")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    await acts.store_scrape(scrape_payload)

    assert calls["record"]["costMilliCents"] == expected_cost_mc
    assert calls["jobs"]
    assert {job["scrapedCostMilliCents"] for job in calls["jobs"]} == {expected_cost_mc}


@pytest.mark.asyncio
async def test_spidercloud_job_details_splits_cost_per_url(monkeypatch):
    total_cost_mc = 900
    urls = [
        "https://example.com/jobs/1",
        "https://example.com/jobs/2",
        "https://example.com/jobs/3",
    ]
    per_url_cost = int(total_cost_mc / len(urls))

    class FakeScraper:
        async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
            assert payload["urls"] == urls
            return {
                "scrape": {
                    "provider": "spidercloud",
                    "sourceUrl": "https://example.com/boards/listing",
                    "items": {
                        "provider": "spidercloud",
                        "normalized": [{"url": url} for url in urls],
                        "raw": [{"url": url} for url in urls],
                        "costMilliCents": total_cost_mc,
                    },
                    "costMilliCents": total_cost_mc,
                }
            }

    monkeypatch.setattr(acts, "_make_spidercloud_scraper", lambda: FakeScraper())

    batch = {"urls": [{"url": url, "sourceUrl": "https://example.com/boards/listing"} for url in urls]}

    result = await acts.process_spidercloud_job_batch(batch)
    scrapes = result["scrapes"]

    assert len(scrapes) == len(urls)
    for scrape, url in zip(scrapes, urls, strict=True):
        assert scrape["subUrls"] == [url]
        assert scrape["costMilliCents"] == per_url_cost
        assert scrape["items"]["costMilliCents"] == per_url_cost
        assert scrape["workflowName"] == "SpidercloudJobDetails"
        assert scrape["provider"] == "spidercloud"


def test_robinhood_greenhouse_fixture_parses_urls():
    fixture_path = Path("tests/fixtures/robinhood_greenhouse_board.json")
    payload_text = fixture_path.read_text(encoding="utf-8")

    board = load_greenhouse_board(payload_text)
    urls = extract_greenhouse_job_urls(board)

    assert len(board.jobs) == 102
    assert any(job.absolute_url == "https://boards.greenhouse.io/robinhood/jobs/7379020?t=gh_src=&gh_jid=7379020" for job in board.jobs)
    # Title filter trims to roles matching required keywords; ensure we still retain many URLs.
    assert len(urls) >= 30


def test_spidercloud_robinhood_scrape_fixture_matches_request():
    request_path = Path("tests/fixtures/spidercloud_robinhood_request.json")
    response_path = Path("tests/fixtures/spidercloud_robinhood_scrape.json")

    request = json.loads(request_path.read_text(encoding="utf-8"))
    response = json.loads(response_path.read_text(encoding="utf-8"))

    assert request["url"] == "https://api.greenhouse.io/v1/boards/robinhood/jobs"
    assert request["params"]["return_format"] == ["commonmark"]
    assert request["contentType"] == "application/jsonl"

    assert isinstance(response, list) and len(response) == 1
    first = response[0]
    assert first.get("url") == request["url"]
    assert first.get("status") == 200
    costs = first.get("costs") or {}
    assert "total_cost" in costs

    content = first.get("content")
    assert isinstance(content, str) and len(content) > 1000

    board = load_greenhouse_board(content)
    assert len(board.jobs) == 102
    assert any(job.absolute_url for job in board.jobs)


def test_extract_job_urls_from_scrape_parses_html_listing_with_filters():
    html = """
    <div>
      <a href="https://example.com/jobs/1">Software Engineer - New York, NY</a>
      <a href="https://example.com/jobs/2">Product Manager - Toronto, ON</a>
      <a href="https://example.com/jobs/3">QA Engineer (Remote)</a>
    </div>
    """

    scrape = {"items": {"raw": [{"content": html}], "provider": "spidercloud"}}

    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://example.com/jobs/1" in urls
    assert "https://example.com/jobs/3" in urls  # Remote allowed when location omitted/remote.
    assert "https://example.com/jobs/2" not in urls  # filtered: title keyword + non-US location.


@pytest.mark.asyncio
async def test_process_pending_job_details_batch_runs_in_workflow(monkeypatch):
    # Ensure the heuristic workflow activity can be invoked without errors.
    from job_scrape_application.workflows.activities import process_pending_job_details_batch

    async def fake_query(name, args=None):
        if name == "router:listPendingJobDetails":
            return []
        if name == "router:listJobDetailConfigs":
            return []
        return []

    async def fake_mutation(name, args=None):
        return {}

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    res = await process_pending_job_details_batch()
    assert res["processed"] == 0
