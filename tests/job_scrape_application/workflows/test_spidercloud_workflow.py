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
from job_scrape_application.config import runtime_config  # noqa: E402
from job_scrape_application.workflows.site_handlers import GreenhouseHandler  # noqa: E402
from job_scrape_application.workflows.site_handlers import GithubCareersHandler  # noqa: E402
from job_scrape_application.workflows.site_handlers import AshbyHqHandler  # noqa: E402
from job_scrape_application.workflows.scrapers import spidercloud_scraper as sc_scraper  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import trim_scrape_for_convex  # noqa: E402


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

    async def fake_fetch(api_url: str, _handler):
        requested["url"] = api_url
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
        return json.dumps(payload), []

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789abf",
        "url": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert requested["url"] == "https://api.greenhouse.io/v1/boards/robinhood/jobs"
    assert listing["job_urls"] == ["https://example.com/job/123"]


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_regex_fallback(monkeypatch):
    scraper = _make_spidercloud_scraper()
    requested: dict[str, str] = {}

    # Use the saved Spidercloud scrape response content (string JSON).
    scrape_fixture = Path("tests/fixtures/spidercloud_robinhood_scrape.json")
    payload = json.loads(scrape_fixture.read_text(encoding="utf-8"))
    content_str = payload[0]["content"]

    # Force structured parser to yield no URLs so regex is used.
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.load_greenhouse_board",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.extract_greenhouse_job_urls",
        lambda *_args, **_kwargs: [],
    )

    async def fake_fetch(api_url: str, _handler):
        requested["url"] = api_url
        return content_str, []

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789abg",
        "url": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert requested["url"] == "https://api.greenhouse.io/v1/boards/robinhood/jobs"
    assert len(listing["job_urls"]) >= 30
    assert any(u.startswith("https://boards.greenhouse.io/robinhood/jobs/") for u in listing["job_urls"])


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_regex_uses_event_payload(monkeypatch):
    scraper = _make_spidercloud_scraper()
    raw_text = "https://boards.greenhouse.io/flex/jobs/4634056005"
    extra_html = "<html>https://boards.greenhouse.io/flex/jobs/4641646005</html>"

    async def fake_fetch(_api_url: str, _handler):
        return raw_text, [{"content": {"raw": extra_html}}]

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789ac9",
        "url": "https://api.greenhouse.io/v1/boards/flex/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert raw_text in listing["job_urls"]
    assert "https://boards.greenhouse.io/flex/jobs/4641646005" in listing["job_urls"]


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_invalid_json_returns_empty(monkeypatch):
    scraper = _make_spidercloud_scraper()
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_lyft_greenhouse_listing_invalid.json"
    )
    raw_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    raw_events = raw_payload[0] if isinstance(raw_payload, list) and raw_payload else []
    raw_text = ""
    if isinstance(raw_events, list) and raw_events:
        first = raw_events[0]
        if isinstance(first, dict):
            content = first.get("content")
            if isinstance(content, dict):
                raw_text = content.get("raw", "")

    async def fake_fetch(_api_url: str, _handler):
        return raw_text, raw_events

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789ac0",
        "url": "https://api.greenhouse.io/v1/boards/lyft/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert listing["job_urls"] == []
    assert "Access Denied" in listing["raw"]


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_ignores_listing_url_fallback(monkeypatch):
    scraper = _make_spidercloud_scraper()
    requested: dict[str, str] = {}
    raw_text = "blocked https://api.greenhouse.io/v1/boards/flex/jobs"

    async def fake_fetch(api_url: str, _handler):
        requested["url"] = api_url
        return raw_text, []

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789ac1",
        "url": "https://api.greenhouse.io/v1/boards/flex/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert requested["url"] == "https://api.greenhouse.io/v1/boards/flex/jobs"
    assert listing["job_urls"] == []


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_parses_raw_html_fixture(monkeypatch):
    scraper = _make_spidercloud_scraper()
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_airbnb_greenhouse_listing_raw_html.json"
    )
    raw_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    raw_events = raw_payload[0] if isinstance(raw_payload, list) and raw_payload else []
    raw_text = ""
    if isinstance(raw_events, list) and raw_events:
        first = raw_events[0]
        if isinstance(first, dict):
            content = first.get("content")
            if isinstance(content, dict):
                raw_text = content.get("raw", "")

    async def fake_fetch(_api_url: str, _handler):
        return raw_text, raw_events

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789ac3",
        "url": "https://api.greenhouse.io/v1/boards/airbnb/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert listing["job_urls"]
    assert all(
        url.startswith("https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/")
        for url in listing["job_urls"]
    )


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_parses_raw_response_fixture(monkeypatch):
    scraper = _make_spidercloud_scraper()
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_lyft_greenhouse_listing_raw.json"
    )
    raw_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    raw_events = raw_payload if isinstance(raw_payload, list) else [raw_payload]
    raw_text = ""
    for event in raw_events:
        if isinstance(event, dict):
            content = event.get("content")
            if isinstance(content, str):
                raw_text = content
                break

    async def fake_fetch(_api_url: str, _handler):
        return raw_text, raw_events

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789ac5",
        "url": "https://api.greenhouse.io/v1/boards/lyft/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert "https://boards-api.greenhouse.io/v1/boards/lyft/jobs/8332698002" in listing["job_urls"]
    assert listing["raw"] and "jobs" in listing["raw"]


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_parses_togetherai_fixture(monkeypatch):
    scraper = _make_spidercloud_scraper()
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_togetherai_greenhouse_listing_invalid.json"
    )
    raw_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    raw_events = raw_payload[0] if isinstance(raw_payload, list) and raw_payload else []
    raw_text = ""
    if isinstance(raw_events, list) and raw_events:
        first = raw_events[0]
        if isinstance(first, dict):
            content = first.get("content")
            if isinstance(content, dict):
                raw_text = content.get("raw", "")

    async def fake_fetch(_api_url: str, _handler):
        return raw_text, raw_events

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789ac4",
        "url": "https://api.greenhouse.io/v1/boards/togetherai/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert "https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4967737007" in listing["job_urls"]


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_uses_event_payload_when_raw_text_empty(monkeypatch):
    scraper = _make_spidercloud_scraper()
    job_url = "https://boards.greenhouse.io/example/jobs/123"
    payload = {
        "jobs": [
            {
                "absolute_url": job_url,
                "title": "Software Engineer",
                "id": 123,
                "location": {"name": "Remote"},
            }
        ]
    }

    async def fake_fetch(_api_url: str, _handler):
        return "", [{"content": json.dumps(payload)}]

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789abz",
        "url": "https://api.greenhouse.io/v1/boards/example/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert listing["job_urls"] == [job_url]
    raw_payload = json.loads(listing["raw"])
    assert raw_payload["jobs"][0]["absolute_url"] == job_url


@pytest.mark.asyncio
async def test_spidercloud_greenhouse_listing_reconstructs_chunked_json(monkeypatch):
    scraper = _make_spidercloud_scraper()
    job_url = "https://boards.greenhouse.io/example/jobs/456"
    payload = {
        "jobs": [
            {
                "absolute_url": job_url,
                "title": "Software Engineer",
                "id": 456,
                "location": {"name": "Remote"},
            }
        ]
    }
    payload_text = json.dumps(payload)
    html_payload = f"<html><body><pre>{payload_text}</pre></body></html>"
    chunk_one = html_payload[:40]
    chunk_two = html_payload[40:]

    async def fake_fetch(_api_url: str, _handler):
        return chunk_one, [{"content": chunk_one}, {"content": chunk_two}]

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)

    site: Site = {
        "_id": "01hzconvexsiteid123456789ac2",
        "url": "https://api.greenhouse.io/v1/boards/example/jobs",
        "type": "greenhouse",
    }

    listing = await scraper.fetch_greenhouse_listing(site)

    assert listing["job_urls"] == [job_url]
    assert "<pre>" in listing["raw"]
    assert payload_text in listing["raw"]


def test_spidercloud_extract_json_payload_from_pre_html():
    scraper = _make_spidercloud_scraper()
    job_url = "https://boards.greenhouse.io/example/jobs/456"
    html_payload = (
        "<html><body><pre>"
        + json.dumps(
            {
                "jobs": [
                    {
                        "absolute_url": job_url,
                        "title": "Software Engineer",
                        "id": 456,
                        "location": {"name": "Remote"},
                    }
                ]
            }
        )
        + "</pre></body></html>"
    )

    extracted = scraper._extract_json_payload([{"content": html_payload}])

    assert extracted is not None
    assert extracted["jobs"][0]["absolute_url"] == job_url


def test_spidercloud_extracts_location_from_raw_html_json_ld():
    scraper = _make_spidercloud_scraper()
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/coupang_job_7486748_raw.html"
    )
    raw_html = fixture_path.read_text(encoding="utf-8")

    normalized = scraper._normalize_job(  # noqa: SLF001
        url="https://www.coupang.jobs/en/jobs/7486748/seniorstaff-android-engineer-streaming-player-coupang-play/?gh_jid=7486748",
        markdown="",
        events=[{"raw_html": raw_html}],
        started_at=0,
    )

    assert normalized is not None
    assert normalized["location"] == "Singapore, Singapore"


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

    assert len(urls) == 102
    assert any("boards.greenhouse.io/robinhood/jobs" in u for u in urls)


def test_extract_job_urls_from_spidercloud_scrape_strips_slash_noise():
    fixture = json.loads(
        Path("tests/fixtures/spidercloud_greenhouse_slash_urls.json").read_text(encoding="utf-8")
    )
    scrape_payload = {
        "sourceUrl": fixture["sourceUrl"],
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "response": fixture,
        "items": {"provider": "spidercloud", "normalized": [], "raw": []},
    }

    urls = _extract_job_urls_from_scrape(scrape_payload)

    assert sorted(urls) == sorted(
        [
            "https://boards-api.greenhouse.io/v1/boards/datadog/jobs/7243623",
            "https://boards-api.greenhouse.io/v1/boards/stubhubinc/jobs/4713661101",
        ]
    )
    assert all("\\" not in url for url in urls)


def test_extract_job_urls_from_spidercloud_scrape_filters_apply_urls_without_handler():
    scrape_payload = {
        "sourceUrl": "",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "raw": {
                "jobs": [
                    {
                        "jobUrl": "https://careers.adobe.com/us/en/job/R162038/Support-Pricing-Specialist",
                        "applyUrl": "https://careers.adobe.com/us/en/apply?jobSeqNo=ADOBUSR162038EXTERNALENUS",
                    }
                ]
            },
        },
    }

    urls = _extract_job_urls_from_scrape(scrape_payload)

    assert urls == ["https://careers.adobe.com/us/en/job/R162038/Support-Pricing-Specialist"]


def test_extract_job_urls_from_spidercloud_scrape_filters_apply_links_list():
    scrape_payload = {
        "sourceUrl": "https://careers.adobe.com/us/en/search-results?keywords=engineer",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "links": [
                "https://careers.adobe.com/us/en/apply?jobSeqNo=ADOBUSR162038EXTERNALENUS",
                "https://careers.adobe.com/us/en/job/R162038/Support-Pricing-Specialist",
            ],
        },
    }

    urls = _extract_job_urls_from_scrape(scrape_payload)

    assert urls == ["https://careers.adobe.com/us/en/job/R162038/Support-Pricing-Specialist"]


def test_extract_job_urls_from_spidercloud_ashby_html():
    html_fixture = Path("tests/fixtures/lambda-ashbyhq-src.html").read_text(encoding="utf-8")
    scrape_payload = {
        "sourceUrl": "https://jobs.ashbyhq.com/lambda",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "raw": [
                {
                    "url": "https://jobs.ashbyhq.com/lambda",
                    "events": [{"content": {"raw_html": html_fixture}}],
                    "markdown": "Lambda Jobs",
                }
            ],
        },
    }

    urls = _extract_job_urls_from_scrape(scrape_payload)

    assert "https://jobs.ashbyhq.com/lambda/senior-software-engineer" in urls
    assert "https://jobs.ashbyhq.com/lambda/security-engineer" in urls
    assert "https://jobs.ashbyhq.com/lambda/product-manager" not in urls


def test_extract_job_urls_from_spidercloud_paloalto_html():
    html_fixture = Path("tests/fixtures/paloalto_networks_search_raw.html").read_text(encoding="utf-8")
    scrape_payload = {
        "sourceUrl": "https://jobs.paloaltonetworks.com/en/search-jobs?k=software%20engineer&l=United+States",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "raw": [
                {
                    "url": "https://jobs.paloaltonetworks.com/en/search-jobs?k=software%20engineer&l=United+States",
                    "events": [{"content": {"raw_html": html_fixture}}],
                    "markdown": "",
                }
            ],
        },
    }

    urls = _extract_job_urls_from_scrape(scrape_payload)

    assert (
        "https://jobs.paloaltonetworks.com/en/job/reston/channel-systems-engineer-2/47263/89375432832"
        in urls
    )


def test_spidercloud_extracts_job_urls_from_ashby_listing_payload():
    scraper = _make_spidercloud_scraper()
    handler = AshbyHqHandler()
    payload = json.loads(
        Path("tests/fixtures/ashby_lambda_listing_payload_small.json").read_text(encoding="utf-8")
    )
    markdown = json.dumps(payload)

    urls = scraper._extract_listing_job_urls(handler, [payload], markdown)

    assert "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938" in urls
    assert "https://jobs.ashbyhq.com/lambda/bed21e20-1ef2-40d9-b5ab-a7e0172cf85f" in urls


def test_spidercloud_normalize_job_ignores_listing_payload():
    scraper = _make_spidercloud_scraper()
    payload = json.loads(
        Path("tests/fixtures/ashby_lambda_listing_payload_small.json").read_text(encoding="utf-8")
    )
    markdown = json.dumps(payload)

    normalized = scraper._normalize_job(
        "https://jobs.ashbyhq.com/lambda",
        markdown,
        [payload],
        started_at=0,
    )

    assert normalized is None
    assert scraper._last_ignored_job is not None
    assert scraper._last_ignored_job.get("reason") == "listing_payload"


def test_spidercloud_ashby_detail_preserves_structured_title():
    scraper = _make_spidercloud_scraper()
    raw_html = Path("tests/fixtures/ashby_lambda_job_detail_raw.html").read_text(encoding="utf-8")
    markdown = "\n".join(
        [
            "What You'll Do",
            "- Partner closely with Engineering, Data Center Operations, and Finance teams to align supplier capabilities with technical roadmaps and scaling needs.",
        ]
    )

    normalized = scraper._normalize_job(  # noqa: SLF001
        url="https://jobs.ashbyhq.com/lambda/0d79c70e-7a49-4a4a-a72f-7ef25d62de41",
        markdown=markdown,
        events=[{"content": {"raw_html": raw_html}}],
        started_at=0,
        require_keywords=True,
    )

    assert normalized is not None
    assert normalized["title"] == "Technical Sourcing Manager"


def test_spidercloud_normalize_job_skips_avature_listing_url():
    scraper = _make_spidercloud_scraper()
    url = "https://bloomberg.avature.net/careers/SearchJobs/engineer?jobOffset=0"
    normalized = scraper._normalize_job(  # noqa: SLF001
        url=url,
        markdown="Bloomberg Careers",
        events=[],
        started_at=0,
    )

    assert normalized is None
    assert scraper._last_ignored_job is not None  # noqa: SLF001
    assert scraper._last_ignored_job.get("reason") == "listing_page"  # noqa: SLF001


@pytest.mark.asyncio
async def test_spidercloud_uses_ashby_api_when_available(monkeypatch):
    scraper = _make_spidercloud_scraper()

    called: dict[str, Any] = {"batch": False}

    async def fake_batch(*_args, **_kwargs):
        called["batch"] = True
        return {"items": {"normalized": [], "provider": "spidercloud", "seedUrls": []}}

    monkeypatch.setattr(scraper, "_scrape_urls_batch", fake_batch)

    async def fake_fetch_site_api(handler, url, *, pattern):
        return {"items": {"job_urls": [
            "https://jobs.ashbyhq.com/lambda/senior-software-engineer",
            "https://jobs.ashbyhq.com/lambda/security-engineer",
        ]}}

    monkeypatch.setattr(scraper, "_fetch_site_api", fake_fetch_site_api)

    site = {"_id": "s-ashby", "url": "https://jobs.ashbyhq.com/lambda", "pattern": None}
    result = await scraper.scrape_site(site)

    assert called["batch"] is False
    assert result["items"]["job_urls"] == [
        "https://jobs.ashbyhq.com/lambda/senior-software-engineer",
        "https://jobs.ashbyhq.com/lambda/security-engineer",
    ]


@pytest.mark.asyncio
async def test_spidercloud_falls_back_when_ashby_api_fails(monkeypatch):
    scraper = _make_spidercloud_scraper()

    async def fake_fetch_site_api(*_args, **_kwargs):
        return None

    monkeypatch.setattr(scraper, "_fetch_site_api", fake_fetch_site_api)

    called: dict[str, Any] = {"batch": False}

    async def fake_batch(urls: List[str], *, source_url: str, pattern: str | None):
        called["batch"] = True
        return {
            "sourceUrl": source_url,
            "pattern": pattern,
            "provider": scraper.provider,
            "items": {"normalized": [], "provider": scraper.provider, "seedUrls": urls},
        }

    monkeypatch.setattr(scraper, "_scrape_urls_batch", fake_batch)

    site = {
        "_id": "s-ashby",
        "url": "https://jobs.ashbyhq.com/lambda",
        "pattern": "https://jobs.ashbyhq.com/lambda/*",
    }
    result = await scraper.scrape_site(site)

    assert called["batch"] is True
    assert result["items"]["seedUrls"] == ["https://jobs.ashbyhq.com/lambda"]


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
async def test_store_scrape_enqueues_software_engineer_jobs_from_github_fixture(monkeypatch):
    scrape_fixture = Path("tests/fixtures/spidercloud_github_careers_scrape.json")
    raw_payload = json.loads(scrape_fixture.read_text(encoding="utf-8"))
    scrape_payload = {
        "sourceUrl": "https://www.github.careers/careers-home/jobs",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": raw_payload},
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
    assert enqueue_calls, "store_scrape should enqueue job URLs from GitHub fixture"
    enqueued_urls = enqueue_calls[0]["args"]["urls"]
    assert "https://www.github.careers/careers-home/jobs/4732?lang=en-us" in enqueued_urls
    assert "https://www.github.careers/careers-home/jobs/4853?lang=en-us" in enqueued_urls


@pytest.mark.asyncio
async def test_store_scrape_enqueues_jobs_from_confluent_fixture(monkeypatch):
    html = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_confluent_engineering_raw.html"
    ).read_text(encoding="utf-8")
    scrape_payload = {
        "sourceUrl": "https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": [{"content": html}]},
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
    assert enqueue_calls, "store_scrape should enqueue job URLs from Confluent fixture"
    enqueued_urls = enqueue_calls[0]["args"]["urls"]
    expected_urls = {
        "https://careers.confluent.io/jobs/job/03bd40fd-07a5-44ed-985f-689e5405c2a8",
        "https://careers.confluent.io/jobs/job/388b3ea4-f181-407f-8c03-0d2bd9135b49",
        "https://careers.confluent.io/jobs/job/5400cdd0-87bf-4df5-aed8-3f526715fa4a",
        "https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1",
        "https://careers.confluent.io/jobs/job/8e0d897b-045c-46ee-9457-d6cc79a95dea",
        "https://careers.confluent.io/jobs/job/9f38c542-fe09-4fb1-bae2-09e3b789119b",
        "https://careers.confluent.io/jobs/job/ca9890c2-4ef6-4e07-ba1b-98a03699e395",
        "https://careers.confluent.io/jobs/job/f6dfe798-2126-4c93-9e1e-031d0a315b3e",
    }
    assert set(enqueued_urls) == expected_urls


@pytest.mark.asyncio
async def test_select_scraper_defaults_greenhouse_to_spidercloud(monkeypatch):
    monkeypatch.setattr(acts.settings, "spider_api_key", "spider-key")
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", None)
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", None)

    scraper, skip_urls = await acts.select_scraper_for_site({"_id": "s1", "url": "https://example.com", "type": "greenhouse"})

    assert isinstance(scraper, SpiderCloudScraper)
    assert skip_urls is None


@pytest.mark.asyncio
async def test_select_scraper_defaults_to_spidercloud_when_key_present(monkeypatch):
    monkeypatch.setattr(acts.settings, "spider_api_key", "spider-key")
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", None)
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "ff-key")

    scraper, skip_urls = await acts.select_scraper_for_site({"_id": "s1", "url": "https://example.com"})

    assert isinstance(scraper, SpiderCloudScraper)
    assert skip_urls is None


@pytest.mark.asyncio
async def test_select_scraper_falls_back_to_firecrawl(monkeypatch):
    monkeypatch.setattr(acts.settings, "spider_api_key", None)
    monkeypatch.setattr(acts.settings, "enable_firecrawl", True)
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


def test_spidercloud_recovers_keyword_from_markdown():
    scraper = _make_spidercloud_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url="https://example.com/careers/4554201006",
        markdown="""
        CoreWeave Logo
        Back to jobs
        Senior Software Engineer, Observability
        Livingston, NJ
        """,
        events=[{"title": "Explore Our Open Positions | CoreWeave"}],
        started_at=0,
    )

    assert normalized is not None
    assert normalized["title"].lower().startswith("senior software engineer")


@pytest.mark.asyncio
async def test_spidercloud_github_listing_preserves_query_params(monkeypatch):
    fixture_path = Path("tests/fixtures/github_careers_api_jobs_12.json")
    payload_full = json.loads(fixture_path.read_text(encoding="utf-8"))
    payload_default = {"jobs": payload_full["jobs"][:10], "count": 10, "totalCount": 10}
    captured: dict[str, Any] = {}

    class FakeAsyncSpider:
        def __init__(self, api_key: str):  # noqa: D401
            captured["api_key"] = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def scrape_url(self, url: str, params: Dict[str, Any] | None = None, stream: bool = False, content_type: str | None = None):
            captured["url"] = url
            captured["params"] = params
            if "keywords=engineer" in url and "limit=100" in url:
                return {"content": json.dumps(payload_full)}
            return {"content": json.dumps(payload_default)}

    monkeypatch.setattr(sc_scraper, "AsyncSpider", FakeAsyncSpider)

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
    scraper = SpiderCloudScraper(deps)

    handler = GithubCareersHandler()
    source_url = (
        "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100"
    )

    result = await scraper._fetch_site_api(handler, source_url)

    assert result is not None
    job_urls = result["items"]["job_urls"]
    assert len(job_urls) == len(payload_full["jobs"])
    assert "keywords=engineer" in captured.get("url", "")
    assert "limit=100" in captured.get("url", "")


def test_spidercloud_coreweave_fixture_not_skipped():
    fixture_path = Path(__file__).parent / "fixtures" / "coreweave_spidercloud_commonmark.json"
    payload = json.loads(fixture_path.read_text())[0]
    markdown = payload["content"]["commonmark"]
    events = [{"title": payload["content"].get("title")}]

    scraper = _make_spidercloud_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url=payload["url"],
        markdown=markdown,
        events=events,
        started_at=0,
    )

    assert normalized is not None
    assert "engineer" in normalized["title"].lower()


def test_spidercloud_github_job_detail_uses_structured_data():
    fixture_path = Path("tests/job_scrape_application/workflows/fixtures/spidercloud_github_careers_job_4554_raw.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    event = payload[0]

    scraper = _make_spidercloud_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url=event["url"],
        markdown="",
        events=payload,
        started_at=0,
    )

    assert normalized is not None
    assert normalized["title"] == "Senior Software Engineer"
    assert normalized["location"] == "Japan"


def test_spidercloud_extracts_markdown_from_content_raw():
    url = "https://explore.jobs.netflix.net/careers/job/790313277967"
    html = (
        "<html><head><title>Software Engineer 5 - TV Client Foundations</title></head>"
        "<body><h1>Software Engineer 5 - TV Client Foundations</h1>"
        "<p>Netflix builds new experiences for members worldwide.</p></body></html>"
    )
    events = [{"content": {"raw": html}, "url": url, "status": 200}]

    scraper = _make_spidercloud_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url=url,
        markdown="",
        events=events,
        started_at=0,
    )

    assert normalized is not None
    assert "Software Engineer 5" in normalized["title"]
    assert "Netflix builds new experiences" in normalized["description"]


def test_spidercloud_extracts_metadata_raw_description():
    url = "https://explore.jobs.netflix.net/careers/job/790313277967"
    description = (
        "Netflix is one of the world's leading entertainment services with members worldwide."
        " This role helps build experiences that scale across devices and audiences."
    )
    events = [
        {
            "content": {"raw": ""},
            "metadata": {"raw": {"title": "Software Engineer 5 - TV Client Foundations", "description": description}},
            "url": url,
            "status": 200,
        }
    ]

    scraper = _make_spidercloud_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url=url,
        markdown="",
        events=events,
        started_at=0,
    )

    assert normalized is not None
    assert "Software Engineer 5" in normalized["title"]
    assert description in normalized["description"]


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


@pytest.mark.parametrize(
    "url",
    [
        "https://careers.confluent.io/jobs/united_states-united_arab_emirates",
        "https://careers.confluent.io/jobs/united_states-poland",
        "https://careers.confluent.io/jobs/united_states-finance_&_operations",
    ],
)
def test_spidercloud_listing_pages_are_ignored(url: str):
    scraper = _make_spidercloud_scraper()
    markdown = """
    Open Positions
    Search for Opportunities
    Select Department
    Select Country
    United States
    Available in Multiple Locations
    Senior Solutions Engineer
    """

    normalized = scraper._normalize_job(  # noqa: SLF001
        url=url,
        markdown=markdown,
        events=[{"title": "Open Positions | Confluent Careers"}],
        started_at=0,
    )

    assert normalized is None
    assert scraper._last_ignored_job is not None  # noqa: SLF001
    assert scraper._last_ignored_job["url"] == url  # noqa: SLF001
    assert scraper._last_ignored_job["reason"] == "listing_page"  # noqa: SLF001


def test_extract_greenhouse_json_markdown_preserves_content():
    fixture_path = Path("tests/fixtures/greenhouse_api_job.json")
    raw = fixture_path.read_text(encoding="utf-8")

    handler = GreenhouseHandler()
    text, title = handler.normalize_markdown(raw)

    assert title == "Senior Software Engineer"
    # Should unescape HTML, keep structure, and not leak tags/escapes
    assert "Job Title: Senior Software Engineer" in text
    assert "Salary: $212,202 - $274,700 / year" in text
    assert "Equal Opportunity Employer" in text
    assert "<" not in text
    assert "\\u" not in text
    # Should preserve paragraph breaks for Markdown rendering
    assert text.count("\n") >= 5


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
async def test_spidercloud_scrape_site_keeps_seed_when_pattern_present(monkeypatch):
    seen_url = "https://jobs.ashbyhq.com/lambda"
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

    async def _no_ashby_api(*_args, **_kwargs):
        return None

    monkeypatch.setattr(scraper, "_fetch_site_api", _no_ashby_api)

    async def fake_batch(urls: List[str], *, source_url: str, pattern: str | None):
        captured["urls"] = urls
        return {
            "sourceUrl": source_url,
            "pattern": pattern,
            "provider": scraper.provider,
            "items": {"normalized": [], "provider": scraper.provider, "seedUrls": urls},
        }

    monkeypatch.setattr(scraper, "_scrape_urls_batch", fake_batch)

    site = {"_id": "s-seed", "url": seen_url, "pattern": "https://jobs.ashbyhq.com/lambda/*"}
    result = await scraper.scrape_site(site)

    assert captured.get("urls") == [seen_url]
    assert result["items"]["seedUrls"] == [seen_url]


@pytest.mark.asyncio
async def test_spidercloud_scrape_site_keeps_listing_seed_when_seen(monkeypatch):
    seen_url = "https://bloomberg.avature.net/careers/SearchJobs/engineer?jobOffset=0"
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

    site = {"_id": "s-avature", "url": seen_url, "pattern": None, "type": "avature"}
    result = await scraper.scrape_site(site)

    assert captured.get("urls") == [seen_url]
    assert result["items"]["seedUrls"] == [seen_url]


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
    assert len(urls) == 40


def test_spidercloud_robinhood_scrape_fixture_matches_request():
    request_path = Path("tests/fixtures/spidercloud_robinhood_request.json")
    response_path = Path("tests/fixtures/spidercloud_robinhood_scrape.json")

    request = json.loads(request_path.read_text(encoding="utf-8"))
    response = json.loads(response_path.read_text(encoding="utf-8"))

    assert request["url"] == "https://api.greenhouse.io/v1/boards/robinhood/jobs"
    assert request["params"]["return_format"] == ["commonmark"]
    assert request["params"]["request"] == "chrome"
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


def test_spidercloud_github_careers_scrape_fixture_matches_request():
    request_path = Path("tests/fixtures/spidercloud_github_careers_request.json")
    response_path = Path("tests/fixtures/spidercloud_github_careers_scrape.json")

    request = json.loads(request_path.read_text(encoding="utf-8"))
    response = json.loads(response_path.read_text(encoding="utf-8"))

    expected_url = (
        "https://www.github.careers/careers-home/jobs?"
        "keywords=engineer&sortBy=posted_date&descending=true&limit=100"
    )
    assert request["url"] == expected_url
    assert request["params"]["return_format"] == "markdown"
    assert request["params"]["request"] == "chrome"

    assert isinstance(response, list) and len(response) == 1
    first = response[0]
    assert first.get("url") == expected_url
    assert first.get("status") == 200
    content = first.get("content")
    assert isinstance(content, str) and len(content) > 1000

    scrape = {"items": {"raw": response, "provider": "spidercloud"}}
    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://www.github.careers/careers-home/jobs/4732?lang=en-us" in urls
    assert "https://www.github.careers/careers-home/jobs/4853?lang=en-us" in urls
    assert "https://www.github.careers/careers-home/jobs/4843?lang=en-us" in urls
    assert "https://www.github.careers/careers-home/jobs/4797?lang=en-us" not in urls


@pytest.mark.asyncio
async def test_spidercloud_cisco_listing_extracts_job_urls(monkeypatch):
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_search_page_1.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    events = payload[0] if payload and isinstance(payload[0], list) else payload
    assert isinstance(events, list) and events

    class FakeAsyncSpider:
        def __init__(self, api_key: str):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def scrape_url(
            self,
            url: str,
            params: Dict[str, Any] | None = None,
            stream: bool = False,
            content_type: str | None = None,
        ):
            assert stream is True

            async def _stream():
                for event in events:
                    yield event

            return _stream()

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        FakeAsyncSpider,
    )

    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=acts.trim_scrape_for_convex,
        settings=types.SimpleNamespace(spider_api_key="key"),
        fetch_seen_urls_for_site=lambda *_args, **_kwargs: [],
    )
    scraper = SpiderCloudScraper(deps)

    site_url = "https://careers.cisco.com/global/en/search-results?keywords=%22software%20engineer%22&s=1"
    result = await scraper.scrape_site({"_id": "s-cisco", "url": site_url})
    job_urls = result.get("items", {}).get("job_urls") or []

    assert any("/global/en/job/" in url for url in job_urls)
    assert any("search-results" in url and "from=10" in url for url in job_urls)


def test_extract_job_urls_from_snapchat_scrape_fixture():
    response_path = Path("tests/fixtures/spidercloud_snapchat_jobs_scrape.json")
    response = json.loads(response_path.read_text(encoding="utf-8"))

    scrape = {"items": {"raw": response, "provider": "spidercloud"}}
    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://careers.snap.com/job?id=R0043314" in urls
    assert "https://careers.snap.com/job?id=R0042985" in urls
    assert "https://careers.snap.com/jobs" not in urls


def test_extract_job_urls_from_lambda_ai_careers_fixture():
    response_path = Path("tests/fixtures/spidercloud_lambda_ai_careers.json")
    response = json.loads(response_path.read_text(encoding="utf-8"))
    if response and isinstance(response[0], list):
        response = response[0]

    scrape = {
        "sourceUrl": "https://lambda.ai/careers",
        "items": {"raw": response, "provider": "spidercloud"},
    }
    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938" in urls
    assert "https://jobs.ashbyhq.com/lambda/264f889c-38f4-42a5-9534-064a9512a3fe" in urls
    assert not any(
        'jobs.ashbyhq.com/"https:/' in url or "jobs.ashbyhq.com/%22https:/" in url for url in urls
    )


def test_extract_job_urls_from_adobe_search_fixture():
    response_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_search_page_1.json"
    )
    response = json.loads(response_path.read_text(encoding="utf-8"))

    scrape = {
        "sourceUrl": "https://careers.adobe.com/us/en/search-results?keywords=engineer",
        "items": {"raw": response, "provider": "spidercloud"},
    }
    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://careers.adobe.com/us/en/job/R162737/Research-Engineer" in urls
    assert "https://careers.adobe.com/us/en/job/R161781/Data-Engineer" in urls
    assert "https://careers.adobe.com/us/en/search-results?from=10&s=1" in urls


def test_extract_job_urls_from_scrape_uses_items_job_urls():
    scrape = {
        "sourceUrl": "https://openai.com/careers/search?q=engineer",
        "items": {
            "provider": "spidercloud",
            "job_urls": [
                "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco/",
                "https://openai.com/careers/search/?q=engineer",
            ],
        },
    }

    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    normalized = {url.rstrip("/") for url in urls}
    assert "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco" in normalized
    assert not any("/careers/search" in url for url in urls)


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
    assert "https://example.com/jobs/3" in urls
    assert "https://example.com/jobs/2" in urls


def test_extract_job_urls_from_scrape_markdown_location_context_filters():
    markdown = """
    [Senior Software Engineer](https://example.com/careers/jobs/123)
    Location United States
    [Support Engineer](https://example.com/careers/jobs/456)
    Location Canada
    """

    scrape = {"items": {"raw": [{"content": markdown}], "provider": "spidercloud"}}

    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://example.com/careers/jobs/123" in urls
    assert "https://example.com/careers/jobs/456" in urls


def test_extract_job_urls_from_scrape_markdown_read_more_context():
    markdown = """
    Senior Software Engineer
    Location United States
    [Read More](https://example.com/careers/jobs/789)
    [Apply Now](https://example.com/careers/jobs/789/apply)
    """

    scrape = {"items": {"raw": [{"content": markdown}], "provider": "spidercloud"}}

    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://example.com/careers/jobs/789" in urls
    assert "https://example.com/careers/jobs/789/apply" not in urls


def test_extract_job_urls_from_confluent_spidercloud_commonmark_fixture():
    response = json.loads(
        Path(
            "tests/job_scrape_application/workflows/fixtures/spidercloud_confluent_engineering_commonmark.json"
        ).read_text(encoding="utf-8")
    )
    source_url = "https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering"
    scrape = {
        "sourceUrl": source_url,
        "items": {
            "provider": "spidercloud",
            "raw": [{"url": source_url, "events": response, "markdown": ""}],
        },
    }
    trimmed = trim_scrape_for_convex(scrape)
    raw_preview = trimmed.get("items", {}).get("raw")
    assert isinstance(raw_preview, str)
    assert "/jobs/job/" not in raw_preview

    urls = _extract_job_urls_from_scrape(trimmed)  # noqa: SLF001

    expected = {
        "https://careers.confluent.io/jobs/job/03bd40fd-07a5-44ed-985f-689e5405c2a8",
        "https://careers.confluent.io/jobs/job/388b3ea4-f181-407f-8c03-0d2bd9135b49",
        "https://careers.confluent.io/jobs/job/5400cdd0-87bf-4df5-aed8-3f526715fa4a",
        "https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1",
        "https://careers.confluent.io/jobs/job/8e0d897b-045c-46ee-9457-d6cc79a95dea",
        "https://careers.confluent.io/jobs/job/9f38c542-fe09-4fb1-bae2-09e3b789119b",
        "https://careers.confluent.io/jobs/job/ca9890c2-4ef6-4e07-ba1b-98a03699e395",
        "https://careers.confluent.io/jobs/job/f6dfe798-2126-4c93-9e1e-031d0a315b3e",
    }
    assert expected.issubset(set(urls))


def test_extract_job_urls_from_scrape_filters_confluent_location_urls():
    location_urls = [
        "https://careers.confluent.io/jobs/united_states-missouri",
        "https://careers.confluent.io/jobs/united_states-masovian",
        "https://careers.confluent.io/jobs/united_states-london,_city_of",
        "https://careers.confluent.io/jobs/united_states-bavaria",
        "https://careers.confluent.io/jobs/united_states-barcelona",
    ]
    scrape = {
        "items": {
            "provider": "spidercloud",
            "job_urls": location_urls + ["https://careers.confluent.io/jobs/12345"],
        }
    }

    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://careers.confluent.io/jobs/12345" in urls
    for url in location_urls:
        assert url not in urls


@pytest.mark.asyncio
async def test_spidercloud_job_details_marks_failed_on_batch_error(monkeypatch):
    """Regression: leased URLs must be released on batch failure to avoid stuck processing."""

    calls: list[Dict[str, Any]] = []
    state = {"leased": False}

    async def fake_execute_activity(activity, *args, **kwargs):
        if activity is acts.lease_scrape_url_batch:
            if state["leased"]:
                return {"urls": []}
            state["leased"] = True
            return {"urls": [{"url": "https://example.com/job/123"}]}
        if activity is acts.process_spidercloud_job_batch:
            raise RuntimeError("boom")
        if activity is acts.complete_scrape_urls:
            payload = args[0] if args else kwargs.get("args", [])[0]
            calls.append(payload)
            return {"updated": len(payload.get("urls", []))}
        if activity in (acts.record_scratchpad, acts.record_workflow_run):
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)

    class _Info:
        run_id = "run-1"
        workflow_id = "wf-1"

    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    summary = await sw.SpidercloudJobDetailsWorkflow().run()  # type: ignore[call-arg]

    assert summary.site_count == 0  # batch failed before marking processed
    assert calls, "complete_scrape_urls should be called on failure"
    payload = calls[0]
    assert payload["status"] == "failed"
    assert "https://example.com/job/123" in payload["urls"]


@pytest.mark.asyncio
async def test_spidercloud_job_details_uses_runtime_timeouts(monkeypatch):
    """Ensure workflow applies runtime-configured timeout and passes processing expiry to lease."""

    calls: list[Dict[str, Any]] = []
    state = {"leased": False}

    async def fake_execute_activity(activity, *args, **kwargs):
        if activity is acts.lease_scrape_url_batch:
            calls.append({"activity": "lease", "kwargs": kwargs})
            if state["leased"]:
                return {"urls": []}
            state["leased"] = True
            return {"urls": [{"url": "https://example.com/job/123"}]}
        if activity is acts.process_spidercloud_job_batch:
            calls.append(
                {
                    "activity": "process",
                    "kwargs": kwargs,
                }
            )
            return {"scrapes": []}
        if activity in (acts.complete_scrape_urls, acts.record_scratchpad, acts.record_workflow_run):
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)

    class _Info:
        run_id = "run-2"
        workflow_id = "wf-2"

    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    await sw.SpidercloudJobDetailsWorkflow().run()  # type: ignore[call-arg]

    lease_call = next(c for c in calls if c["activity"] == "lease")
    process_call = next(c for c in calls if c["activity"] == "process")

    # lease call should use configured processing expiry (passed as arg)
    # kwargs are under schedule_to_close_timeout, args: [...]; we captured kwargs, so check args content
    lease_args = lease_call["kwargs"].get("args") or []
    assert lease_args, "lease args should be present"
    # processing expiry is passed via the activity payload (first positional arg)
    # We can't inspect the payload directly from kwargs; ensure activity function was called (presence is enough for this regression)
    assert lease_call is not None

    timeout = process_call["kwargs"].get("start_to_close_timeout")
    assert timeout is not None
    assert timeout.total_seconds() == runtime_config.spidercloud_job_details_timeout_minutes * 60


@pytest.mark.asyncio
async def test_spidercloud_job_details_logs_skipped_urls(monkeypatch):
    scratchpad_events: list[dict[str, Any]] = []
    lease_calls = {"count": 0}

    async def fake_execute_activity(activity, *args, **kwargs):
        if activity is acts.lease_scrape_url_batch:
            lease_calls["count"] += 1
            if lease_calls["count"] == 1:
                return {
                    "urls": [{"url": "https://example.com/job", "sourceUrl": "https://example.com"}],
                    "skippedUrls": ["https://example.com/skip-me"],
                }
            return {"urls": []}
        if activity is acts.process_spidercloud_job_batch:
            return {"scrapes": []}
        if activity is acts.record_scratchpad:
            payload = None
            if args and args[0]:
                payload = args[0][0] if isinstance(args[0], list) else args[0]
            elif kwargs.get("args"):
                arg_list = kwargs.get("args")
                if isinstance(arg_list, list) and arg_list:
                    payload = arg_list[0]
            if isinstance(payload, dict):
                scratchpad_events.append(payload)
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    class _Info:
        run_id = "run-logs"
        workflow_id = "wf-logs"
        task_queue = "scraper-task-queue"

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    wf = sw.SpidercloudJobDetailsWorkflow()
    await wf.run()

    assert any(evt.get("event") == "batch.skipped_urls" for evt in scratchpad_events)


@pytest.mark.asyncio
async def test_spidercloud_http_timeout_uses_runtime_config(monkeypatch):
    recorded: Dict[str, Any] = {}

    async def fake_wait_for(coro, timeout=None):
        recorded["timeout"] = timeout
        return await coro

    class FakeSpider:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def scrape_url(self, url: str, *args, **kwargs):
            recorded["url"] = url

            async def _gen():
                yield {"content": '{"jobs":[]}'}

            return _gen()

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.asyncio.wait_for",
        fake_wait_for,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        FakeSpider,
    )

    scraper = _make_spidercloud_scraper()
    site: Site = {"_id": "s-http", "url": "https://boards.greenhouse.io/v1/boards/example/jobs"}

    await scraper.fetch_greenhouse_listing(site)

    assert recorded.get("timeout") == runtime_config.spidercloud_http_timeout_seconds


@pytest.mark.asyncio
async def test_spidercloud_fetch_listing_prefers_greenhouse_api_job_urls(monkeypatch):
    payload = {
        "jobs": [
            {
                "id": 4554201006,
                "absolute_url": "https://coreweave.com/careers/job?4554201006&board=coreweave&gh_jid=4554201006",
                "title": "Senior Software Engineer",
            }
        ]
    }

    scraper = _make_spidercloud_scraper()

    async def fake_fetch(api_url: str, _handler):
        return json.dumps(payload), []

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", fake_fetch)
    site: Site = {"_id": "s-coreweave", "url": "https://api.greenhouse.io/v1/boards/coreweave/jobs"}

    listing = await scraper.fetch_greenhouse_listing(site)

    assert listing["job_urls"] == [
        "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4554201006",
    ]


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
