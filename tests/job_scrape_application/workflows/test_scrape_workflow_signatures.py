from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows import scrape_workflow as sw  # noqa: E402
from job_scrape_application.services import convex_client  # noqa: E402
from job_scrape_application.workflows import worker  # noqa: E402


@pytest.mark.asyncio
async def test_lease_site_sends_filters(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_mutation(name: str, args: dict[str, object] | None = None):
        captured["name"] = name
        captured["args"] = args or {}
        return {"_id": "site-1"}

    monkeypatch.setattr(convex_client, "convex_mutation", fake_mutation)

    result = await acts.lease_site("worker-x", 120, "greenhouse", "firecrawl")

    assert result["_id"] == "site-1"
    assert captured["name"] == "router:leaseSite"
    assert captured["args"] == {
        "workerId": "worker-x",
        "lockSeconds": 120,
        "siteType": "greenhouse",
        "scrapeProvider": "firecrawl",
    }


@pytest.mark.asyncio
async def test_scrape_workflow_uses_args_kw(monkeypatch):
    calls = []
    state = {"leased_once": False}

    async def fake_execute_activity(activity, *args, **kwargs):
        calls.append((activity, args, kwargs))
        if activity is acts.lease_site:
            if state["leased_once"]:
                return None
            state["leased_once"] = True
            return {"_id": "site-123", "url": "https://example.com"}
        if activity is acts.scrape_site:
            return {"items": {"normalized": [{"url": "https://example.com", "title": "Engineer"}]}}
        if activity is acts.store_scrape:
            return "scrape-1"
        if activity is acts.complete_site:
            return None
        if activity is acts.record_workflow_run:
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(0))

    class _Info:
        run_id = "run-1"
        workflow_id = "wf-1"

    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    summary = await sw._run_scrape_workflow(acts.scrape_site, "ScrapeWorkflow")  # noqa: SLF001

    assert summary.site_count == 1
    assert summary.scrape_ids == ["scrape-1"]

    for activity, args, kwargs in calls:
        assert args == ()
        if activity is acts.lease_site:
            assert kwargs["args"] == ["scraper-worker", 300, None, "fetchfox"]
            assert kwargs["schedule_to_close_timeout"] == timedelta(seconds=30)
        if activity is acts.scrape_site:
            assert kwargs["args"] == [{"_id": "site-123", "url": "https://example.com"}]
        if activity is acts.store_scrape:
            assert kwargs["args"][0]["items"]["normalized"][0]["url"] == "https://example.com"
        if activity is acts.complete_site:
            assert kwargs["args"] == ["site-123"]
        if activity is acts.record_workflow_run:
            assert kwargs["args"][0]["workflowName"] == "ScrapeWorkflow"


@pytest.mark.asyncio
async def test_scrape_workflow_handles_no_sites(monkeypatch):
    calls = []

    async def fake_execute_activity(activity, *args, **kwargs):
        calls.append((activity, args, kwargs))
        if activity is acts.lease_site:
            return None
        if activity is acts.record_workflow_run:
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(0))

    class _Info:
        run_id = "run-1"
        workflow_id = "wf-1"

    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())

    summary = await sw._run_scrape_workflow(acts.scrape_site, "ScrapeWorkflow")  # noqa: SLF001

    assert summary.site_count == 0
    assert summary.scrape_ids == []

    lease_calls = [c for c in calls if c[0] is acts.lease_site]
    assert len(lease_calls) == 1

    record_calls = [c for c in calls if c[0] is acts.record_workflow_run]
    assert len(record_calls) == 1
    payload = record_calls[0][2]["args"][0]
    assert payload["sitesProcessed"] == 0
    assert "No sites were leased" in (payload["error"] or "")


def test_worker_registers_spidercloud_job_details_workflow():
    names = {wf.__name__ for wf in worker.WORKFLOW_CLASSES}
    assert "SpidercloudJobDetailsWorkflow" in names


def test_activities_exports_job_detail_batch_helpers():
    assert hasattr(acts, "lease_scrape_url_batch")
    assert hasattr(acts, "process_spidercloud_job_batch")
    assert hasattr(acts, "complete_scrape_urls")


def test_worker_registers_job_detail_activities():
    names = {fn.__name__ for fn in worker.ACTIVITY_FUNCTIONS}
    assert "lease_scrape_url_batch" in names
    assert "process_spidercloud_job_batch" in names


@pytest.mark.asyncio
async def test_store_scrape_passes_metadata(monkeypatch):
    captured: list[dict[str, object]] = []

    async def fake_mutation(name: str, args: dict[str, object] | None = None):
        captured.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-123"
        return None

    monkeypatch.setattr(convex_client, "convex_mutation", fake_mutation)

    completed_at = 1_700_000_000_000
    scrape_payload = {
        "sourceUrl": "https://example.com",
        "pattern": None,
        "startedAt": completed_at - 5000,
        "completedAt": completed_at,
        "provider": "firecrawl",
        "workflowName": "ScrapeWorkflow",
        "request": {
            "method": "POST",
            "url": "https://api.firecrawl.dev/v2/batch/scrape",
            "body": {"urls": ["https://example.com"], "jobId": "job-123"},
            "headers": {"Authorization": "secret-token"},
        },
        "items": {
            "provider": "firecrawl",
            "normalized": [
                {
                    "title": "Engineer",
                    "company": "Example",
                    "description": "Build things",
                    "location": "Remote",
                    "remote": True,
                    "level": "senior",
                    "total_compensation": 200000,
                    "url": "https://example.com/job/1",
                    "posted_at": completed_at,
                }
            ],
        },
    }

    await acts.store_scrape(scrape_payload)

    insert = next(c for c in captured if c["name"] == "router:insertScrapeRecord")
    assert insert["args"]["provider"] == "firecrawl"
    assert insert["args"]["workflowName"] == "ScrapeWorkflow"
    stored_request = insert["args"]["request"]
    assert stored_request["method"] == "POST"
    assert stored_request["body"]["jobId"] == "job-123"
    assert stored_request["headers"]["Authorization"] != "secret-token"
    assert "..." in stored_request["headers"]["Authorization"]
    assert insert["args"]["items"]["request"] == stored_request

    ingest = next(c for c in captured if c["name"] == "router:ingestJobsFromScrape")
    jobs = ingest["args"]["jobs"]
    assert jobs[0]["scrapedWith"] == "firecrawl"
    assert jobs[0]["scrapedAt"] == completed_at
    assert jobs[0]["workflowName"] == "ScrapeWorkflow"
