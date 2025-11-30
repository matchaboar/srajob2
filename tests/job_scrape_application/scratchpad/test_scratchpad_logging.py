from __future__ import annotations

import os
import sys
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

# Ensure repo root importable for module imports
sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as activities_mod
from job_scrape_application.workflows import greenhouse_workflow as gh
from job_scrape_application.workflows import scrape_workflow as sw


@pytest.mark.asyncio
async def test_scrape_workflow_records_scratchpad_events(monkeypatch):
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

    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(1_700_000_000))
    monkeypatch.setattr(sw.workflow, "info", lambda: SimpleNamespace(run_id="run-123", workflow_id="wf-abc"))

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


@pytest.mark.asyncio
async def test_greenhouse_workflow_records_scratchpad_events(monkeypatch):
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
