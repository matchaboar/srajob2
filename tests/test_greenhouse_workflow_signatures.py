from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import greenhouse_workflow as gh  # noqa: E402


@pytest.mark.asyncio
async def test_greenhouse_workflow_uses_args_kw(monkeypatch):
    calls = []
    state = {"leased_once": False}

    async def fake_execute_activity(activity, *args, **kwargs):
        calls.append((activity, args, kwargs))
        if activity is gh.lease_site:
            if state["leased_once"]:
                return None
            state["leased_once"] = True
            return {"_id": "site1", "url": "https://example.com"}
        if activity is gh.fetch_greenhouse_listing:
            return {"job_urls": ["https://example.com/job/1"]}
        if activity is gh.filter_existing_job_urls:
            return []
        if activity is gh.scrape_greenhouse_jobs:
            return {
                "scrape": {"items": {"normalized": [{"url": "https://example.com/job/1"}]}},
                "jobsScraped": 1,
            }
        if activity is gh.store_scrape:
            return "scrape123"
        if activity is gh.complete_site:
            return None
        if activity is gh.record_workflow_run:
            return None
        raise RuntimeError(f"Unexpected activity {activity}")

    monkeypatch.setattr(gh.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(gh.workflow, "now", lambda: datetime.fromtimestamp(0))

    class _Info:
        run_id = "run-1"
        workflow_id = "wf-1"

    monkeypatch.setattr(gh.workflow, "info", lambda: _Info())

    wf = gh.GreenhouseScraperWorkflow()
    summary = await wf.run()

    assert summary.site_count == 1
    assert summary.jobs_scraped == 1
    assert summary.scrape_ids == ["scrape123"]

    # Ensure every activity call passed arguments via the args kw and not positional args
    for activity, args, kwargs in calls:
        assert args == ()
        if activity is gh.lease_site:
            assert kwargs["args"] == ["scraper-worker", 300, "greenhouse"]
            assert kwargs["schedule_to_close_timeout"] == timedelta(seconds=30)
        if activity is gh.fetch_greenhouse_listing:
            assert kwargs["args"] == [{"_id": "site1", "url": "https://example.com"}]
        if activity is gh.filter_existing_job_urls:
            assert kwargs["args"] == [["https://example.com/job/1"]]
        if activity is gh.scrape_greenhouse_jobs:
            assert kwargs["args"][0]["urls"] == ["https://example.com/job/1"]
        if activity is gh.complete_site:
            assert kwargs["args"] == ["site1"]
