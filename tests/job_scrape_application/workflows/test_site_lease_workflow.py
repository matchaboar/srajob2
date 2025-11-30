from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import webhook_workflow as wf  # noqa: E402


@pytest.mark.asyncio
async def test_site_lease_workflow_handles_activity_failure(monkeypatch):
    site1 = {"_id": "s1", "url": "https://one.example"}
    site2 = {"_id": "s2", "url": "https://two.example"}
    lease_iter = iter([site1, site2, None])

    calls: Dict[str, Any] = {"fail_site": [], "record": None, "jobs_started": []}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.lease_site:
            return next(lease_iter)
        if fn is wf.start_firecrawl_webhook_scrape:
            site_arg = args[0]
            if site_arg["_id"] == "s2":
                raise wf.ApplicationError("firecrawl failed", non_retryable=True)
            calls["jobs_started"].append(site_arg["_id"])
            return {"jobId": "job-1", "kind": "site_crawl"}
        if fn is wf.fail_site:
            calls["fail_site"].append(args[0])
            return None
        if fn is wf.record_workflow_run:
            calls["record"] = args[0]
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r1", "workflow_id": "SiteLease/1"})(),
    )

    result = await wf.SiteLeaseWorkflow().run()

    assert result.leased == 2
    assert result.jobs_started == 1
    assert calls["fail_site"] == [{"id": "s2", "error": "firecrawl failed"}]
    assert calls["record"]["status"] == "failed"
