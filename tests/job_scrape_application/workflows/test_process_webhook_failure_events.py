from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import webhook_workflow as wf  # noqa: E402


@pytest.mark.asyncio
async def test_process_webhook_handles_fail_event(monkeypatch):
    events = [
        {"_id": "e1", "event": "failed", "siteId": "s1", "status": "error", "metadata": {}},
        {"_id": "e2", "event": "completed", "siteId": "s2", "siteUrl": "https://two"},
    ]

    calls: Dict[str, Any] = {"fail": [], "mark": [], "collect": []}

    fetch_calls = {"count": 0}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            calls["collect"].append(args[0]["_id"])
            return {
                "kind": "site_crawl",
                "siteId": "s2",
                "siteUrl": "https://two",
                "scrape": {"items": {"normalized": []}},
                "jobsScraped": 0,
            }
        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.store_scrape:
            return None
        if fn is wf.complete_site:
            return None
        if fn is wf.fail_site:
            calls["fail"].append(args[0])
            return None
        if fn is wf.mark_firecrawl_webhook_processed:
            calls["mark"].append((args[0], args[1]))
            return None
        if fn is wf.record_workflow_run:
            return None
        return None

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r3", "workflow_id": "ProcessWebhook/2"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    # First event skipped to fail_site + mark; second processed
    assert summary.processed == 2
    assert summary.failed == 0
    assert calls["fail"] == [{"id": "s1", "error": "error"}]
    assert calls["collect"] == ["e2"]
    assert calls["mark"][0][0] == "e1"
