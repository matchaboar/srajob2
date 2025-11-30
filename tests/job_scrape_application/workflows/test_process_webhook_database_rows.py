from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import webhook_workflow as wf  # noqa: E402


@pytest.mark.asyncio
async def test_process_webhook_marks_invalid_job_id_processed(monkeypatch):
    # Mirrors row md7e106pyvt7xad8719bd18vp97w30ke (invalid job id, completed event)
    events = [
        {
            "_id": "md7e106pyvt7xad8719bd18vp97w30ke",
            "event": "completed",
            "jobId": "manual-test-job-002",
            "siteId": "manual-site-1",
            "siteUrl": "https://example.com/manual",
            "metadata": {"siteId": "manual-site-1", "siteUrl": "https://example.com/manual"},
            "status": "completed",
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, Any] = {"mark": [], "complete": [], "record": None}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []

        if fn is wf.collect_firecrawl_job_result:
            # Simulate the activity returning a handled error (no scrape payload)
            return {
                "kind": "site_crawl",
                "siteId": "manual-site-1",
                "siteUrl": "https://example.com/manual",
                "status": "error",
                "jobsScraped": 0,
                "error": "Bad Request: Failed to get crawl status. Invalid job ID - No additional error details provided.",
                "scrape": None,
            }

        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.scrape_greenhouse_jobs:
            return {"jobsScraped": 0, "scrape": None}
        if fn is wf.store_scrape:
            return None
        if fn is wf.complete_site:
            calls["complete"].append(args[0])
            return None
        if fn is wf.fail_site:
            return None
        if fn is wf.mark_firecrawl_webhook_processed:
            calls["mark"].append((args[0], args[1]))
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
        lambda: type("Info", (), {"run_id": "r-invalid", "workflow_id": "ProcessWebhook/invalid"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 1
    assert summary.failed == 0
    assert summary.jobs_scraped == 0
    # Mark should include the error string so the row is considered processed
    assert calls["mark"] == [("md7e106pyvt7xad8719bd18vp97w30ke", "Bad Request: Failed to get crawl status. Invalid job ID - No additional error details provided.")]
    # Site should still be completed best-effort
    assert calls["complete"] == ["manual-site-1"]
    assert calls["record"]["status"] == "completed"


@pytest.mark.asyncio
async def test_process_webhook_handles_batch_scrape_completed_greenhouse(monkeypatch):
    # Mirrors row md7850tezwfv9nvzv2m4mmnayd7w2da3 (batch scrape completed, greenhouse)
    events = [
        {
            "_id": "md7850tezwfv9nvzv2m4mmnayd7w2da3",
            "event": "batch_scrape.completed",
            "jobId": "bfe21ec4-beaf-4779-8e48-efbd91a83cf3",
            "metadata": {
                "kind": "greenhouse_listing",
                "siteId": "kd7fsv55v6a7a9vfwg4emh0rwn7w25dd",
                "siteType": "greenhouse",
                "siteUrl": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
            },
            "processed": False,
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, List[Any]] = {"collect": [], "mark": [], "store": [], "complete": []}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []

        if fn is wf.collect_firecrawl_job_result:
            calls["collect"].append(args[0]["_id"])
            return {
                "kind": "greenhouse_listing",
                "siteId": "kd7fsv55v6a7a9vfwg4emh0rwn7w25dd",
                "siteUrl": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
                "status": "completed",
                "job_urls": ["https://jobs/robinhood/1"],
                "raw": "{}",
            }

        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.scrape_greenhouse_jobs:
            return {"jobsScraped": 1, "scrape": {"items": {"normalized": [{"title": "Role"}]}}}
        if fn is wf.store_scrape:
            calls["store"].append(args[0])
            return None
        if fn is wf.complete_site:
            calls["complete"].append(args[0])
            return None
        if fn is wf.mark_firecrawl_webhook_processed:
            calls["mark"].append((args[0], args[1]))
            return None
        if fn is wf.record_workflow_run:
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r-batch", "workflow_id": "ProcessWebhook/batch"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 1
    assert summary.failed == 0
    assert summary.stored == 1
    assert summary.jobs_scraped == 1
    assert calls["collect"] == ["md7850tezwfv9nvzv2m4mmnayd7w2da3"]
    # Should mark processed with no error
    assert calls["mark"] == [("md7850tezwfv9nvzv2m4mmnayd7w2da3", None)]
    # Ensure scrape stored and site completed
    assert calls["store"], "Scrape payload should be stored"
    assert calls["complete"] == ["kd7fsv55v6a7a9vfwg4emh0rwn7w25dd"]


@pytest.mark.asyncio
async def test_greenhouse_listing_with_no_jobs_still_stores_raw(monkeypatch):
    events = [
        {
            "_id": "md-empty",
            "event": "batch_scrape.completed",
            "jobId": "job-empty",
            "metadata": {
                "kind": "greenhouse_listing",
                "siteId": "site-empty",
                "siteType": "greenhouse",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
            },
            "status": "completed",
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, List[Any]] = {"collect": [], "mark": [], "store": [], "complete": []}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            calls["collect"].append(args[0]["_id"])
            return {
                "kind": "greenhouse_listing",
                "siteId": "site-empty",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
                "status": "completed",
                "job_urls": [],
                "raw": "{\"items\":[]}",
            }
        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.scrape_greenhouse_jobs:
            return {"jobsScraped": 0, "scrape": None}
        if fn is wf.store_scrape:
            calls["store"].append(args[0])
            return None
        if fn is wf.complete_site:
            calls["complete"].append(args[0])
            return None
        if fn is wf.mark_firecrawl_webhook_processed:
            calls["mark"].append((args[0], args[1]))
            return None
        if fn is wf.record_workflow_run:
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r-empty", "workflow_id": "ProcessWebhook/empty"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 1
    assert summary.stored == 1
    assert summary.jobs_scraped == 0
    assert calls["mark"] == [("md-empty", None)]
    assert calls["complete"] == ["site-empty"]
    assert calls["store"], "Raw listing should be stored even when job list is empty"


@pytest.mark.asyncio
async def test_pending_webhook_retries_on_429(monkeypatch):
    """Regression: webhook row stuck when Firecrawl status 429s repeatedly."""

    events = [
        {
            "_id": "67348f58-c6fc-4ff9-b409-7b480e60fd92",
            "event": "completed",
            "jobId": "67348f58-c6fc-4ff9-b409-7b480e60fd92",
            "siteId": "site-stuck",
            "siteUrl": "https://example.com/stuck",
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, Any] = {"mark": [], "fail": [], "record": None}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            raise wf.ApplicationError("429 Too Many Requests")
        if fn is wf.mark_firecrawl_webhook_processed:
            calls["mark"].append((args[0], args[1]))
            return None
        if fn is wf.fail_site:
            calls["fail"].append(args[0])
            return None
        if fn is wf.record_workflow_run:
            calls["record"] = args[0]
            return None
        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.scrape_greenhouse_jobs:
            return {"jobsScraped": 0, "scrape": None}
        if fn is wf.store_scrape:
            return None
        if fn is wf.complete_site:
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r-stuck", "workflow_id": "ProcessWebhook/stuck"})(),
    )

    with pytest.raises(wf.ApplicationError):
        await wf.ProcessWebhookIngestWorkflow().run()

    # On transient 429 we should raise to allow workflow retry and leave webhook row untouched.
    assert calls["mark"] == []
    assert calls["fail"] == []
    assert calls["record"]["status"] == "retry"
