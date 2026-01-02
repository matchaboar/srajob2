from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import webhook_workflow as wf  # noqa: E402
from job_scrape_application.workflows.exceptions import PaymentRequiredWorkflowError  # noqa: E402


def _compute_urls_to_scrape(job_urls, existing):
    cleaned = [u for u in (job_urls or []) if isinstance(u, str) and u.strip()]
    existing_set = {u for u in (existing or []) if isinstance(u, str)}
    return {
        "urlsToScrape": [u for u in cleaned if u not in existing_set],
        "existingCount": len(existing_set),
        "totalCount": len(cleaned),
    }


@pytest.mark.asyncio
async def test_process_webhook_workflow_continues_on_failure(monkeypatch):
    events = [
        {"_id": "e1", "event": "completed", "siteId": "s1", "siteUrl": "https://one"},
        {"_id": "e2", "event": "completed", "siteId": "s2", "siteUrl": "https://two"},
    ]
    fetch_calls = {"count": 0}
    calls: Dict[str, Any] = {
        "collect": [],
        "store": [],
        "mark": [],
        "complete": [],
        "fail_site": [],
        "record": None,
    }

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            event = args[0]
            if event["_id"] == "e2":
                raise wf.ApplicationError("collect failed", non_retryable=True)
            calls["collect"].append(event["_id"])
            return {
                "kind": "greenhouse_listing",
                "siteId": event["siteId"],
                "siteUrl": event["siteUrl"],
                "job_urls": ["https://jobs/1"],
                "scrape": {"items": {"normalized": [{}]}},
                "jobsScraped": 1,
            }
        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.compute_urls_to_scrape:
            return _compute_urls_to_scrape(args[0], args[1] if len(args) > 1 else [])
        if fn is wf.scrape_greenhouse_jobs:
            return {"jobsScraped": 1, "scrape": {"items": {"normalized": [{}]}}}
        if fn is wf.store_scrape:
            calls["store"].append(args[0])
            return None
        if fn is wf.complete_site:
            calls["complete"].append(args[0])
            return None
        if fn is wf.fail_site:
            calls["fail_site"].append(args[0])
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
        lambda: type("Info", (), {"run_id": "r2", "workflow_id": "ProcessWebhook/1"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 2
    assert summary.failed == 1
    assert summary.stored == 1
    assert summary.jobs_scraped == 1

    # Failure path still marks webhook and calls fail_site
    assert calls["mark"][-1][0] == "e2"
    assert calls["fail_site"] == [{"id": "s2", "error": "collect failed"}]
    assert calls["record"]["status"] == "failed"


@pytest.mark.asyncio
async def test_process_webhook_greenhouse_scrapes_new_job_urls(monkeypatch):
    events = [
        {
            "_id": "gh1",
            "event": "batch_scrape.completed",
            "siteId": "site-1",
            "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
            "metadata": {
                "siteId": "site-1",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
            },
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, Any] = {
        "filter": [],
        "scrape": [],
        "store": [],
        "mark": [],
        "complete": [],
        "record": None,
    }

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            return {
                "kind": "greenhouse_listing",
                "siteId": "site-1",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
                "status": "completed",
                "job_urls": ["https://jobs/1", "https://jobs/2"],
                "raw": "{}",
            }
        if fn is wf.filter_existing_job_urls:
            calls["filter"].append(args[0])
            return ["https://jobs/1"]  # pretend the first URL already exists
        if fn is wf.compute_urls_to_scrape:
            return _compute_urls_to_scrape(args[0], args[1] if len(args) > 1 else [])
        if fn is wf.scrape_greenhouse_jobs:
            calls["scrape"].append(args[0])
            return {
                "jobsScraped": 1,
                "scrape": {
                    "sourceUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
                    "items": {"normalized": [{"title": "New Role"}]},
                },
            }
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
            calls["record"] = args[0]
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r-gh", "workflow_id": "ProcessWebhook/greenhouse"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 1
    assert summary.failed == 0
    assert summary.stored == 1
    assert summary.jobs_scraped == 1

    assert calls["filter"] == [["https://jobs/1", "https://jobs/2"]]
    assert calls["scrape"] == [
        {
            "urls": ["https://jobs/2"],
            "source_url": "https://api.greenhouse.io/v1/boards/example/jobs",
            "idempotency_key": "gh1",
            "webhook_id": "gh1",
        }
    ]
    assert calls["store"], "Scrape result should be stored"
    assert calls["mark"] == [("gh1", None)]
    assert calls["complete"] == ["site-1"]
    assert calls["record"]["status"] == "completed"


@pytest.mark.asyncio
async def test_process_webhook_batches_up_to_50_urls(monkeypatch):
    all_job_urls = [f"https://jobs/{i}" for i in range(75)]
    events = [
        {
            "_id": "gh-many",
            "event": "batch_scrape.completed",
            "siteId": "site-many",
            "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
            "metadata": {
                "siteId": "site-many",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
            },
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, Any] = {"filter": [], "scrape": [], "mark": [], "complete": [], "record": None}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            return {
                "kind": "greenhouse_listing",
                "siteId": "site-many",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
                "status": "completed",
                "job_urls": list(all_job_urls),
                "raw": "{}",
            }
        if fn is wf.filter_existing_job_urls:
            calls["filter"].append(list(args[0]))
            # Pretend Convex already has the first 25 URLs; 50 remain pending
            return all_job_urls[:25]
        if fn is wf.compute_urls_to_scrape:
            return _compute_urls_to_scrape(args[0], args[1] if len(args) > 1 else [])
        if fn is wf.scrape_greenhouse_jobs:
            payload = args[0]
            calls["scrape"].append(payload)
            return {
                "jobsScraped": len(payload.get("urls", [])),
                "scrape": {
                    "sourceUrl": payload.get("source_url"),
                    "items": {"normalized": [{"url": u} for u in payload.get("urls", [])]},
                },
            }
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
        lambda: type("Info", (), {"run_id": "r-batch50", "workflow_id": "ProcessWebhook/batch50"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 1
    assert summary.failed == 0
    assert summary.stored == 1
    assert summary.jobs_scraped == 50

    assert calls["filter"] == [all_job_urls]
    assert len(calls["scrape"]) == 1
    scraped_urls = calls["scrape"][0]["urls"]
    assert len(scraped_urls) == 50
    assert set(scraped_urls) == set(all_job_urls[25:])
    assert calls["mark"] == [("gh-many", None)]
    assert calls["complete"] == ["site-many"]
    assert calls["record"]["status"] == "completed"


@pytest.mark.asyncio
async def test_process_webhook_stores_single_job_scrape(monkeypatch):
    events = [
        {
            "_id": "single",
            "event": "completed",
            "siteId": "site-2",
            "siteUrl": "https://example.com/job",
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, Any] = {"store": [], "mark": [], "complete": [], "record": None}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            return {
                "kind": "site_crawl",
                "siteId": "site-2",
                "siteUrl": "https://example.com/job",
                "status": "completed",
                "scrape": {"sourceUrl": "https://example.com/job", "items": {"normalized": [{"title": "Solo"}]}},
                "jobsScraped": 1,
            }
        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.compute_urls_to_scrape:
            return _compute_urls_to_scrape(args[0], args[1] if len(args) > 1 else [])
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
            calls["record"] = args[0]
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r-single", "workflow_id": "ProcessWebhook/single"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 1
    assert summary.failed == 0
    assert summary.stored == 1
    assert summary.jobs_scraped == 1
    assert calls["store"] and calls["store"][0]["items"]["normalized"] == [{"title": "Solo"}]
    assert calls["mark"] == [("single", None)]
    assert calls["complete"] == ["site-2"]
    assert calls["record"]["status"] == "completed"


@pytest.mark.asyncio
async def test_process_webhook_retries_on_transient_429(monkeypatch):
    events = [
        {
            "_id": "retry-429",
            "event": "completed",
            "siteId": "site-3",
            "siteUrl": "https://example.com",
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
        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.compute_urls_to_scrape:
            return _compute_urls_to_scrape(args[0], args[1] if len(args) > 1 else [])
        if fn is wf.scrape_greenhouse_jobs:
            return {"jobsScraped": 0, "scrape": None}
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
            calls["record"] = args[0]
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r-retry", "workflow_id": "ProcessWebhook/retry"})(),
    )

    with pytest.raises(wf.ApplicationError):
        await wf.ProcessWebhookIngestWorkflow().run()

    # Transient errors should bubble so the workflow can be retried; no side effects recorded
    assert calls["mark"] == []
    assert calls["fail"] == []
    assert calls["record"]["status"] == "retry"


@pytest.mark.asyncio
async def test_process_webhook_payment_required_marks_failed(monkeypatch):
    events = [
        {
            "_id": "pay-1",
            "event": "batch_scrape.completed",
            "siteId": "site-pay",
            "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
            "metadata": {
                "siteId": "site-pay",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
            },
        }
    ]

    fetch_calls = {"count": 0}
    calls: Dict[str, Any] = {"mark": [], "fail": [], "record": None}

    async def fake_execute_activity(fn, args=None, **_kwargs):  # type: ignore[override]
        if fn is wf.fetch_pending_firecrawl_webhooks:
            fetch_calls["count"] += 1
            return events if fetch_calls["count"] == 1 else []
        if fn is wf.collect_firecrawl_job_result:
            return {
                "kind": "greenhouse_listing",
                "siteId": "site-pay",
                "siteUrl": "https://api.greenhouse.io/v1/boards/example/jobs",
                "status": "completed",
                "job_urls": ["https://jobs/1"],
                "raw": "{}",
            }
        if fn is wf.filter_existing_job_urls:
            return []
        if fn is wf.compute_urls_to_scrape:
            return _compute_urls_to_scrape(args[0], args[1] if len(args) > 1 else [])
        if fn is wf.scrape_greenhouse_jobs:
            raise PaymentRequiredWorkflowError("Payment Required: insufficient credits")
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
            calls["record"] = args[0]
            return None
        raise AssertionError(f"Unexpected activity {fn}")

    monkeypatch.setattr(wf.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(wf.workflow, "now", lambda: datetime.fromtimestamp(1))
    monkeypatch.setattr(
        wf.workflow,
        "info",
        lambda: type("Info", (), {"run_id": "r-pay", "workflow_id": "ProcessWebhook/payment"})(),
    )

    summary = await wf.ProcessWebhookIngestWorkflow().run()

    assert summary.processed == 1
    assert summary.failed == 1
    assert summary.stored == 0
    assert summary.jobs_scraped == 0

    assert calls["mark"] == [("pay-1", "Payment Required: insufficient credits")]
    assert calls["fail"] == [{"id": "site-pay", "error": "Payment Required: insufficient credits"}]
    assert calls["record"]["status"] == "failed"
