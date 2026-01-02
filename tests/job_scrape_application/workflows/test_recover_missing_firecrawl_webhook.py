from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import timedelta
from typing import Any, Dict, List

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

sys.path.insert(0, os.path.abspath("."))

import job_scrape_application.workflows.webhook_workflow as wf


@pytest.mark.asyncio
async def test_recover_missing_after_long_offline_marks_expired(monkeypatch):
    status_calls: List[str] = []
    collect_calls: List[Dict[str, Any]] = []
    stored_scrapes: List[Dict[str, Any]] = []
    marks: List[Dict[str, Any]] = []
    fail_calls: List[Dict[str, Any]] = []
    complete_calls: List[str] = []

    # Treat any pending job as immediately past the timeout window.
    monkeypatch.setattr(wf, "FIRECRAWL_WEBHOOK_RECHECK", timedelta(seconds=0))
    monkeypatch.setattr(wf, "FIRECRAWL_WEBHOOK_TIMEOUT", timedelta(seconds=0))

    @activity.defn
    async def get_firecrawl_webhook_status(job_id: str):
        status_calls.append(job_id)
        return {}

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        collect_calls.append(event)
        return {
            "kind": "site_crawl",
            "siteId": event.get("siteId"),
            "siteUrl": event.get("siteUrl"),
            "status": "cancelled_expired",
            "jobsScraped": 0,
            "scrape": None,
        }

    @activity.defn
    async def store_scrape(scrape: Dict[str, Any]):
        stored_scrapes.append(scrape)
        return "stored"

    @activity.defn
    async def mark_firecrawl_webhook_processed(webhook_id: str, error: str | None = None):
        marks.append({"id": webhook_id, "error": error})
        return None

    @activity.defn
    async def complete_site(site_id: str):
        complete_calls.append(site_id)
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):
        fail_calls.append(payload)
        return None

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
        return None

    monkeypatch.setattr(wf, "get_firecrawl_webhook_status", get_firecrawl_webhook_status, raising=False)
    monkeypatch.setattr(wf, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf, "record_workflow_run", record_workflow_run, raising=False)

    job_payload = {
        "jobId": "job-expired-50",
        "siteId": "site-expired",
        "siteUrl": "https://example.com/batch-50",
        "statusUrl": "https://api.firecrawl.dev/v2/batch/scrape/job-expired-50",
        "webhookId": "webhook-expired-50",
        "metadata": {"seedUrls": [f"https://example.com/job-{i}" for i in range(50)]},
        "receivedAt": 0,
    }

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"recover-expired-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[wf.RecoverMissingFirecrawlWebhookWorkflow],
            activities=[
                get_firecrawl_webhook_status,
                collect_firecrawl_job_result,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
            ],
        )

        async with worker:
            result = await env.client.execute_workflow(
                wf.RecoverMissingFirecrawlWebhookWorkflow.run,
                args=[job_payload],
                id=f"wf-recover-expired-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    # When the worker comes back after 24h, the pending job is treated as expired and closed out.
    assert status_calls and set(status_calls) == {"job-expired-50"}
    assert len(collect_calls) == 1
    assert len(stored_scrapes) == 1
    assert stored_scrapes[0]["items"]["raw"]["status"] == "cancelled_expired"
    assert marks == [{"id": "webhook-expired-50", "error": "cancelled_expired"}]
    assert complete_calls == ["site-expired"]
    assert not fail_calls
    assert result.recovered == 1
    assert result.failed == 0
    assert result.checked >= 1


@pytest.mark.asyncio
async def test_recover_missing_firecrawl_webhook_retries_after_timeout(monkeypatch):
    status_calls: List[str] = []
    collect_calls: List[Dict[str, Any]] = []
    stored_scrapes: List[Dict[str, Any]] = []
    marks: List[Dict[str, Any]] = []
    fail_calls: List[Dict[str, Any]] = []
    complete_calls: List[str] = []

    # Shorten timers so the test can skip ahead quickly while exercising 24h behavior.
    monkeypatch.setattr(wf, "FIRECRAWL_WEBHOOK_RECHECK", timedelta(seconds=5))
    monkeypatch.setattr(wf, "FIRECRAWL_WEBHOOK_TIMEOUT", timedelta(seconds=10))

    @activity.defn
    async def get_firecrawl_webhook_status(job_id: str):
        status_calls.append(job_id)
        return {}

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        collect_calls.append(event)
        return {
            "kind": "site_crawl",
            "siteId": event.get("siteId"),
            "siteUrl": event.get("siteUrl"),
            "status": "completed",
            "jobsScraped": 0,
            "scrape": {
                "provider": "firecrawl",
                "sourceUrl": event.get("siteUrl"),
                "items": {"normalized": []},
            },
        }

    @activity.defn
    async def store_scrape(scrape: Dict[str, Any]):
        stored_scrapes.append(scrape)
        return "stored"

    @activity.defn
    async def mark_firecrawl_webhook_processed(webhook_id: str, error: str | None = None):
        marks.append({"id": webhook_id, "error": error})
        return None

    @activity.defn
    async def complete_site(site_id: str):
        complete_calls.append(site_id)
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):
        fail_calls.append(payload)
        return None

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
        return None

    monkeypatch.setattr(wf, "get_firecrawl_webhook_status", get_firecrawl_webhook_status, raising=False)
    monkeypatch.setattr(wf, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf, "record_workflow_run", record_workflow_run, raising=False)

    job_payload = {
        "jobId": "job-missing-1",
        "siteId": "site-missing",
        "siteUrl": "https://example.com/missing",
        "statusUrl": "https://api.firecrawl.dev/v2/batch/scrape/job-missing-1",
        "webhookId": "webhook-missing-1",
        "metadata": {},
    }

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"recover-missing-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[wf.RecoverMissingFirecrawlWebhookWorkflow],
            activities=[
                get_firecrawl_webhook_status,
                collect_firecrawl_job_result,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
            ],
        )

        async with worker:
            result = await env.client.execute_workflow(
                wf.RecoverMissingFirecrawlWebhookWorkflow.run,
                args=[job_payload],
                id=f"wf-recover-missing-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    # Status is checked once immediately and again after the recheck delay before forcing a fetch.
    assert status_calls == ["job-missing-1", "job-missing-1"]
    assert len(collect_calls) == 1
    assert stored_scrapes  # recovered scrape persisted
    assert marks == [{"id": "webhook-missing-1", "error": None}]
    assert complete_calls == ["site-missing"]
    assert not fail_calls
    assert result.recovered == 1
    assert result.failed == 0
    assert result.checked >= 2


@pytest.mark.asyncio
async def test_backlogged_webhook_processed_before_recovery_retry(monkeypatch):
    # Simulate week-long downtime (beyond 24h) while a real webhook row waits in Convex.
    # Recovery workflow should see the pending webhook data and skip re-fetching, allowing
    # the ingest workflow to process the batch first even with multiple workers running.
    monkeypatch.setattr(wf, "FIRECRAWL_WEBHOOK_RECHECK", timedelta(seconds=0))
    monkeypatch.setattr(wf, "FIRECRAWL_WEBHOOK_TIMEOUT", timedelta(seconds=0))

    job_urls = [f"https://example.com/job-{i}" for i in range(3)]
    events_queue: List[Dict[str, Any]] = [
        {
            "_id": "wh-backlog-1",
            "jobId": "job-backlog-1",
            "event": "batch_scrape.completed",
            "status": "completed",
            "metadata": {"siteId": "site-backlog", "siteUrl": "https://example.com/listing"},
            "receivedAt": 1,
            "data": [{"job_urls": job_urls}],
        }
    ]

    webhook_state = {"hasRealEvent": True, "hasProcessed": False, "pendingProcessed": False}
    collect_events: List[str] = []
    scraped_payloads: List[Dict[str, Any]] = []
    marks: List[Dict[str, Any]] = []
    status_checks: List[str] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        nonlocal events_queue
        drained, events_queue = events_queue[:batch], events_queue[batch:]
        return drained

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        collect_events.append(event.get("event"))
        payload = event.get("data", [{}])[0] if event.get("data") else {}
        return {
            "kind": "greenhouse_listing",
            "siteId": event.get("metadata", {}).get("siteId"),
            "siteUrl": event.get("metadata", {}).get("siteUrl"),
            "status": event.get("status"),
            "job_urls": payload.get("job_urls") or [],
        }

    @activity.defn
    async def filter_existing_job_urls(urls: List[str]):
        return []

    @activity.defn
    async def compute_urls_to_scrape(job_urls: List[str], existing_urls: List[str] | None = None):
        cleaned = [u for u in job_urls if isinstance(u, str) and u.strip()]
        existing_set = {u for u in (existing_urls or []) if isinstance(u, str)}
        return {
            "urlsToScrape": [u for u in cleaned if u not in existing_set],
            "existingCount": len(existing_set),
            "totalCount": len(cleaned),
        }

    @activity.defn
    async def scrape_greenhouse_jobs(payload: Dict[str, Any]):
        scraped_payloads.append(payload)
        return {"scrape": None, "jobsScraped": len(payload.get("urls", []))}

    @activity.defn
    async def mark_firecrawl_webhook_processed(event_id: str, error: str | None = None):
        marks.append({"id": event_id, "error": error})
        if event_id == "wh-backlog-1":
            webhook_state["hasProcessed"] = True
        return None

    @activity.defn
    async def get_firecrawl_webhook_status(job_id: str):
        status_checks.append(job_id)
        return dict(webhook_state)

    @activity.defn
    async def complete_site(site_id: str):
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def store_scrape(scrape: Dict[str, Any]):
        return "stored"

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
        return None

    monkeypatch.setattr(wf, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)
    monkeypatch.setattr(wf, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf, "compute_urls_to_scrape", compute_urls_to_scrape, raising=False)
    monkeypatch.setattr(wf, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)
    monkeypatch.setattr(wf, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf, "get_firecrawl_webhook_status", get_firecrawl_webhook_status, raising=False)
    monkeypatch.setattr(wf, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf, "record_workflow_run", record_workflow_run, raising=False)

    job_payload = {
        "jobId": "job-backlog-1",
        "siteId": "site-backlog",
        "siteUrl": "https://example.com/listing",
        "statusUrl": "https://api.firecrawl.dev/v2/batch/scrape/job-backlog-1",
        "webhookId": "pending-backlog-1",
        "metadata": {"urls": job_urls},
        "receivedAt": 0,
    }

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"backlog-retry-{uuid.uuid4().hex[:6]}"
        # Two workers on the same queue, each capable of handling both workflows
        # to simulate multiple worker processes restarting together.
        worker_a = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[wf.ProcessWebhookIngestWorkflow, wf.RecoverMissingFirecrawlWebhookWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls,
                compute_urls_to_scrape,
                scrape_greenhouse_jobs,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                get_firecrawl_webhook_status,
            ],
        )
        worker_b = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[wf.ProcessWebhookIngestWorkflow, wf.RecoverMissingFirecrawlWebhookWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls,
                compute_urls_to_scrape,
                scrape_greenhouse_jobs,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                get_firecrawl_webhook_status,
            ],
        )

        async with worker_a, worker_b:
            ingest_run = env.client.execute_workflow(
                wf.ProcessWebhookIngestWorkflow.run,
                id=f"wf-backlog-ingest-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )
            recovery_run = env.client.execute_workflow(
                wf.RecoverMissingFirecrawlWebhookWorkflow.run,
                args=[job_payload],
                id=f"wf-backlog-recover-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

            ingest_result, recovery_result = await asyncio.gather(ingest_run, recovery_run)

    # Webhook ingestion should handle the backlog before any recovery retry fires.
    assert ingest_result.processed == 1
    assert [payload["urls"] for payload in scraped_payloads] == [job_urls]
    assert collect_events == ["batch_scrape.completed"]  # recovery never fetched again
    assert set(status_checks) == {"job-backlog-1"}
    assert any(mark["id"] == "wh-backlog-1" for mark in marks)
    assert recovery_result.recovered == 0
    assert recovery_result.failed == 0
