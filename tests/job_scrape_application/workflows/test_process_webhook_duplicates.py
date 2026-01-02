from __future__ import annotations

import os
import sys
import uuid
from typing import Any, Dict, List

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.webhook_workflow import ProcessWebhookIngestWorkflow


def _event(job_id: str, event_id: str, status: str = "completed") -> Dict[str, Any]:
    return {
        "_id": event_id,
        "jobId": job_id,
        "event": "batch_scrape.completed",
        "status": status,
        "metadata": {"siteId": "site-dup", "siteUrl": "https://example.com"},
        "receivedAt": 1,
    }


@pytest.mark.asyncio
async def test_duplicate_webhook_events_only_store_once(monkeypatch):
    queue: List[Dict[str, Any]] = [_event("job-1", "wh-1"), _event("job-1", "wh-2")]

    processed_events: List[str] = []
    stored_scrapes: List[Dict[str, Any]] = []
    marks: List[Dict[str, Any]] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        nonlocal queue
        drained, queue = queue[:batch], queue[batch:]
        return drained

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        processed_events.append(event["_id"])
        meta = event.get("metadata") or {}
        return {
            "jobId": event.get("jobId"),
            "siteId": meta.get("siteId"),
            "siteUrl": meta.get("siteUrl"),
            "scrape": {
                "provider": "firecrawl",
                "sourceUrl": meta.get("siteUrl"),
                "items": {"normalized": [{"job_title": "Mocked", "url": meta.get("siteUrl")}]},
            },
            "jobsScraped": 1,
            "status": event.get("status"),
            "kind": "site_crawl",
        }

    @activity.defn
    async def mark_firecrawl_webhook_processed(event_id: str, error: str | None = None):
        marks.append({"id": event_id, "error": error})
        return None

    @activity.defn
    async def complete_site(site_id: str):
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def store_scrape(scrape: Dict[str, Any]):
        stored_scrapes.append(scrape)
        return "stored"

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def record_scratchpad(payload: Dict[str, Any]):
        return None

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
        return {"scrape": None, "jobsScraped": 0}

    import job_scrape_application.workflows.webhook_workflow as wf_mod

    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)
    monkeypatch.setattr(wf_mod, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf_mod, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "record_scratchpad", record_scratchpad, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf_mod, "compute_urls_to_scrape", compute_urls_to_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"mock-dup-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
                filter_existing_job_urls,
                compute_urls_to_scrape,
                scrape_greenhouse_jobs,
            ],
        )

        async with worker:
            result = await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-dup-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    # Only one scrape stored even though two webhook rows exist
    assert result.processed == 1
    assert len(stored_scrapes) == 1
    assert processed_events == ["wh-1"]
    # Both webhook rows should be marked processed (one as duplicate)
    assert {m["id"] for m in marks} == {"wh-1", "wh-2"}


@pytest.mark.asyncio
async def test_duplicate_success_after_failure_only_keeps_first(monkeypatch):
    queue: List[Dict[str, Any]] = [_event("job-2", "wh-a", status="failed"), _event("job-2", "wh-b")]

    stored_scrapes: List[Dict[str, Any]] = []
    failures: List[str] = []
    marks: List[Dict[str, Any]] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        nonlocal queue
        drained, queue = queue[:batch], queue[batch:]
        return drained

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        if event.get("status") == "failed":
            failures.append(event.get("_id"))
            raise ApplicationError("mock failure", non_retryable=True)
        meta = event.get("metadata") or {}
        return {
            "jobId": event.get("jobId"),
            "siteId": meta.get("siteId"),
            "siteUrl": meta.get("siteUrl"),
            "scrape": {
                "provider": "firecrawl",
                "sourceUrl": meta.get("siteUrl"),
                "items": {"normalized": [{"job_title": "Mocked", "url": meta.get("siteUrl")}]},
            },
            "jobsScraped": 1,
            "status": event.get("status"),
            "kind": "site_crawl",
        }

    @activity.defn
    async def mark_firecrawl_webhook_processed(event_id: str, error: str | None = None):
        marks.append({"id": event_id, "error": error})
        return None

    @activity.defn
    async def complete_site(site_id: str):
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def store_scrape(scrape: Dict[str, Any]):
        stored_scrapes.append(scrape)
        return "stored"

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def record_scratchpad(payload: Dict[str, Any]):
        return None

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
        return {"scrape": None, "jobsScraped": 0}

    import job_scrape_application.workflows.webhook_workflow as wf_mod

    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)
    monkeypatch.setattr(wf_mod, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf_mod, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "record_scratchpad", record_scratchpad, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf_mod, "compute_urls_to_scrape", compute_urls_to_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"mock-dup-fail-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
                filter_existing_job_urls,
                compute_urls_to_scrape,
                scrape_greenhouse_jobs,
            ],
        )

        async with worker:
            result = await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-dup-fail-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    # Failure event should cause retry skip; success duplicate should be ignored by dedup set
    assert result.processed == 1
    assert len(stored_scrapes) == 0  # failure prevented storage, duplicate skipped
    assert failures == ["wh-a"]
    assert {m["id"] for m in marks} == {"wh-a", "wh-b"}
