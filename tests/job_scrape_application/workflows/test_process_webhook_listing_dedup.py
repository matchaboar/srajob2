from __future__ import annotations

import os
import sys
import uuid
from typing import Any, Dict, List

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.webhook_workflow import ProcessWebhookIngestWorkflow


def _listing_event(job_urls: List[str], event_id: str = "wh-list-1") -> Dict[str, Any]:
    return {
        "_id": event_id,
        "jobId": "job-list-1",
        "event": "batch_scrape.completed",
        "status": "completed",
        "metadata": {"siteId": "site-list", "siteUrl": "https://example.com/list"},
        "receivedAt": 1,
        "data": [{"job_urls": job_urls}],
    }


@pytest.mark.asyncio
async def test_listing_webhook_scrapes_new_urls_only_once(monkeypatch):
    # Simulate worker cold start; dedup derives from Convex existing URLs
    events_queue: List[Dict[str, Any]] = [_listing_event(["https://example.com/j1", "https://example.com/j2"])]
    stored_scrapes: List[Dict[str, Any]] = []
    marks: List[Dict[str, Any]] = []
    scraped_urls: List[str] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        nonlocal events_queue
        drained, events_queue = events_queue[:batch], events_queue[batch:]
        return drained

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        # Return listing payload with job_urls
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
        # First run: none exist
        return []

    @activity.defn
    async def scrape_greenhouse_jobs(payload: Dict[str, Any]):
        scraped_urls.extend(payload.get("urls", []))
        return {"scrape": None, "jobsScraped": len(payload.get("urls", []))}

    @activity.defn
    async def complete_site(site_id: str):
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def mark_firecrawl_webhook_processed(event_id: str, error: str | None = None):
        marks.append({"id": event_id, "error": error})
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

    import job_scrape_application.workflows.webhook_workflow as wf_mod

    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)
    monkeypatch.setattr(wf_mod, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf_mod, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "record_scratchpad", record_scratchpad, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"listing-dedup-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls,
                scrape_greenhouse_jobs,
                complete_site,
                fail_site,
                mark_firecrawl_webhook_processed,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
            ],
        )

        async with worker:
            await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-listing-dedup-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert scraped_urls == ["https://example.com/j1", "https://example.com/j2"]

    # Simulate new worker instance restart and second webhook run; now both URLs are existing
    events_queue = [_listing_event(["https://example.com/j1", "https://example.com/j2"], event_id="wh-list-2")]
    scraped_urls.clear()

    @activity.defn(name="filter_existing_job_urls")
    async def filter_existing_job_urls_second(urls: List[str]):
        return list(urls)  # all already present

    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls_second, raising=False)
    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"listing-dedup2-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls_second,
                scrape_greenhouse_jobs,
                complete_site,
                fail_site,
                mark_firecrawl_webhook_processed,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
            ],
        )

        async with worker:
            await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-listing-dedup2-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert scraped_urls == []


@pytest.mark.asyncio
async def test_listing_webhook_batches_urls_once_with_idempotency(monkeypatch):
    job_urls = [f"https://example.com/job-{i}" for i in range(50)]
    events_queue: List[Dict[str, Any]] = [
        _listing_event(job_urls, event_id="wh-batch-50"),
    ]
    scraped_payloads: List[Dict[str, Any]] = []
    marks: List[Dict[str, Any]] = []
    stored_scrapes: List[Dict[str, Any]] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        nonlocal events_queue
        drained, events_queue = events_queue[:batch], events_queue[batch:]
        return drained

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        meta = event.get("metadata") or {}
        return {
            "kind": "greenhouse_listing",
            "siteId": meta.get("siteId"),
            "siteUrl": meta.get("siteUrl"),
            "status": event.get("status"),
            "job_urls": job_urls,
        }

    @activity.defn
    async def filter_existing_job_urls(urls: List[str]):
        return []

    @activity.defn
    async def scrape_greenhouse_jobs(payload: Dict[str, Any]):
        scraped_payloads.append(payload)
        return {"scrape": None, "jobsScraped": len(payload.get("urls", []))}

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

    import job_scrape_application.workflows.webhook_workflow as wf_mod

    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)
    monkeypatch.setattr(wf_mod, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)
    monkeypatch.setattr(wf_mod, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "record_scratchpad", record_scratchpad, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"listing-batch-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls,
                scrape_greenhouse_jobs,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
            ],
        )

        async with worker:
            await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-listing-batch-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert len(scraped_payloads) == 1
    assert scraped_payloads[0]["urls"] == job_urls
    assert scraped_payloads[0]["idempotency_key"] == "wh-batch-50"
    assert scraped_payloads[0]["webhook_id"] == "wh-batch-50"

    # Second worker run sees the same webhook but all URLs already exist so no duplicate batch is sent.
    events_queue = [_listing_event(job_urls, event_id="wh-batch-50")]

    @activity.defn(name="filter_existing_job_urls")
    async def filter_existing_job_urls_existing(urls: List[str]):
        return list(urls)

    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls_existing, raising=False)
    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"listing-batch2-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls_existing,
                scrape_greenhouse_jobs,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
            ],
        )

        async with worker:
            await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-listing-batch2-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert len(scraped_payloads) == 1
    assert stored_scrapes  # listing still stored/marked but no new Firecrawl batch


@pytest.mark.asyncio
async def test_individual_job_url_scraped_once_forever(monkeypatch):
    # Dedup relies on filter_existing_job_urls — simulate existing Convex record on second run.
    events_queue: List[Dict[str, Any]] = [_listing_event(["https://example.com/job-unique"], event_id="wh-single-1")]
    marks: List[Dict[str, Any]] = []
    scraped_urls: List[str] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        nonlocal events_queue
        drained, events_queue = events_queue[:batch], events_queue[batch:]
        return drained

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        return {
            "kind": "greenhouse_listing",
            "siteId": "site-single",
            "siteUrl": "https://example.com/list",
            "status": event.get("status"),
            "job_urls": ["https://example.com/job-unique"],
        }

    @activity.defn(name="filter_existing_job_urls")
    async def filter_existing_job_urls_first(urls: List[str]):
        return []

    @activity.defn(name="filter_existing_job_urls")
    async def filter_existing_job_urls_second(urls: List[str]):
        return list(urls)

    @activity.defn
    async def scrape_greenhouse_jobs(payload: Dict[str, Any]):
        scraped_urls.extend(payload.get("urls", []))
        return {"scrape": None, "jobsScraped": len(payload.get("urls", []))}

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
        return "stored"

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def record_scratchpad(payload: Dict[str, Any]):
        return None

    import job_scrape_application.workflows.webhook_workflow as wf_mod

    # First run: no existing job URLs
    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)
    monkeypatch.setattr(wf_mod, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls_first, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)
    monkeypatch.setattr(wf_mod, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "record_scratchpad", record_scratchpad, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"job-once-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls_first,
                scrape_greenhouse_jobs,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
            ],
        )

        async with worker:
            await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-job-once-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert scraped_urls == ["https://example.com/job-unique"]

    # Second run with new worker; Convex reports URL exists so scrape should be skipped forever
    events_queue = [_listing_event(["https://example.com/job-unique"], event_id="wh-single-2")]
    scraped_urls.clear()
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls_second, raising=False)
    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"job-once2-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                filter_existing_job_urls_second,
                scrape_greenhouse_jobs,
                mark_firecrawl_webhook_processed,
                complete_site,
                fail_site,
                store_scrape,
                record_workflow_run,
                record_scratchpad,
            ],
        )

        async with worker:
            await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-job-once2-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert scraped_urls == []
