from __future__ import annotations

import asyncio
import os
import sys
import uuid
from typing import Any, Dict, List

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.testing.firecrawl_mock import (  # noqa: E402
    MockFirecrawl,
    MockFirecrawlScenario,
    MockWebhookQueue,
)
from job_scrape_application.workflows.webhook_workflow import (  # noqa: E402
    ProcessWebhookIngestWorkflow,
)


@pytest.mark.asyncio
async def test_process_webhook_with_mock_firecrawl(monkeypatch):
    queue = MockWebhookQueue()
    client = MockFirecrawl(webhook_queue=queue, webhook_delay=0.01)
    webhook = type(
        "WebhookStub",
        (),
        {
            "url": "https://demo.convex.site/api/firecrawl/webhook",
            "metadata": {"siteId": "site-int", "siteUrl": "https://example.com"},
        },
    )
    response = client(site_url="https://example.com", webhook=webhook)

    assert response.status_code == 200
    assert await queue.wait_for(1, timeout=0.5)

    processed_events: List[str] = []
    stored_scrapes: List[Dict[str, Any]] = []
    completed_sites: List[str] = []
    failed_sites: List[Dict[str, Any]] = []
    marks: List[Dict[str, Any]] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        return queue.drain(batch)

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        processed_events.append(event["_id"])
        metadata = event.get("metadata") or {}
        return {
            "jobId": event.get("jobId"),
            "siteId": metadata.get("siteId"),
            "siteUrl": metadata.get("siteUrl"),
            "scrape": {
                "provider": "firecrawl",
                "sourceUrl": metadata.get("siteUrl"),
                "items": {"normalized": [{"job_title": "Mocked", "url": metadata.get("siteUrl")}]},
            },
            "jobsScraped": 1,
            "status": event.get("status"),
            "kind": "site_crawl",
        }

    @activity.defn
    async def complete_site(site_id: str):
        completed_sites.append(site_id)
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):
        failed_sites.append(payload)
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

    monkeypatch.setattr(
        wf_mod,
        "fetch_pending_firecrawl_webhooks",
        fetch_pending_firecrawl_webhooks,
        raising=False,
    )
    monkeypatch.setattr(
        wf_mod,
        "collect_firecrawl_job_result",
        collect_firecrawl_job_result,
        raising=False,
    )
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(
        wf_mod,
        "mark_firecrawl_webhook_processed",
        mark_firecrawl_webhook_processed,
        raising=False,
    )
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf_mod, "compute_urls_to_scrape", compute_urls_to_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"mock-firecrawl-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                complete_site,
                fail_site,
                mark_firecrawl_webhook_processed,
                store_scrape,
                record_workflow_run,
                filter_existing_job_urls,
                compute_urls_to_scrape,
                scrape_greenhouse_jobs,
            ],
        )

        async with worker:
            result = await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-mock-firecrawl-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert result.processed == 1
    assert result.stored == 1
    assert result.failed == 0
    assert completed_sites == ["site-int"]
    assert marks and marks[0]["id"].startswith("wh-")
    assert processed_events == [marks[0]["id"]]
    assert stored_scrapes and stored_scrapes[0]["items"]["normalized"][0]["job_title"] == "Mocked"


@pytest.mark.asyncio
async def test_process_webhook_when_mock_webhook_missing(monkeypatch):
    queue = MockWebhookQueue()
    client = MockFirecrawl(
        scenario=MockFirecrawlScenario.WEBHOOK_POST_FAILS,
        webhook_queue=queue,
        webhook_delay=0.01,
    )
    webhook = {
        "url": "https://demo.convex.site/api/firecrawl/webhook",
        "metadata": {"siteId": "site-miss", "siteUrl": "https://example.com"},
    }
    response = client(site_url="https://example.com", webhook=webhook)

    assert response.status_code == 200
    await asyncio.sleep(0.05)

    processed_events: List[str] = []
    marks: List[Dict[str, Any]] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        return queue.drain(batch)

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        processed_events.append(event["_id"])
        return {
            "jobId": event.get("jobId"),
            "siteId": event.get("siteId"),
            "siteUrl": event.get("siteUrl"),
            "scrape": None,
            "jobsScraped": 0,
            "status": event.get("status"),
            "kind": "site_crawl",
        }

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
        return "stored"

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
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

    monkeypatch.setattr(
        wf_mod,
        "fetch_pending_firecrawl_webhooks",
        fetch_pending_firecrawl_webhooks,
        raising=False,
    )
    monkeypatch.setattr(
        wf_mod,
        "collect_firecrawl_job_result",
        collect_firecrawl_job_result,
        raising=False,
    )
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(
        wf_mod,
        "mark_firecrawl_webhook_processed",
        mark_firecrawl_webhook_processed,
        raising=False,
    )
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf_mod, "compute_urls_to_scrape", compute_urls_to_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"mock-firecrawl-miss-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ProcessWebhookIngestWorkflow],
            activities=[
                fetch_pending_firecrawl_webhooks,
                collect_firecrawl_job_result,
                complete_site,
                fail_site,
                mark_firecrawl_webhook_processed,
                store_scrape,
                record_workflow_run,
                filter_existing_job_urls,
                compute_urls_to_scrape,
                scrape_greenhouse_jobs,
            ],
        )

        async with worker:
            result = await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-mock-firecrawl-miss-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert result.processed == 0
    assert result.stored == 0
    assert result.failed == 0
    assert marks == []
    assert processed_events == []
