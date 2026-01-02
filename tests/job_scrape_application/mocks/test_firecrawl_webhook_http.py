from __future__ import annotations

import uuid
from typing import Any, Dict, List

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.testing.firecrawl_mock import (
    MockFirecrawlWebhookServer,
    MockWebhookQueue,
)
from job_scrape_application.workflows.webhook_workflow import ProcessWebhookIngestWorkflow


def _base_payload(job_id: str = "job-1") -> Dict[str, Any]:
    return {
        "_id": f"wh-{job_id}",
        "jobId": job_id,
        "event": "batch_scrape.completed",
        "status": "completed",
        "metadata": {"siteId": "site-123", "siteUrl": "https://example.com"},
        "receivedAt": 1,
    }


def test_webhook_server_validates_input():
    server = MockFirecrawlWebhookServer()

    resp = server.post({})
    assert resp.status_code == 400
    assert "jobId" in resp.json()["error"]

    resp = server.post({"jobId": "j1"})
    assert resp.status_code == 400

    ok = server.post(_base_payload("j2"))
    assert ok.status_code == 200


@pytest.mark.asyncio
async def test_webhook_post_then_workflow_consumes(monkeypatch):
    queue = MockWebhookQueue()
    server = MockFirecrawlWebhookServer(queue=queue)

    # Simulate HTTP POST from Firecrawl
    post_resp = server.post(_base_payload("job-post"))
    assert post_resp.status_code == 200

    processed_events: List[str] = []
    marks: List[Dict[str, Any]] = []
    stored_scrapes: List[Dict[str, Any]] = []

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
    monkeypatch.setattr(wf_mod, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(wf_mod, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(wf_mod, "mark_firecrawl_webhook_processed", mark_firecrawl_webhook_processed, raising=False)
    monkeypatch.setattr(wf_mod, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(wf_mod, "record_scratchpad", record_scratchpad, raising=False)
    monkeypatch.setattr(wf_mod, "filter_existing_job_urls", filter_existing_job_urls, raising=False)
    monkeypatch.setattr(wf_mod, "compute_urls_to_scrape", compute_urls_to_scrape, raising=False)
    monkeypatch.setattr(wf_mod, "scrape_greenhouse_jobs", scrape_greenhouse_jobs, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"mock-webhook-{uuid.uuid4().hex[:6]}"
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
                record_scratchpad,
                filter_existing_job_urls,
                compute_urls_to_scrape,
                scrape_greenhouse_jobs,
            ],
        )

        async with worker:
            result = await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-mock-webhook-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert result.processed == 1
    assert processed_events == ["wh-job-post"]
    assert marks and marks[0]["id"] == "wh-job-post"
    assert stored_scrapes and stored_scrapes[0]["items"]["normalized"][0]["job_title"] == "Mocked"


@pytest.mark.asyncio
async def test_webhook_post_invalid_never_reaches_workflow(monkeypatch):
    queue = MockWebhookQueue()
    server = MockFirecrawlWebhookServer(queue=queue)

    bad = server.post({"event": "batch_scrape.completed"})
    assert bad.status_code == 400
    assert queue.drain() == []

    processed_events: List[str] = []

    @activity.defn
    async def fetch_pending_firecrawl_webhooks(batch: int = 25, cursor: str | None = None):
        return queue.drain(batch)

    @activity.defn
    async def collect_firecrawl_job_result(event: Dict[str, Any]):
        processed_events.append(event.get("_id", ""))
        return {"scrape": None, "jobsScraped": 0, "kind": "site_crawl"}

    import job_scrape_application.workflows.webhook_workflow as wf_mod

    monkeypatch.setattr(wf_mod, "fetch_pending_firecrawl_webhooks", fetch_pending_firecrawl_webhooks, raising=False)
    monkeypatch.setattr(wf_mod, "collect_firecrawl_job_result", collect_firecrawl_job_result, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"mock-webhook-empty-{uuid.uuid4().hex[:6]}"
        worker = Worker(env.client, task_queue=task_queue, workflows=[ProcessWebhookIngestWorkflow], activities=[fetch_pending_firecrawl_webhooks, collect_firecrawl_job_result])

        async with worker:
            result = await env.client.execute_workflow(
                ProcessWebhookIngestWorkflow.run,
                id=f"wf-mock-webhook-empty-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    assert result.processed == 0
    assert processed_events == []
