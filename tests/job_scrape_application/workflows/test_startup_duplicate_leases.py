from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import job_scrape_application.workflows.scrape_workflow as sw


pytest.skip("Disabled per request.", allow_module_level=True)


@pytest.mark.asyncio
async def test_first_startup_can_lease_same_site_four_times(monkeypatch):
    """Characterize the first-startup duplicate lease with 4 concurrent workflows."""

    site = {
        "_id": "site-gh",
        "name": "GitHub Careers",
        "url": "https://www.github.careers",
        "type": "general",
        "pattern": None,
        "scrapeProvider": "spidercloud",
    }
    lease_attempts = 0
    leased_urls: List[str] = []
    scraped_urls: List[str] = []
    completed_sites: List[str] = []

    @activity.defn
    async def lease_site(
        worker_id: str,
        lock_seconds: int = 300,
        site_type: str | None = None,  # noqa: ARG001
        scrape_provider: str | None = None,  # noqa: ARG001
    ):
        nonlocal lease_attempts
        lease_attempts += 1
        if lease_attempts <= 4:
            leased_urls.append(site["url"])
            return dict(site)
        return None

    @activity.defn
    async def scrape_site(payload: Dict[str, Any]):
        scraped_urls.append(payload.get("url"))
        return {
            "provider": "mock",
            "sourceUrl": payload.get("url"),
            "items": {
                "normalized": [
                    {
                        "job_title": "Mocked",
                        "url": payload.get("url"),
                    }
                ]
            },
        }

    @activity.defn
    async def store_scrape(scrape: Dict[str, Any]):  # noqa: ARG001
        return f"scrape-{len(scraped_urls)}"

    @activity.defn
    async def complete_site(site_id: str):
        completed_sites.append(site_id)
        return None

    @activity.defn
    async def fail_site(payload: Dict[str, Any]):  # noqa: ARG001
        return None

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):  # noqa: ARG001
        return None

    @activity.defn
    async def record_scratchpad(payload: Dict[str, Any]):  # noqa: ARG001
        return None

    monkeypatch.setattr(sw, "lease_site", lease_site, raising=False)
    monkeypatch.setattr(sw, "scrape_site", scrape_site, raising=False)
    monkeypatch.setattr(sw, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(sw, "complete_site", complete_site, raising=False)
    monkeypatch.setattr(sw, "fail_site", fail_site, raising=False)
    monkeypatch.setattr(sw, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(sw, "record_scratchpad", record_scratchpad, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"lease-dup-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[sw.ScrapeWorkflow],
            activities=[
                lease_site,
                scrape_site,
                store_scrape,
                complete_site,
                fail_site,
                record_workflow_run,
                record_scratchpad,
            ],
        )

        async with worker:
            workflow_prefix = uuid.uuid4().hex[:6]
            await asyncio.wait_for(
                asyncio.gather(
                    *[
                        env.client.execute_workflow(
                            sw.ScrapeWorkflow.run,
                            id=f"wf-dup-{workflow_prefix}-{idx}",
                            task_queue=task_queue,
                        )
                        for idx in range(4)
                    ]
                ),
                timeout=5,
            )

    assert leased_urls == [site["url"]] * 4
    assert scraped_urls == [site["url"]] * 4
    assert completed_sites == ["site-gh"] * 4
