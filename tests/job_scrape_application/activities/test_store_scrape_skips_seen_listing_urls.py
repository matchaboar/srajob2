from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts


@pytest.mark.asyncio
async def test_store_scrape_enqueues_detail_urls_from_listing_payload(monkeypatch: pytest.MonkeyPatch):
    source_url = "https://explore.jobs.netflix.net/careers?query=engineer"
    listing_url = (
        "https://explore.jobs.netflix.net/api/apply/v2/jobs"
        "?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date&start=10&num=10"
    )
    job_url = "https://explore.jobs.netflix.net/careers/job/790313345439"

    mutation_calls: List[Dict[str, Any]] = []
    queue_calls: List[Dict[str, Any]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        mutation_calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        queue_calls.append(payload)
        return {"queued": len(payload.get("urls", []))}

    async def fake_fetch_seen(source: str, pattern: str | None):
        assert source == source_url
        return [listing_url]

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)

    scrape_payload: Dict[str, Any] = {
        "sourceUrl": source_url,
        "pattern": "https://explore.jobs.netflix.net/careers/job/**",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "raw": {
                "job_urls": [listing_url, job_url],
            },
        },
    }

    await acts.store_scrape(scrape_payload)
    await acts.store_scrape(scrape_payload)

    assert len(queue_calls) == 2, "expected listing URLs to be re-enqueued on next schedule"
    first_args = queue_calls[0]
    second_args = queue_calls[1]
    assert first_args["urls"] == [job_url]
    assert second_args["urls"] == [job_url]
    assert first_args["urlTypes"] == ["detail"]
    assert second_args["urlTypes"] == ["detail"]


@pytest.mark.asyncio
async def test_listing_urls_scraped_by_job_details_worker_enqueue_jobs(monkeypatch: pytest.MonkeyPatch):
    """
    Listing URLs are scraped by the SpiderCloud job-details workflow; store_scrape
    then extracts the job description URLs and enqueues them for detail scraping.
    """
    source_url = "https://example.com/jobs?query=engineer"
    listing_url = "https://example.com/api/jobs?start=10&num=10"
    job_url = "https://example.com/jobs/123"

    mutation_calls: List[Dict[str, Any]] = []
    queue_calls: List[Dict[str, Any]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        mutation_calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        queue_calls.append(payload)
        return {"queued": len(payload.get("urls", []))}

    async def fake_fetch_seen(source: str, pattern: str | None):
        assert source == source_url
        return [listing_url]

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)

    scrape_payload: Dict[str, Any] = {
        "sourceUrl": source_url,
        "pattern": "https://example.com/jobs/**",
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {
            "provider": "spidercloud",
            "raw": {
                "job_urls": [listing_url, job_url],
            },
        },
    }

    await acts.store_scrape(scrape_payload)

    assert queue_calls, "expected enqueueScrapeUrls to be called"
    assert queue_calls[0]["urls"] == [job_url]
    assert queue_calls[0]["urlTypes"] == ["detail"]
