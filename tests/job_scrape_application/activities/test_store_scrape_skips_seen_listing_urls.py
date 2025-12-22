from __future__ import annotations

from typing import Any, Dict, List

import pytest

from job_scrape_application.workflows import activities as acts


@pytest.mark.asyncio
async def test_store_scrape_skips_seen_listing_urls(monkeypatch: pytest.MonkeyPatch):
    source_url = "https://explore.jobs.netflix.net/careers?query=engineer"
    listing_url = (
        "https://explore.jobs.netflix.net/api/apply/v2/jobs"
        "?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date&start=10&num=10"
    )
    job_url = "https://explore.jobs.netflix.net/careers/job/790313345439"

    mutation_calls: List[Dict[str, Any]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        mutation_calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    async def fake_fetch_seen(source: str, pattern: str | None):
        assert source == source_url
        return [listing_url]

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)
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

    enqueue_calls = [c for c in mutation_calls if c["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "expected enqueueScrapeUrls to be called"
    assert enqueue_calls[0]["args"]["urls"] == [job_url]
