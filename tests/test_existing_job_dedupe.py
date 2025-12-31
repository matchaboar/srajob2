import asyncio
import time

import job_scrape_application.workflows.activities as activities
from job_scrape_application.services import convex_client


def test_greenhouse_listing_skips_existing_jobs_when_seen_urls_empty(monkeypatch):
    existing_url = "https://boards.greenhouse.io/acme/jobs/123"
    new_url = "https://boards.greenhouse.io/acme/jobs/456"
    now_ms = int(time.time() * 1000)

    class FakeScraper:
        provider = "spidercloud"

        async def fetch_greenhouse_listing(self, site):
            return {"job_urls": [existing_url, new_url]}

    async def fake_fetch_seen_urls_for_site(source_url, pattern):
        return []

    async def fake_filter_existing_job_urls(urls):
        return [existing_url]

    enqueued_payloads = []

    async def fake_convex_mutation(name, args):
        if name == "router:enqueueScrapeUrls":
            enqueued_payloads.append(args)
            return {"queued": args.get("urls", [])}
        if name == "router:completeScrapeUrls":
            return {"updated": len(args.get("urls", []))}
        raise AssertionError(f"unexpected mutation {name}")

    async def fake_convex_query(name, args):
        if name == "router:listQueuedScrapeUrls":
            return [
                {"url": existing_url, "createdAt": now_ms, "status": "pending"},
                {"url": new_url, "createdAt": now_ms, "status": "pending"},
            ]
        raise AssertionError(f"unexpected query {name}")

    monkeypatch.setattr(activities, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)
    monkeypatch.setattr(activities, "filter_existing_job_urls", fake_filter_existing_job_urls)
    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)
    monkeypatch.setattr(convex_client, "convex_query", fake_convex_query)

    site = {
        "_id": "a" * 26,
        "url": "https://boards.greenhouse.io/acme",
        "type": "greenhouse",
    }

    result = asyncio.run(
        activities._scrape_spidercloud_greenhouse(FakeScraper(), site, [])
    )

    assert existing_url in result["items"]["existing"]
    assert new_url not in result["items"]["existing"]
    assert result["items"]["queuedCount"] == 1
    assert enqueued_payloads[0]["urls"] == [new_url]


def test_filter_existing_job_urls_returns_only_known_job_urls(monkeypatch):
    urls = [
        "https://boards.greenhouse.io/acme/jobs/123",
        "https://boards.greenhouse.io/acme/jobs/456",
    ]
    captured: dict[str, object] = {}

    async def fake_convex_query(name, args):
        captured["name"] = name
        captured["args"] = args
        return {"existing": [urls[0], None, 123]}

    monkeypatch.setattr(convex_client, "convex_query", fake_convex_query)

    result = asyncio.run(activities.filter_existing_job_urls(urls))

    assert result == [urls[0]]
    assert captured["name"] == "router:findExistingJobUrls"
    assert captured["args"] == {"urls": urls}


def test_filter_existing_job_urls_ignores_malformed_payloads(monkeypatch):
    async def fake_convex_query(_name, _args):
        return {"existing": "not-a-list"}

    monkeypatch.setattr(convex_client, "convex_query", fake_convex_query)

    result = asyncio.run(
        activities.filter_existing_job_urls(["https://boards.greenhouse.io/acme/jobs/789"])
    )

    assert result == []


def test_filter_existing_job_urls_returns_empty_on_error(monkeypatch):
    async def fake_convex_query(_name, _args):
        raise RuntimeError("boom")

    monkeypatch.setattr(convex_client, "convex_query", fake_convex_query)

    result = asyncio.run(
        activities.filter_existing_job_urls(["https://boards.greenhouse.io/acme/jobs/000"])
    )

    assert result == []
