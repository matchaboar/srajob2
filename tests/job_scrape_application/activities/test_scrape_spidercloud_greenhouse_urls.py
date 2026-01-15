from __future__ import annotations

from typing import Any, Dict

import pytest


from job_scrape_application.workflows import activities


class _StubScraper:
    provider = "spidercloud"

    async def fetch_greenhouse_listing(self, site: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "job_urls": [
                "https://boards-api.greenhouse.io/v1/boards/mongodb/jobs/7477065////\\\\",
                "https://boards-api.greenhouse.io/v1/boards/mongodb/jobs/7477065\\\\\\",
            ]
        }


@pytest.mark.asyncio
async def test_scrape_spidercloud_greenhouse_normalizes_listing_urls(monkeypatch):
    scraper = _StubScraper()
    site = {
        "_id": "s-gh-1",
        "url": "https://api.greenhouse.io/v1/boards/mongodb/jobs",
        "type": "greenhouse",
    }
    captured: Dict[str, Any] = {}

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        captured["enqueue"] = payload
        return {"queued": len(payload.get("urls", []))}

    async def fake_fetch_seen_urls_for_site(*_args: Any, **_kwargs: Any) -> list[str]:
        return []

    async def fake_filter_new_job_urls(urls: list[str]) -> list[str]:
        return urls  # All URLs are "new"

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(activities, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)
    monkeypatch.setattr(activities, "filter_new_job_urls", fake_filter_new_job_urls)

    res = await activities._scrape_spidercloud_greenhouse(scraper, site, [])

    urls = res.get("items", {}).get("job_urls") or []
    assert urls == ["https://boards-api.greenhouse.io/v1/boards/mongodb/jobs/7477065"]
    assert captured["enqueue"]["urls"] == ["https://boards-api.greenhouse.io/v1/boards/mongodb/jobs/7477065"]
    assert all("\\" not in url for url in urls)
