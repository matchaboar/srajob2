from __future__ import annotations

import os
import sys
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

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

    async def fake_convex_mutation(name: str, payload: Dict[str, Any]) -> None:
        if name == "router:enqueueScrapeUrls":
            captured["enqueue"] = payload
        return None

    async def fake_convex_query(_name: str, _payload: Dict[str, Any]) -> list[Any]:
        return []

    async def fake_fetch_seen_urls_for_site(*_args: Any, **_kwargs: Any) -> list[str]:
        return []

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation
    )
    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_query", fake_convex_query
    )
    monkeypatch.setattr(activities, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)

    res = await activities._scrape_spidercloud_greenhouse(scraper, site, [])

    urls = res.get("items", {}).get("job_urls") or []
    assert urls == ["https://boards-api.greenhouse.io/v1/boards/mongodb/jobs/7477065"]
    assert captured["enqueue"]["urls"] == ["https://boards-api.greenhouse.io/v1/boards/mongodb/jobs/7477065"]
    assert all("\\" not in url for url in urls)
