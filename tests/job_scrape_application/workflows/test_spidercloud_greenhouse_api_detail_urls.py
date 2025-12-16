from __future__ import annotations

from typing import Any, Dict

import pytest
from job_scrape_application.workflows.activities import process_spidercloud_job_batch
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (
    SpiderCloudScraper,
    SpidercloudDependencies,
)


def _make_scraper() -> SpiderCloudScraper:
    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=lambda *_args, **_kwargs: [],
    )
    return SpiderCloudScraper(deps)


@pytest.mark.asyncio
async def test_process_batch_rewrites_greenhouse_detail_urls(monkeypatch):
    # Arrange: batch entries pointing at marketing site with gh_jid + board params
    batch = {
        "urls": [
            {
                "url": "https://coreweave.com/careers/job?4607747006&board=coreweave&gh_jid=4607747006",
                "sourceUrl": "https://api.greenhouse.io/v1/boards/coreweave/jobs",
            }
        ]
    }

    # Fake scraper to capture URLs passed to scrape_greenhouse_jobs
    captured: Dict[str, Any] = {}

    async def fake_scrape(payload: Dict[str, Any]) -> Dict[str, Any]:
        captured.update(payload)
        return {
            "scrape": {
                "items": {
                    "normalized": [{"url": payload["urls"][0]}],
                    "raw": [{"url": payload["urls"][0]}],
                }
            }
        }

    scraper = _make_scraper()
    monkeypatch.setattr(scraper, "scrape_greenhouse_jobs", fake_scrape)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities._make_spidercloud_scraper",
        lambda: scraper,
    )

    # Act
    res = await process_spidercloud_job_batch(batch)

    # Assert: upstream scrape call received API URL
    assert captured["urls"] == [
        "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4607747006"
    ]
    # And returned scrapes also contain API URL
    scrapes = res.get("scrapes")
    assert isinstance(scrapes, list) and scrapes
    assert scrapes[0]["subUrls"] == [
        "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4607747006"
    ]


@pytest.mark.asyncio
async def test_process_batch_leaves_non_greenhouse_urls(monkeypatch):
    url = "https://example.com/job/123"
    batch = {"urls": [{"url": url}]}

    scraper = _make_scraper()

    async def fake_scrape(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"scrape": {"items": {"normalized": [{"url": url}]}}}

    monkeypatch.setattr(scraper, "scrape_greenhouse_jobs", fake_scrape)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities._make_spidercloud_scraper",
        lambda: scraper,
    )

    res = await process_spidercloud_job_batch(batch)
    scrapes = res.get("scrapes")
    assert isinstance(scrapes, list) and scrapes
    assert scrapes[0]["subUrls"] == [url]
