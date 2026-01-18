from __future__ import annotations

import asyncio
from typing import Any, Dict

from job_scrape_application.workflows.workflow.process_spidercloud_job_batch import (
    process_spidercloud_job_batch,
)
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


def test_process_batch_rewrites_greenhouse_detail_urls(monkeypatch):
    # Mock record_scrape_url_attempts to avoid real Convex calls
    def mock_record_attempts(entries):
        pass

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.record_scrape_url_attempts.record_scrape_url_attempts",
        mock_record_attempts,
    )
    # Mock filter_new_job_urls to return all URLs as new
    def mock_filter_new(urls):
        return urls

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.filter_new_job_urls.filter_new_job_urls",
        mock_filter_new,
    )

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
        "job_scrape_application.workflows.activities.factories._make_spidercloud_scraper",
        lambda: scraper,
    )

    # Act
    res = asyncio.run(process_spidercloud_job_batch(batch, persist_scrapes=False))

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

    # And the normalized payload should expose a marketing apply link when present
    normalized = scrapes[0].get("items", {}).get("normalized") or []
    assert normalized
    assert normalized[0].get("apply_url") == "https://boards.greenhouse.io/coreweave/jobs/4607747006"


def test_process_batch_leaves_non_greenhouse_urls(monkeypatch):
    # Mock record_scrape_url_attempts to avoid real Convex calls
    def mock_record_attempts(entries):
        pass

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.record_scrape_url_attempts.record_scrape_url_attempts",
        mock_record_attempts,
    )
    # Mock filter_new_job_urls to return all URLs as new
    def mock_filter_new(urls):
        return urls

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.filter_new_job_urls.filter_new_job_urls",
        mock_filter_new,
    )

    url = "https://example.com/job/123"
    batch = {"urls": [{"url": url}]}

    scraper = _make_scraper()

    async def fake_scrape(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"scrape": {"items": {"normalized": [{"url": url}]}}}

    monkeypatch.setattr(scraper, "scrape_greenhouse_jobs", fake_scrape)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.factories._make_spidercloud_scraper",
        lambda: scraper,
    )

    res = asyncio.run(process_spidercloud_job_batch(batch, persist_scrapes=False))
    scrapes = res.get("scrapes")
    assert isinstance(scrapes, list) and scrapes
    assert scrapes[0]["subUrls"] == [url]


def test_process_batch_does_not_rewrite_ashby_urls(monkeypatch):
    # Mock record_scrape_url_attempts to avoid real Convex calls
    def mock_record_attempts(entries):
        pass

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.record_scrape_url_attempts.record_scrape_url_attempts",
        mock_record_attempts,
    )
    # Mock filter_new_job_urls to return all URLs as new
    def mock_filter_new(urls):
        return urls

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.filter_new_job_urls.filter_new_job_urls",
        mock_filter_new,
    )

    url = "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938"
    batch = {"urls": [{"url": url, "sourceUrl": "https://jobs.ashbyhq.com/lambda"}]}

    scraper = _make_scraper()
    captured: Dict[str, Any] = {}

    async def fake_scrape(payload: Dict[str, Any]) -> Dict[str, Any]:
        captured.update(payload)
        return {"scrape": {"items": {"normalized": [{"url": payload["urls"][0]}]}}}

    monkeypatch.setattr(scraper, "scrape_greenhouse_jobs", fake_scrape)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.factories._make_spidercloud_scraper",
        lambda: scraper,
    )

    res = asyncio.run(process_spidercloud_job_batch(batch, persist_scrapes=False))
    assert captured["urls"] == [url]
    scrapes = res.get("scrapes")
    assert isinstance(scrapes, list) and scrapes
    assert scrapes[0]["subUrls"] == [url]
