"""Test SpiderCloud _fetch_site_api retry logic for empty responses."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest


from job_scrape_application.workflows.scrapers.spidercloud_scraper import (
    SpiderCloudScraper,
)
from job_scrape_application.workflows.site_handlers.ashby import AshbyHqHandler


class MockDeps:
    def mask_secret(self, s: str) -> str:
        return s

    def log_dispatch(self, *args: Any, **kwargs: Any) -> None:
        pass

    def log_sync_response(self, *args: Any, **kwargs: Any) -> None:
        pass

    def build_request_snapshot(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {}

    def trim_scrape_for_convex(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return payload

    async def fetch_seen_urls_for_site(
        self, source_url: str, pattern: Optional[str]
    ) -> List[str]:
        return []


def _make_scraper() -> SpiderCloudScraper:
    scraper = SpiderCloudScraper.__new__(SpiderCloudScraper)
    scraper.deps = MockDeps()
    scraper.provider = "spidercloud"
    return scraper


def _make_valid_ashby_response() -> List[Dict[str, Any]]:
    """Create a valid SpiderCloud response with Ashby API data."""
    return [
        {
            "content": {
                "raw": '<html><pre>{"jobs":[{"id":"abc","title":"Test","jobUrl":"https://jobs.ashbyhq.com/test/abc"}]}</pre></html>'
            },
            "url": "https://api.ashbyhq.com/posting-api/job-board/test",
        }
    ]


def _make_empty_response() -> List[Any]:
    """Create an empty SpiderCloud response."""
    return []


@pytest.mark.asyncio
async def test_fetch_site_api_succeeds_on_first_attempt():
    """Test that _fetch_site_api returns job URLs on first successful attempt."""
    scraper = _make_scraper()
    handler = AshbyHqHandler()

    async def mock_iterate(*args: Any, **kwargs: Any):
        for item in _make_valid_ashby_response():
            yield item

    with patch.object(scraper, "_iterate_scrape_response", mock_iterate):
        with patch.object(scraper, "_api_key", return_value="test-key"):
            with patch("spider.AsyncSpider"):
                result = await scraper._fetch_site_api(
                    handler, "https://jobs.ashbyhq.com/test"
                )

    assert result is not None
    assert "items" in result
    assert "job_urls" in result["items"]
    assert len(result["items"]["job_urls"]) > 0


@pytest.mark.asyncio
async def test_fetch_site_api_retries_on_empty_response():
    """Test that _fetch_site_api retries when SpiderCloud returns empty."""
    scraper = _make_scraper()
    handler = AshbyHqHandler()

    call_count = 0

    async def mock_iterate_with_retry(*args: Any, **kwargs: Any):
        nonlocal call_count
        call_count += 1
        # First two calls return empty, third returns valid
        if call_count < 3:
            for item in _make_empty_response():
                yield item
        else:
            for item in _make_valid_ashby_response():
                yield item

    with patch.object(scraper, "_iterate_scrape_response", mock_iterate_with_retry):
        with patch.object(scraper, "_api_key", return_value="test-key"):
            with patch("spider.AsyncSpider"):
                result = await scraper._fetch_site_api(
                    handler, "https://jobs.ashbyhq.com/test"
                )

    assert call_count == 3  # Should have retried twice
    assert result is not None
    assert "items" in result
    assert "job_urls" in result["items"]


@pytest.mark.asyncio
async def test_fetch_site_api_returns_none_after_all_retries_exhausted():
    """Test that _fetch_site_api returns None after exhausting all retries."""
    scraper = _make_scraper()
    handler = AshbyHqHandler()

    call_count = 0

    async def mock_iterate_always_empty(*args: Any, **kwargs: Any):
        nonlocal call_count
        call_count += 1
        for item in _make_empty_response():
            yield item

    with patch.object(scraper, "_iterate_scrape_response", mock_iterate_always_empty):
        with patch.object(scraper, "_api_key", return_value="test-key"):
            with patch("spider.AsyncSpider"):
                result = await scraper._fetch_site_api(
                    handler, "https://jobs.ashbyhq.com/test"
                )

    assert call_count == 3  # Should have tried 3 times (initial + 2 retries)
    assert result is None


@pytest.mark.asyncio
async def test_fetch_site_api_retries_on_exception():
    """Test that _fetch_site_api retries when SpiderCloud raises an exception."""
    scraper = _make_scraper()
    handler = AshbyHqHandler()

    call_count = 0

    async def mock_iterate_with_exception(*args: Any, **kwargs: Any):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ConnectionError("SpiderCloud connection failed")
        for item in _make_valid_ashby_response():
            yield item

    with patch.object(scraper, "_iterate_scrape_response", mock_iterate_with_exception):
        with patch.object(scraper, "_api_key", return_value="test-key"):
            with patch("spider.AsyncSpider"):
                result = await scraper._fetch_site_api(
                    handler, "https://jobs.ashbyhq.com/test"
                )

    assert call_count == 2  # Should have retried once
    assert result is not None
    assert "items" in result
