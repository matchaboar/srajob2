from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SPIDERCLOUD_BATCH_SIZE,
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


class _FakeClient:
    def __init__(self, payloads: List[Any]) -> None:
        self.payloads = payloads
        self.calls: List[Dict[str, Any]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    async def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        # Record params for assertions; emit payloads once.
        self.calls.append({"url": url, "params": params, "stream": stream, "content_type": content_type})
        for payload in self.payloads:
            yield payload


@pytest.mark.asyncio
async def test_batch_params_use_raw_for_greenhouse_api(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"html": "<h1>Software Engineer</h1>"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    await scraper._scrape_urls_batch(
        ["https://boards-api.greenhouse.io/v1/boards/demo/jobs/1"],
        source_url="https://boards-api.greenhouse.io/v1/boards/demo/jobs/1",
    )

    call = fake_client.calls[0]
    assert "raw_html" in call["params"]["return_format"]
    assert "commonmark" not in call["params"]["return_format"]
    assert call["params"]["request"] == "chrome"
    assert call["params"]["preserve_host"] is False


@pytest.mark.asyncio
async def test_batch_params_use_commonmark_for_non_api(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"commonmark": "### hi"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    await scraper._scrape_urls_batch(["https://example.com/job"], source_url="https://example.com/job")

    call = fake_client.calls[0]
    assert call["params"]["return_format"] == ["commonmark"]
    assert call["params"]["request"] == "smart"
    assert call["params"]["preserve_host"] is True


@pytest.mark.asyncio
async def test_scrape_single_url_sets_raw_format_for_api(monkeypatch):
    scraper = _make_scraper()
    payload = {"raw_html": "<h1>Software Engineer</h1><p>Hello</p>"}
    fake_client = _FakeClient([payload])

    async def _fake_scrape_url(url: str, params: Dict[str, Any], stream: bool, content_type: str):
        fake_client.calls.append({"params": params})
        yield payload

    fake_client.scrape_url = _fake_scrape_url  # type: ignore[assignment]
    result = await scraper._scrape_single_url(
        fake_client,
        "https://boards-api.greenhouse.io/v1/boards/demo/jobs/1",
        {"return_format": ["commonmark"]},
    )

    assert any("raw_html" in c["params"]["return_format"] for c in fake_client.calls)
    assert result["normalized"]["description"]


@pytest.mark.asyncio
async def test_scrape_single_url_keeps_commonmark_for_non_api():
    scraper = _make_scraper()
    payload = {"commonmark": "### Senior Software Engineer\nBody"}
    fake_client = _FakeClient([payload])
    result = await scraper._scrape_single_url(
        fake_client,
        "https://example.com/job",
        {"return_format": ["commonmark"]},
    )

    assert "Senior Software Engineer" in result["normalized"]["description"]


def test_extract_markdown_handles_raw_html_key():
    scraper = _make_scraper()
    html_payload = {"raw_html": "<p>Hi</p>"}
    text = scraper._extract_markdown(html_payload)
    assert text == "Hi"


def test_normalize_job_handles_api_json_string():
    scraper = _make_scraper()
    json_body = json.dumps({"title": "Software Engineer", "content": "<p>Role</p>"})
    normalized = scraper._normalize_job("https://boards-api.greenhouse.io/v1/boards/demo/jobs/1", json_body, [], 0)
    assert normalized is not None
    assert normalized["title"] == "Software Engineer"
    assert "Role" in normalized["description"]


def test_normalize_job_handles_api_json_events():
    scraper = _make_scraper()
    events = [{"title": "Senior Software Engineer"}]
    normalized = scraper._normalize_job(
        "https://boards-api.greenhouse.io/v1/boards/demo/jobs/1",
        '{"content": "<p>content</p>"}',
        events,
        0,
    )
    assert normalized is not None
    assert normalized["title"] == "Senior Software Engineer"


@pytest.mark.asyncio
async def test_batch_truncates_over_batch_size(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)
    async def _fake_single_url(*_args, **_kwargs):
        return {"normalized": {"url": "u"}}

    monkeypatch.setattr(scraper, "_scrape_single_url", _fake_single_url)

    urls = [f"https://example.com/{i}" for i in range(SPIDERCLOUD_BATCH_SIZE + 5)]
    payload = await scraper._scrape_urls_batch(urls, source_url="https://example.com")
    assert len(payload["items"]["seedUrls"]) == SPIDERCLOUD_BATCH_SIZE


@pytest.mark.asyncio
async def test_raw_html_description_is_used(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"raw_html": "<h1>Software Engineer</h1><p>Body</p>"}])
    result = await scraper._scrape_single_url(fake_client, "https://example.com", {"return_format": ["commonmark"]})
    assert "Body" in result["normalized"]["description"]
