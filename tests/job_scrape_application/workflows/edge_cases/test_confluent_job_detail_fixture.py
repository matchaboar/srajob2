from __future__ import annotations

import orjson
from pathlib import Path
from typing import Any, Dict

import pytest

from job_scrape_application.config import settings
from job_scrape_application.workflows.activities import step as step_module
# Import SpiderCloud batch processor
from job_scrape_application.workflows.workflow.process_spidercloud_job_batch import process_spidercloud_job_batch
from job_scrape_application.workflows.scrapers import spidercloud_scraper
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/"
    "spidercloud_confluent_job_detail_commonmark.json"
)
JOB_URL = "https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1"


class _FakeClient:
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        self.calls.append({"url": url, "params": params, "stream": stream, "content_type": content_type})
        if stream:
            return self._stream_response()
        return self._sync_response()

    async def _stream_response(self):
        yield self.payload

    async def _sync_response(self):
        return self.payload


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


def _load_fixture() -> Dict[str, Any]:
    payload = orjson.loads(FIXTURE.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        payload = payload[0][0]
    if not isinstance(payload, dict):
        raise AssertionError("Expected spidercloud fixture to yield a dict payload")
    return payload


@pytest.mark.asyncio
async def test_confluent_job_detail_fixture_should_normalize_job():
    scraper = _make_scraper()
    payload = _load_fixture()

    result = await scraper._scrape_single_url_sync(  # noqa: SLF001
        _FakeClient(payload),
        JOB_URL,
        {"return_format": ["commonmark"]},
    )

    assert result["normalized"] is not None
    assert "Staff Software Engineer" in (result["normalized"] or {}).get("title", "")


@pytest.mark.asyncio
async def test_process_spidercloud_job_batch_normalizes_confluent_job_detail(reset_dbos, monkeypatch):
    payload = _load_fixture()

    class _FakeAsyncSpider:
        def __init__(self, *args, **kwargs):
            self.payload = payload

        async def __aenter__(self):
            return _FakeClient(self.payload)

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    def fake_record_scrape_url_attempts(entries: list[dict[str, Any]]) -> None:
        """Mock attempt recording."""
        return None

    monkeypatch.setattr(spidercloud_scraper, "AsyncSpider", _FakeAsyncSpider)
    monkeypatch.setattr(settings, "spider_api_key", "key")
    monkeypatch.setattr(step_module, "record_scrape_url_attempts", fake_record_scrape_url_attempts)

    res = await process_spidercloud_job_batch(
        {"urls": [{"url": JOB_URL, "sourceUrl": JOB_URL}]},
        persist_scrapes=False,
    )

    # Verify workflow completed successfully
    assert res.get("provider") == "spidercloud", "Expected spidercloud provider"
    # With persist_scrapes=False, scraping happens but no storage
    # The detailed normalization check is in test_confluent_job_detail_fixture_should_normalize_job
    assert "error" not in res or not res.get("error"), f"Unexpected error: {res.get('error')}"
