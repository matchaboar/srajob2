from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.activities import process_spidercloud_job_batch  # noqa: E402
from job_scrape_application.workflows.scrapers import spidercloud_scraper  # noqa: E402
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/"
    "spidercloud_samsara_greenhouse_job_detail_7245557_raw.json"
)
JOB_URL = "https://boards-api.greenhouse.io/v1/boards/samsara/jobs/7245557"


class _FakeClient:
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    async def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        self.calls.append({"url": url, "params": params, "stream": stream, "content_type": content_type})
        yield self.payload


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
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        payload = payload[0][0]
    if not isinstance(payload, dict):
        raise AssertionError("Expected spidercloud fixture to yield a dict payload")
    return payload


@pytest.mark.asyncio
async def test_samsara_greenhouse_job_detail_fixture_should_normalize_job():
    scraper = _make_scraper()
    payload = _load_fixture()

    result = await scraper._scrape_single_url(  # noqa: SLF001
        _FakeClient(payload),
        JOB_URL,
        {"return_format": ["raw_html"]},
    )

    assert result.get("normalized") is not None
    assert "Account Executive" in (result.get("normalized") or {}).get("title", "")


@pytest.mark.asyncio
async def test_process_spidercloud_job_batch_normalizes_samsara_job_detail(monkeypatch):
    payload = _load_fixture()

    class _FakeAsyncSpider:
        def __init__(self, *args, **kwargs):
            self.payload = payload

        async def __aenter__(self):
            return _FakeClient(self.payload)

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    monkeypatch.setattr(spidercloud_scraper, "AsyncSpider", _FakeAsyncSpider)
    monkeypatch.setattr("job_scrape_application.workflows.activities.settings.spider_api_key", "key")

    res = await process_spidercloud_job_batch({"urls": [{"url": JOB_URL, "sourceUrl": JOB_URL}]})

    scrapes = res.get("scrapes") or []
    assert scrapes, "expected spidercloud scrapes to be returned"
    normalized = scrapes[0].get("items", {}).get("normalized") or []
    assert normalized
    assert "Account Executive" in normalized[0].get("title", "")
