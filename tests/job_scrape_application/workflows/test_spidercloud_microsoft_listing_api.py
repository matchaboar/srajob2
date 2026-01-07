from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers.microsoft_careers import (  # noqa: E402
    MicrosoftCareersHandler,
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
PAGE_1 = FIXTURE_DIR / "spidercloud_microsoft_api_page_1.json"
PAGE_2 = FIXTURE_DIR / "spidercloud_microsoft_api_page_2.json"
SOURCE_URL = (
    "https://apply.careers.microsoft.com/api/pcsx/search"
    "?domain=microsoft.com&query=software%20engineer&start=0"
)
PAGE_2_SOURCE_URL = (
    "https://apply.careers.microsoft.com/api/pcsx/search"
    "?domain=microsoft.com&query=software%20engineer&start=10"
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


def _load_spidercloud_fixture(path: Path) -> Any:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _extract_positions(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    positions = payload.get("positions")
    if isinstance(positions, list):
        return [pos for pos in positions if isinstance(pos, dict)]
    data = payload.get("data") if isinstance(payload.get("data"), dict) else None
    positions = data.get("positions") if isinstance(data, dict) else None
    if isinstance(positions, list):
        return [pos for pos in positions if isinstance(pos, dict)]
    return []


def test_microsoft_api_fixture_parses_positions():
    scraper = _make_scraper()
    payload = _load_spidercloud_fixture(PAGE_1)
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)

    positions = _extract_positions(parsed)
    assert positions

    count = parsed.get("count")
    assert isinstance(count, int)
    assert count > len(positions)


def test_microsoft_handler_extracts_job_urls_and_pagination():
    scraper = _make_scraper()
    payload = _load_spidercloud_fixture(PAGE_1)
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)

    handler = MicrosoftCareersHandler()
    urls = handler.get_links_from_json(parsed)
    filtered = handler.filter_job_urls(urls)
    assert filtered
    assert any(
        url.startswith("https://apply.careers.microsoft.com/careers/job/")
        for url in filtered
    )

    positions = _extract_positions(parsed)
    page_size = len(positions)
    count = parsed.get("count")
    assert page_size
    assert isinstance(count, int)
    assert count > page_size

    pagination_urls = handler.get_pagination_urls_from_json(parsed, SOURCE_URL)
    assert pagination_urls
    assert any("start=10" in url for url in pagination_urls)
    last_start = (count - 1) // page_size * page_size
    if last_start:
        assert any(f"start={last_start}" in url for url in pagination_urls)


@pytest.mark.asyncio
async def test_store_scrape_enqueues_microsoft_listing_pagination(monkeypatch):
    scraper = _make_scraper()
    payload = _load_spidercloud_fixture(PAGE_2)
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)

    handler = MicrosoftCareersHandler()
    job_urls = handler.get_links_from_json(parsed)
    pagination_urls = handler.get_pagination_urls_from_json(parsed, PAGE_2_SOURCE_URL)
    job_urls = handler.filter_job_urls(job_urls + pagination_urls)
    assert job_urls

    scrape_payload = {
        "sourceUrl": PAGE_2_SOURCE_URL,
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": payload, "job_urls": job_urls},
    }

    calls: list[dict] = []

    async def fake_mutation(name: str, args: dict):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    async def fake_seen(*_args, **_kwargs):
        return []

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)

    await acts.store_scrape(scrape_payload)

    enqueue_calls = [call for call in calls if call["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "store_scrape should enqueue Microsoft URLs"

    urls = enqueue_calls[0]["args"]["urls"]
    assert any(url.startswith("https://apply.careers.microsoft.com/careers/job/") for url in urls)
    assert any("/api/pcsx/search" in url and "start=20" in url for url in urls)

    delays = enqueue_calls[0]["args"].get("delaysMs") or []
    delay_for_listing = None
    for url, delay in zip(urls, delays):
        if "/api/pcsx/search" in url and "start=20" in url:
            delay_for_listing = delay
            break
    assert delay_for_listing is not None and delay_for_listing > 0
