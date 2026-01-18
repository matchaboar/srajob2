from __future__ import annotations

import orjson
from pathlib import Path
from typing import Any, Dict, List

import pytest


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
PAGE_3 = FIXTURE_DIR / "spidercloud_microsoft_api_page_3.json"
SOURCE_URL = (
    "https://apply.careers.microsoft.com/api/pcsx/search"
    "?domain=microsoft.com&query=software%20engineer&start=0"
)
PAGE_2_SOURCE_URL = (
    "https://apply.careers.microsoft.com/api/pcsx/search"
    "?domain=microsoft.com&query=software%20engineer&start=10"
)
PAGE_3_SOURCE_URL = (
    "https://apply.careers.microsoft.com/api/pcsx/search"
    "?domain=microsoft.com&query=software%20engineer&start=20"
)

PAGE_FIXTURES = (
    (PAGE_1, SOURCE_URL),
    (PAGE_2, PAGE_2_SOURCE_URL),
    (PAGE_3, PAGE_3_SOURCE_URL),
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
    payload = orjson.loads(path.read_text(encoding="utf-8"))
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


@pytest.mark.parametrize("fixture_path,_source_url", PAGE_FIXTURES)
def test_microsoft_api_fixture_parses_positions(fixture_path: Path, _source_url: str):
    scraper = _make_scraper()
    payload = _load_spidercloud_fixture(fixture_path)
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)

    positions = _extract_positions(parsed)
    assert positions

    count = parsed.get("count")
    assert isinstance(count, int)
    assert count > len(positions)


@pytest.mark.parametrize("fixture_path,source_url", PAGE_FIXTURES)
def test_microsoft_handler_extracts_job_urls_and_pagination(
    fixture_path: Path,
    source_url: str,
):
    scraper = _make_scraper()
    payload = _load_spidercloud_fixture(fixture_path)
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

    pagination_urls = handler.get_pagination_urls_from_json(parsed, source_url)
    assert pagination_urls
    expected_next_start = (handler._extract_start_param(source_url) or 0) + page_size
    assert any(f"start={expected_next_start}" in url for url in pagination_urls)
    last_start = (count - 1) // page_size * page_size
    if last_start:
        assert any(f"start={last_start}" in url for url in pagination_urls)
