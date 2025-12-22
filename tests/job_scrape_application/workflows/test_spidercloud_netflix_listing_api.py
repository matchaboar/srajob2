from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers.netflix import NetflixHandler  # noqa: E402


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


def test_spidercloud_extract_json_payload_supports_netflix_positions():
    scraper = _make_scraper()
    fixture = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_api_page_1.json"
    )
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)
    positions = parsed.get("positions")
    assert isinstance(positions, list)
    assert positions


def test_netflix_api_payload_generates_pagination_urls():
    scraper = _make_scraper()
    fixture = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_api_page_3.json"
    )
    payload_raw = json.loads(fixture.read_text(encoding="utf-8"))
    parsed = scraper._extract_json_payload(payload_raw)
    assert isinstance(parsed, dict)

    handler = NetflixHandler()
    request_url = (
        "https://explore.jobs.netflix.net/api/apply/v2/jobs"
        "?query=engineer&Region=ucan&domain=netflix.com&start=0&num=10"
    )
    pagination_urls = handler.get_pagination_urls_from_json(parsed, request_url)

    positions = parsed.get("positions") if isinstance(parsed, dict) else None
    page_size = len(positions) if isinstance(positions, list) else 0
    count = parsed.get("count") if isinstance(parsed, dict) else None
    assert page_size
    assert isinstance(count, int) and count > page_size
    assert pagination_urls
    assert any(f"start={page_size}" in url for url in pagination_urls)
    last_start = (count - 1) // page_size * page_size
    if last_start != 0:
        assert any(f"start={last_start}" in url for url in pagination_urls)


def test_netflix_handler_reads_positions_from_pre_payload():
    fixture = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_api_pre_recaptcha.html"
    )
    html_text = fixture.read_text(encoding="utf-8")
    handler = NetflixHandler()
    urls = handler.get_links_from_raw_html(html_text)
    assert "https://explore.jobs.netflix.net/careers/job/123" in urls
