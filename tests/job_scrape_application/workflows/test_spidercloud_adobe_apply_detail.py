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


def _load_event(fixture_path: str) -> tuple[dict, str]:
    payload = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")
    return event, markdown


def test_adobe_apply_page_normalization_keeps_apply_url_and_placeholder_title():
    event, markdown = _load_event(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_apply_r162038_commonmark.json"
    )
    scraper = _make_scraper()
    normalized = scraper._normalize_job(
        "https://careers.adobe.com/us/en/apply?jobSeqNo=ADOBUSR162038EXTERNALENUS",
        markdown,
        [event],
        0,
        require_keywords=False,
    )

    assert normalized is not None
    assert normalized["title"] == "]"
    assert "You are applying for" in normalized["description"]
    assert normalized["url"].endswith("apply?jobSeqNo=ADOBUSR162038EXTERNALENUS")


def test_adobe_job_detail_normalization_prefers_job_description_page():
    event, markdown = _load_event(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_job_detail_r162038_commonmark.json"
    )
    scraper = _make_scraper()
    normalized = scraper._normalize_job(
        "https://careers.adobe.com/us/en/job/R162038/Support-Pricing-Specialist",
        markdown,
        [event],
        0,
        require_keywords=False,
    )

    assert normalized is not None
    assert "Support Pricing Specialist" in normalized["title"]
    assert "Our Company" in normalized["description"]
    assert normalized["url"].endswith("/job/R162038/Support-Pricing-Specialist")


def test_adobe_apply_page_commonmark_includes_job_detail_link():
    _, markdown = _load_event(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_apply_r162038_commonmark.json"
    )

    assert "https://careers.adobe.com/us/en/job/R162038/Support-Pricing-Specialist" in markdown
