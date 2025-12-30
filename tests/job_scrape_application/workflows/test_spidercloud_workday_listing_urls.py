from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers import get_site_handler  # noqa: E402

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_dataminr_workday_listing.json"
)
SOURCE_URL = "https://dataminr.wd12.myworkdayjobs.com/en-US/Dataminr?q=engineer"


def _load_fixture() -> Any:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _extract_first_event(payload: Any) -> Dict[str, Any] | None:
    if isinstance(payload, list) and payload and isinstance(payload[0], list) and payload[0]:
        item = payload[0][0]
        if isinstance(item, dict):
            return item
    return None


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


def test_spidercloud_workday_listing_extracts_job_urls_from_events():
    payload = _load_fixture()
    event = _extract_first_event(payload)
    assert event is not None, "expected a Workday listing event payload"

    handler = get_site_handler(SOURCE_URL)
    assert handler is not None and handler.name == "workday"

    scraper = _make_scraper()
    urls = scraper._extract_listing_job_urls_from_events(  # noqa: SLF001
        handler,
        [event],
        "",
        base_url=SOURCE_URL,
    )
    assert urls, "expected job URLs extracted from Workday listing HTML"
    assert any("JR1652" in url or "Software-Engineer" in url for url in urls)
