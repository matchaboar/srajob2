from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)


def _make_scraper() -> SpiderCloudScraper:
    async def _fetch_seen_urls_for_site(*_args, **_kwargs) -> list[str]:
        return []

    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=_fetch_seen_urls_for_site,
    )
    return SpiderCloudScraper(deps)


def test_try_parse_json_handles_multiple_objects() -> None:
    scraper = _make_scraper()
    raw = '{"foo": 1} {"bar": 2}'

    parsed = scraper._try_parse_json(raw)

    assert parsed == [{"foo": 1}, {"bar": 2}]


def test_try_parse_json_handles_unicode_escaped_payload() -> None:
    scraper = _make_scraper()
    raw = r"{\"@type\":\"JobPosting\",\"title\":\"Engineer\"}"

    parsed = scraper._try_parse_json(raw)

    assert isinstance(parsed, dict)
    assert parsed["title"] == "Engineer"
