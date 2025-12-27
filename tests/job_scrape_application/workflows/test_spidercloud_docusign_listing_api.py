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
from job_scrape_application.workflows.site_handlers.docusign import (  # noqa: E402
    DocusignHandler,
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


def test_docusign_handler_extracts_job_urls():
    scraper = _make_scraper()
    fixture = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_docusign_api_page_1.json"
    )
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)

    handler = DocusignHandler()
    urls = handler.get_links_from_json(parsed)
    filtered = handler.filter_job_urls(urls)

    assert filtered
    assert "https://careers.docusign.com/jobs/27215?lang=en-us" in filtered


def test_docusign_handler_builds_pagination_urls():
    scraper = _make_scraper()
    fixture = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_docusign_api_page_1.json"
    )
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)

    handler = DocusignHandler()
    request_url = (
        "https://careers.docusign.com/api/jobs"
        "?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations"
        "&page=1"
        "&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States"
        "&sortBy=relevance&descending=false&internal=false"
    )
    pagination_urls = handler.get_pagination_urls_from_json(parsed, request_url)
    assert pagination_urls
    assert any("page=2" in url for url in pagination_urls)
    assert any("page=4" in url for url in pagination_urls)
