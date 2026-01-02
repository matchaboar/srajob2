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


def _load_spidercloud_fixture(path: Path) -> Any:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _extract_first_job_url(payload: Dict[str, Any]) -> str:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise AssertionError("expected jobs list in payload")
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_data = job.get("data") if isinstance(job.get("data"), dict) else job
        if not isinstance(job_data, dict):
            continue
        meta = job_data.get("meta_data")
        if isinstance(meta, dict):
            canonical = meta.get("canonical_url")
            if isinstance(canonical, str) and canonical.strip():
                return canonical.strip()
        for key in ("canonical_url", "jobUrl", "postingUrl", "url"):
            value = job_data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    raise AssertionError("no job url found in payload")


def test_docusign_handler_extracts_job_urls():
    scraper = _make_scraper()
    fixture = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_docusign_api_page_1.json"
    )
    payload = _load_spidercloud_fixture(fixture)
    parsed = scraper._extract_json_payload(payload)
    assert isinstance(parsed, dict)
    expected_url = _extract_first_job_url(parsed)

    handler = DocusignHandler()
    urls = handler.get_links_from_json(parsed)
    filtered = handler.filter_job_urls(urls)

    assert filtered
    assert expected_url in filtered


def test_docusign_handler_builds_pagination_urls():
    scraper = _make_scraper()
    fixture = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_docusign_api_page_1.json"
    )
    payload = _load_spidercloud_fixture(fixture)
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
