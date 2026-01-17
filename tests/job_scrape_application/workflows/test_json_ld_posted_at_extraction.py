from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest


from job_scrape_application.workflows.helpers.regex_patterns import (  # noqa: E402
    JSON_LD_SCRIPT_PATTERN,
)
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers.base import (  # noqa: E402
    BaseSiteHandler,
)

JSON_LD_SCRIPT_RE = re.compile(JSON_LD_SCRIPT_PATTERN, re.IGNORECASE | re.DOTALL)

JSON_LD_FIXTURES = [
    Path("tests/job_scrape_application/workflows/fixtures/broadcom_workday_r024197_raw.json"),
    Path("tests/fixtures/ashby_lambda_job_detail_raw.html"),
    Path("tests/job_scrape_application/workflows/fixtures/github_careers_job_4554_raw.html"),
    Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_paloalto_networks_job_detail_raw_html.json"
    ),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_github_careers_job_4648_raw.json"),
    Path("tests/job_scrape_application/workflows/fixtures/adobe_refine_search_page_2.json"),
    Path(
        "tests/job_scrape_application/workflows/fixtures/"
        "spidercloud_paloalto_networks_job_detail_tools_platforms_raw_html.json"
    ),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_ashby_ramp_job_detail_raw_html.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_search_page_3.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_github_jibe_job_4691.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_search_page_3.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_search_page_2.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_github_jibe_job_4771_raw.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_github_careers_job_4554_raw.json"),
    Path("tests/job_scrape_application/workflows/fixtures/adobe_refine_search_page_1.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_github_careers_job_4793_raw.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_search_page_1.json"),
    Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_meta_job_detail_1394915781774041_raw.html.json"
    ),
    Path("tests/job_scrape_application/workflows/fixtures/coupang_job_7486748_raw.html"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_search_page_2.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_job_detail_790313323421_raw_html.json"),
    Path("tests/job_scrape_application/workflows/fixtures/adobe_refine_search_page_3.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_listing_page.json"),
    Path("tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_search_page_1.json"),
    Path(
        "tests/job_scrape_application/workflows/fixtures/"
        "spidercloud_cisco_job_detail_commonmark_raw_html.json"
    ),
]


class _DummyHandler(BaseSiteHandler):
    @classmethod
    def matches_url(cls, url: str) -> bool:  # pragma: no cover - test helper
        return True


def _extract_raw_html(payload: Any) -> str | None:
    if isinstance(payload, str):
        lowered = payload.lower()
        if "<script" in lowered and "ld+json" in lowered:
            return payload
        if "<html" in lowered:
            return payload
        return None
    if isinstance(payload, dict):
        for key in ("raw_html", "raw", "html"):
            value = payload.get(key)
            if isinstance(value, str):
                lowered = value.lower()
                if "<script" in lowered and "ld+json" in lowered:
                    return value
                if "<html" in lowered:
                    return value
        for value in payload.values():
            found = _extract_raw_html(value)
            if found:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _extract_raw_html(item)
            if found:
                return found
    return None


def _load_raw_html(path: Path) -> str:
    if path.suffix == ".html":
        return path.read_text(encoding="utf-8")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    raw_html = _extract_raw_html(payload)
    assert raw_html is not None, f"expected raw HTML with JSON-LD in {path}"
    return raw_html


def _find_date(node: Any) -> str | None:
    if isinstance(node, dict):
        for key in ("datePosted", "dateCreated", "dateModified", "validThrough"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in node.values():
            found = _find_date(value)
            if found:
                return found
    if isinstance(node, list):
        for item in node:
            found = _find_date(item)
            if found:
                return found
    return None


def _extract_expected_date(raw_html: str) -> str | None:
    for match in JSON_LD_SCRIPT_RE.finditer(raw_html):
        payload_raw = match.group("payload").strip()
        if not payload_raw:
            continue
        try:
            parsed = json.loads(payload_raw)
        except Exception:
            continue
        expected = _find_date(parsed)
        if expected:
            return expected
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


@pytest.mark.parametrize("fixture_path", JSON_LD_FIXTURES)
def test_json_ld_fixture_posted_at_extraction(fixture_path: Path) -> None:
    raw_html = _load_raw_html(fixture_path)
    expected = _extract_expected_date(raw_html)
    handler = _DummyHandler()
    extracted = handler.extract_posted_at(raw_html)
    assert extracted == expected


def test_json_ld_stale_date_falls_back_to_markdown() -> None:
    now_ms = int(datetime(2026, 1, 8, tzinfo=timezone.utc).timestamp() * 1000)
    raw_html = (
        "<html><head><script type=\"application/ld+json\">"
        "{\"@type\":\"JobPosting\",\"datePosted\":\"2023-01-01\"}"
        "</script></head><body>Test</body></html>"
    )
    markdown = "Posted 2 days ago"
    url = "https://www.github.careers/careers-home/jobs/test?lang=en-us"
    event = {"content": {"raw": raw_html}}

    scraper = _make_scraper()
    normalized = scraper._normalize_job(url, markdown, [event], now_ms, require_keywords=False)
    assert normalized is not None

    expected_posted_at, expected_unknown = parse_posted_at_with_unknown(markdown, now_ms=now_ms)
    assert expected_unknown is False
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is False
