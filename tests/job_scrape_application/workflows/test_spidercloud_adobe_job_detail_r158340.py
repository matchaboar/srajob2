from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_job_detail_r158340_commonmark.json"
)
JOB_URL = "https://careers.adobe.com/us/en/job/R158340/Enterprise-Sales-Account-Manager"


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


def _normalize_adobe_job() -> dict[str, Any]:
    payload = _load_spidercloud_fixture(FIXTURE_PATH)
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")
    scraper = _make_scraper()
    normalized = scraper._normalize_job(JOB_URL, markdown, [event], 0, require_keywords=False)
    assert normalized is not None
    return normalized


def test_spidercloud_adobe_r158340_normalizes_fields_and_posted_date():
    normalized = _normalize_adobe_job()

    assert normalized["title"] == "Enterprise Sales Account Manager"
    assert normalized["company"] == "Adobe"
    assert normalized["location"] == "Bangkok, Thailand"
    assert normalized["remote"] is False

    expected_posted_at = int(datetime(2025, 8, 15, tzinfo=timezone.utc).timestamp() * 1000)
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is False


def test_spidercloud_adobe_r158340_location_components_and_salary_absent():
    normalized = _normalize_adobe_job()
    resolved = _resolve_location_from_dictionary(normalized["location"])

    assert resolved is not None
    assert resolved.get("city") == "Bangkok"
    assert resolved.get("state") == "Thailand"
    assert resolved.get("country") == "Thailand"
    assert normalized["remote"] is False

    hints = parse_markdown_hints(normalized.get("description") or "")
    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None
    assert hints.get("compensation") is None


def test_spidercloud_adobe_r158340_description_strips_junk():
    normalized = _normalize_adobe_job()
    description = normalized.get("description") or ""

    assert len(description) > 200
    for junk in (
        "Card text",
        "Cookie Settings",
        "Jobseekers Also Viewed",
        "Profile recommendations",
        "Stay in the loop.",
        "Widget title goes here",
    ):
        assert junk not in description
