from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson

from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    parse_markdown_hints,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_job_detail_r163844_commonmark.json"
)
JOB_URL = "https://careers.adobe.com/us/en/job/R163844/Technical-Support-Engineer"
FIXED_NOW = datetime(2026, 1, 8, tzinfo=timezone.utc)
FIXED_NOW_MS = int(FIXED_NOW.timestamp() * 1000)


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


def _normalize_adobe_job() -> dict[str, Any]:
    payload = _load_spidercloud_fixture(FIXTURE_PATH)
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")
    scraper = _make_scraper()
    normalized = scraper._normalize_job(JOB_URL, markdown, [event], FIXED_NOW_MS, require_keywords=False)
    assert normalized is not None
    return normalized


def test_spidercloud_adobe_r163844_normalizes_fields_and_posted_date():
    normalized = _normalize_adobe_job()

    assert normalized["title"] == "Technical Support Engineer"
    assert normalized["company"] == "Adobe"
    assert normalized["location"] == "Uttar Pradesh, India"
    assert normalized["remote"] is True

    expected_posted_at = int(datetime(2026, 1, 7, tzinfo=timezone.utc).timestamp() * 1000)
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is False


def test_spidercloud_adobe_r163844_location_components_and_salary_absent():
    normalized = _normalize_adobe_job()
    parts = [part.strip() for part in normalized["location"].split(",")]

    assert parts[0] == "Uttar Pradesh"
    assert parts[-1] == "India"
    assert normalized["remote"] is True

    hints = parse_markdown_hints(normalized.get("description") or "")
    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None
    assert hints.get("compensation") is None


def test_spidercloud_adobe_r163844_description_strips_junk():
    normalized = _normalize_adobe_job()
    description = normalized.get("description") or ""

    assert len(description) > 200
    for junk in (
        "Apply now",
        "Cookie Settings",
        "Get notified for similar jobs",
        "Get tailored job",
        "Manage alerts",
        "Profile recommendations",
        "Save job",
        "Similar Jobs",
        "We use cookies",
    ):
        assert junk not in description
