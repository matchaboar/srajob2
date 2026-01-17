from __future__ import annotations

import orjson
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_snap_job_detail_R0042515_markdown.json"
)
JOB_URL = "https://careers.snap.com/job?id=R0042515"
FIXED_NOW = datetime(2026, 1, 7, tzinfo=timezone.utc)
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


def _normalize_snap_job() -> dict[str, Any]:
    payload = _load_spidercloud_fixture(FIXTURE_PATH)
    event = payload[0][0]
    markdown = event.get("content", {}).get("markdown", "")

    scraper = _make_scraper()
    normalized = scraper._normalize_job(JOB_URL, markdown, [event], FIXED_NOW_MS, require_keywords=False)
    assert normalized is not None
    return normalized


def test_spidercloud_snap_job_normalizes_core_fields_and_posted_date():
    normalized = _normalize_snap_job()

    assert normalized["title"] == "Staff Technical Program Manager, Infrastructure"
    assert normalized["company"] == "Snap"
    assert normalized["location"] == "Los Angeles, CA"
    assert normalized["remote"] is False

    expected_posted_at = int((FIXED_NOW - timedelta(days=92)).timestamp() * 1000)
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is False


def test_spidercloud_snap_job_location_components_and_salary_range():
    normalized = _normalize_snap_job()
    resolved = _resolve_location_from_dictionary(normalized["location"])

    assert resolved is not None
    assert resolved.get("city") == "Los Angeles"
    assert resolved.get("state") == "California"
    assert resolved.get("country") == "United States"
    assert normalized["remote"] is False

    hints = parse_markdown_hints(normalized.get("description") or "")
    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 209000
    assert comp_range.get("high") == 313000


def test_spidercloud_snap_job_description_strips_junk():
    normalized = _normalize_snap_job()
    description = normalized.get("description") or ""

    assert len(description) > 200
    for junk in (
        "Apply Now",
        "View Openings",
        "Ready to join Team Snap",
        "Life at Snap",
        "R0042515",
    ):
        assert junk not in description
