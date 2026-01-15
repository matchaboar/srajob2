from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_snap_job_r0043639_commonmark.json"
)
JOB_URL = "https://careers.snap.com/job?id=R0043639"


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


def _load_event() -> dict:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    return payload[0][0]


def _normalize_job(*, started_at: int = 0) -> dict:
    event = _load_event()
    markdown = event.get("content", {}).get("commonmark", "")
    scraper = _make_scraper()
    normalized = scraper._normalize_job(
        JOB_URL,
        markdown,
        [event],
        started_at,
        require_keywords=False,
    )
    assert normalized is not None, "expected normalized job payload"
    return normalized


def test_snap_job_detail_normalizes_core_fields_and_strips_marketing():
    normalized = _normalize_job()

    assert normalized["title"] == "HR Business Partner"
    assert normalized["company"] == "Snap"
    assert normalized["location"] == "Los Angeles, CA"
    assert normalized["remote"] is False

    description = normalized["description"]
    assert "Life at Snap" not in description
    assert "Ready to join Team Snap" not in description
    assert "Apply Now" not in description
    assert "Knowledge, Skills" in description


def test_snap_job_detail_compensation_range_and_location_fields():
    normalized = _normalize_job()
    hints = parse_markdown_hints(normalized["description"])
    assert hints.get("compensation_range") == {"low": 130000, "high": 196000}

    resolved = _resolve_location_from_dictionary(normalized["location"])
    assert resolved is not None
    assert resolved.get("city") == "Los Angeles"
    assert resolved.get("state") == "California"
    assert resolved.get("country") == "United States"


def test_snap_job_detail_posted_at_falls_back_to_scrape_time():
    started_at = 1700000000000
    normalized = _normalize_job(started_at=started_at)
    expected_posted_at, expected_unknown = parse_posted_at_with_unknown(
        "Posted 23 days ago",
        now_ms=started_at,
    )
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is expected_unknown
