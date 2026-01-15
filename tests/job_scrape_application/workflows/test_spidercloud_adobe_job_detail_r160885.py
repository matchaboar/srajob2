from __future__ import annotations

import json
from pathlib import Path
from typing import Any


from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_adobe_job_detail_r160885_raw.json"
)
JOB_URL = "https://careers.adobe.com/us/en/job/R160885/2026-Intern-Research-Scientist"


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


def _extract_event(payload: Any) -> dict[str, Any]:
    if payload and isinstance(payload[0], list) and payload[0]:
        event = payload[0][0]
    elif payload and isinstance(payload[0], dict):
        event = payload[0]
    else:
        raise AssertionError("Unexpected SpiderCloud fixture shape")
    if not isinstance(event, dict):
        raise AssertionError("Expected dict event from SpiderCloud fixture")
    return event


def _normalize_adobe_job() -> dict[str, Any]:
    payload = _load_spidercloud_fixture(FIXTURE_PATH)
    event = _extract_event(payload)
    markdown = event.get("content", {}).get("commonmark", "")
    scraper = _make_scraper()
    normalized = scraper._normalize_job(JOB_URL, markdown, [event], 0, require_keywords=False)
    assert normalized is not None
    return normalized


def test_spidercloud_adobe_r160885_normalizes_fields_and_posted_date():
    normalized = _normalize_adobe_job()

    assert normalized["title"] == "2026 Intern - Research Scientist"
    assert normalized["company"] == "Adobe"
    assert normalized["location"] == "Paris, France"
    assert normalized["remote"] is False

    expected_posted_at = parse_posted_at("2025-12-14")
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is False


def test_spidercloud_adobe_r160885_location_components_and_salary_absent():
    normalized = _normalize_adobe_job()
    resolved = _resolve_location_from_dictionary(normalized["location"])

    assert resolved is not None
    assert resolved.get("city") == "Paris"
    assert resolved.get("state") == "France"
    assert resolved.get("country") == "France"
    assert normalized["remote"] is False

    hints = parse_markdown_hints(normalized.get("description") or "")
    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None
    assert hints.get("compensation") is None


def test_spidercloud_adobe_r160885_description_strips_junk():
    normalized = _normalize_adobe_job()
    description = normalized.get("description") or ""

    assert len(description) > 200
    for junk in (
        "Card text",
        "Widget title goes here",
        "Your engaging subtitle goes here",
        "Meta card 1",
        "Meta card 2",
        "Meta card 3",
        "Your engaging footer subtitle goes here",
        "Stay in the loop.",
        "Join our talent community",
    ):
        assert junk not in description
