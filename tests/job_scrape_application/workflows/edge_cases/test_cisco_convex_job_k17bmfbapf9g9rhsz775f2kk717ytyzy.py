from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

CONVEX_FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k17bmfbapf9g9rhsz775f2kk717ytyzy.json"
)
SPIDERCLOUD_FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_job_1451067_commonmark.json"
)
JOB_URL = "https://careers.cisco.com/global/en/job/1451067/Indirect-Tax-Analyst-EMEA-Hybrid"


def _load_convex_fixture() -> dict:
    return json.loads(CONVEX_FIXTURE.read_text(encoding="utf-8"))


def _load_spidercloud_fixture() -> list:
    payload = json.loads(SPIDERCLOUD_FIXTURE.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    if not isinstance(payload, list):
        raise AssertionError("Expected list response from SpiderCloud fixture")
    return payload


def _extract_event(payload: list) -> dict:
    if payload and isinstance(payload[0], list) and payload[0]:
        event = payload[0][0]
    elif payload and isinstance(payload[0], dict):
        event = payload[0]
    else:
        raise AssertionError("Unexpected SpiderCloud fixture shape")
    if not isinstance(event, dict):
        raise AssertionError("Expected dict event from SpiderCloud fixture")
    return event


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


def _normalize_job() -> dict:
    payload = _load_spidercloud_fixture()
    event = _extract_event(payload)
    markdown = event.get("content", {}).get("commonmark", "")
    started_at = int(_load_convex_fixture().get("postedAt") or 0)

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


def test_cisco_convex_job_normalizes_core_fields():
    normalized = _normalize_job()

    assert normalized["title"] == "Indirect Tax Analyst EMEA (Hybrid)"
    assert normalized["company"] == "Cisco"
    assert normalized["location"] == "Krakow, Poland"
    assert normalized["remote"] is False

    resolved = _resolve_location_from_dictionary(normalized["location"])
    assert resolved is not None
    assert resolved.get("city") == "Krakow"
    assert resolved.get("state") == "Poland"
    assert resolved.get("country") == "Poland"

    convex_row = _load_convex_fixture()
    expected_posted_at = int(convex_row.get("postedAt") or 0)
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is True


def test_cisco_convex_job_description_strips_junk():
    normalized = _normalize_job()
    description = normalized.get("description") or ""
    lowered = description.lower()

    assert "apply now" not in lowered
    assert "save job" not in lowered
    assert "get notified for similar jobs" not in lowered
    assert "similar jobs" not in lowered
    assert "lorem ipsum" not in lowered


def test_cisco_convex_job_compensation_range_empty():
    normalized = _normalize_job()
    hints = parse_markdown_hints(normalized.get("description") or "")
    comp_range = hints.get("compensation_range") or {}

    assert comp_range.get("low") is None
    assert comp_range.get("high") is None
