from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers import HubspotCareersHandler  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)

CONVEX_FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k178arygp08ynza3gh6dx5k03x7yv210.json"
)
SPIDERCLOUD_FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_hubspot_job_detail_7519220_commonmark.json"
)
JOB_URL = "https://www.hubspot.com/careers/jobs/7519220"


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


def test_hubspot_convex_job_hints_extract_core_fields():
    payload = _load_spidercloud_fixture()
    event = _extract_event(payload)
    markdown = event.get("content", {}).get("commonmark", "")

    handler = HubspotCareersHandler()
    cleaned, title = handler.normalize_markdown(markdown)

    assert title == "Field Marketer, France"

    hints = parse_markdown_hints(cleaned)
    assert hints.get("title") == "Field Marketer, France"
    assert hints.get("remote") is True

    location = hints.get("location") or ""
    assert "France" in location

    resolved = _resolve_location_from_dictionary(location)
    assert resolved is not None
    assert resolved.get("city") is None
    assert resolved.get("state") is None
    assert resolved.get("country") == "France"

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None


def test_hubspot_convex_job_normalization_strips_application_form():
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

    assert normalized is not None
    assert normalized["title"] == "Field Marketer, France"
    assert normalized["company"] == "HubSpot"
    assert "France" in (normalized.get("location") or "")
    assert normalized.get("remote") is True

    description = normalized.get("description") or ""
    assert "Responsibilities" in description
    assert "Apply for This Job" not in description
    assert "Submit Your Application" not in description
    assert "Voluntary Equal Opportunity Employment" not in description

    assert normalized.get("posted_at") == started_at
    assert normalized.get("posted_at_unknown") is True


def test_hubspot_convex_job_posted_at_from_convex():
    row = _load_convex_fixture()
    posted_at, posted_unknown = parse_posted_at_with_unknown(row.get("postedAt"))

    assert posted_at == int(row["postedAt"])
    assert posted_unknown is False
