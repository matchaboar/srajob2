from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
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


def test_cisco_job_detail_normalization_strips_junk_and_keeps_location():
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_job_detail_commonmark.json"
    )
    payload = _load_spidercloud_fixture(fixture_path)
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")

    scraper = _make_scraper()
    normalized = scraper._normalize_job(
        "https://careers.cisco.com/global/en/job/2000531/Consulting-Engineer-I-Full-Time-United-States",
        markdown,
        [event],
        0,
        require_keywords=False,
    )

    assert normalized is not None
    assert normalized["title"] == "Consulting Engineer I (Full Time) - United States"
    assert "Research Triangle Park" in normalized["location"]
    assert "Please note this posting is to advertise potential job opportunities" in normalized["description"]
    assert "Save job" not in normalized["description"]
    assert "Share via" not in normalized["description"]
    assert "Apply Now" not in normalized["description"]


def test_paloalto_job_detail_uses_structured_description_and_location():
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_paloalto_networks_job_detail_raw_html.json"
    )
    payload = _load_spidercloud_fixture(fixture_path)
    event = payload[0][0]

    scraper = _make_scraper()
    normalized = scraper._normalize_job(
        "https://jobs.paloaltonetworks.com/en/job/santa-clara/senior-ui-software-engineer-cortex/47263/88366836256",
        "",
        [event],
        0,
        require_keywords=False,
    )

    assert normalized is not None
    assert normalized["title"] == "Senior UI Software Engineer (Cortex)"
    assert "Santa Clara" in normalized["location"]
    assert "Our Mission" in normalized["description"]
    assert "Key Responsibilities" in normalized["description"]


def test_paloalto_job_detail_prefers_structured_description_when_markdown_has_chrome():
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_paloalto_networks_job_detail_tools_platforms_raw_html.json"
    )
    payload = _load_spidercloud_fixture(fixture_path)
    event = payload[0][0]

    scraper = _make_scraper()
    markdown = scraper._extract_markdown(event)
    assert markdown, "expected markdown extracted from raw HTML fixture"

    normalized = scraper._normalize_job(
        "https://jobs.paloaltonetworks.com/en/job/santa-clara/sr-software-engineer-tools-and-platforms-cortex/47263/85461897024",
        markdown,
        [event],
        0,
        require_keywords=False,
    )

    assert normalized is not None
    assert "Our Mission" in normalized["description"]
    assert "Saved Jobs" not in normalized["description"]
    assert "Job Alerts" not in normalized["description"]
