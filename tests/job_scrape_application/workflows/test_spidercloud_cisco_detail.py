from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.helpers.regex_patterns import (  # noqa: E402
    JSON_LD_SCRIPT_PATTERN,
)
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    parse_posted_at_with_unknown,
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


def _extract_json_ld_date(raw_html: str) -> str | None:
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

    for match in re.finditer(JSON_LD_SCRIPT_PATTERN, raw_html, flags=re.IGNORECASE | re.DOTALL):
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


def test_cisco_job_detail_uses_raw_html_json_ld_for_posted_at():
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_job_detail_commonmark_raw_html.json"
    )
    payload = _load_spidercloud_fixture(fixture_path)
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")
    raw_html = event.get("content", {}).get("raw", "")

    assert raw_html, "expected raw HTML payload for JSON-LD"
    expected_date = _extract_json_ld_date(raw_html)
    assert expected_date, "expected JSON-LD datePosted in raw HTML"

    now_ms = int(datetime(2026, 1, 8, tzinfo=timezone.utc).timestamp() * 1000)
    scraper = _make_scraper()
    normalized = scraper._normalize_job(
        "https://careers.cisco.com/global/en/job/1448524/Site-Reliability-Engineer-SplunkCloud-Tech-Ops-FedRAMP-33504",
        markdown,
        [event],
        now_ms,
        require_keywords=False,
    )

    assert normalized is not None
    expected_posted_at, expected_unknown = parse_posted_at_with_unknown(expected_date, now_ms=now_ms)
    assert expected_unknown is False
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is False
