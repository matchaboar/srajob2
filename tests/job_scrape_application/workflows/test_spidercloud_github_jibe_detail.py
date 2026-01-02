from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

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
    "tests/job_scrape_application/workflows/fixtures/spidercloud_github_jibe_job_4691.json"
)
JOB_URL = "https://githubinc.jibeapply.com/jobs/4691?lang=en-us"


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
    return payload[0][0]


def _normalize_job() -> dict:
    event = _load_event()
    markdown = event.get("content", {}).get("commonmark", "")
    scraper = _make_scraper()
    normalized = scraper._normalize_job(
        JOB_URL,
        markdown,
        [event],
        0,
        require_keywords=False,
    )
    assert normalized is not None, "expected normalized job payload"
    return normalized


def test_github_jibe_job_detail_uses_structured_description():
    normalized = _normalize_job()
    assert normalized["title"] == "Staff Applied Researcher, AI Quality"
    assert normalized["company"] == "GitHub"
    assert normalized["location"] == "United States"
    assert normalized["remote"] is True
    description = normalized["description"]
    assert "About GitHub" in description
    assert "LoginorRegister" not in description
    assert "JOB_DESCRIPTION.SHARE.HTML" not in description
    assert "<iframe" not in description
    assert "mail_outline" not in description


def test_github_jibe_job_detail_compensation_range_and_location_fields():
    normalized = _normalize_job()
    hints = parse_markdown_hints(normalized["description"])
    assert hints.get("compensation_range") == {"low": 140400, "high": 372300}
    resolved = _resolve_location_from_dictionary(normalized["location"])
    assert resolved is not None
    assert resolved.get("city") is None
    assert resolved.get("state") is None
    assert resolved.get("country") == "United States"


def test_github_jibe_job_detail_posted_at_from_structured_payload():
    event = _load_event()
    scraper = _make_scraper()
    structured = scraper._extract_structured_job_posting([event])
    assert structured is not None and structured.get("datePosted")
    expected_posted_at, expected_unknown = parse_posted_at_with_unknown(
        structured.get("datePosted")
    )
    normalized = _normalize_job()
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] == expected_unknown
