from __future__ import annotations

import html as html_lib
import json
import os
import re
import sys
from pathlib import Path

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _JobRowNormalizer,
    parse_markdown_hints,
    parse_posted_at,
    strip_known_nav_blocks,
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


def _load_spidercloud_event() -> tuple[dict, str]:
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_githubinc_jibeapply_job_detail_commonmark.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")
    return event, markdown


def _load_jibe_raw_event() -> tuple[dict, str]:
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_github_jibe_job_4771_raw.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    event = payload[0][0]
    raw_html = event.get("content", {}).get("raw", "")
    return event, raw_html


def _extract_json_ld(raw_html: str) -> dict:
    match = re.search(
        r"<script[^>]*application/ld\+json[^>]*>(?P<payload>.*?)</script>",
        raw_html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not match:
        raise AssertionError("Missing JSON-LD payload in JibeApply fixture")
    payload_raw = html_lib.unescape(match.group("payload").strip())
    return json.loads(payload_raw)


def test_jibeapply_markdown_hints_extract_company_location_salary_remote():
    _, markdown = _load_spidercloud_event()
    hints = parse_markdown_hints(markdown)

    assert hints.get("title") == "Staff Software Engineer, Data Engineering"
    assert hints.get("company") == "GitHub"
    assert hints.get("remote") is True
    assert hints.get("location") == "United States"
    assert "United States" in hints.get("locations", [])

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 140400
    assert comp_range.get("high") == 372300

    location = hints.get("location") or ""
    city = None
    state = None
    country = location if location == "United States" else None
    if "," in location:
        parts = [part.strip() for part in location.split(",", 1)]
        if len(parts) == 2:
            city, state = parts

    assert city is None
    assert state is None
    assert country == "United States"


def test_spidercloud_jibeapply_detail_normalizes_company_title_location_and_description():
    event, markdown = _load_spidercloud_event()
    scraper = _make_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        "https://githubinc.jibeapply.com/jobs/4788?lang=en-us",
        markdown,
        [event],
        0,
        require_keywords=False,
    )

    assert normalized is not None
    assert normalized["title"] == "Staff Software Engineer, Data Engineering"
    assert normalized["company"] == "GitHub"
    assert normalized["remote"] is True
    assert "United States" in normalized["location"]

    cleaned = strip_known_nav_blocks(normalized["description"])
    for junk in (
        "JOB_DESCRIPTION.SHARE.HTML",
        "CAROUSEL_PARAGRAPH",
        "mail_outline",
        "LoginorRegister",
        "Get future jobs matching this search",
    ):
        assert junk not in cleaned


def test_convex_job_normalizer_uses_hint_company_title_and_posted_at():
    fixture_path = Path("tests/job_scrape_application/workflows/fixtures/convex_job_k1748.json")
    row = json.loads(fixture_path.read_text(encoding="utf-8"))
    normalizer = _JobRowNormalizer()

    normalized = normalizer.normalize_row(row)
    assert normalized is not None
    assert normalized["company"] == "GitHub"
    assert normalized["job_title"] == "Staff Software Engineer, Data Engineering"
    assert normalized["posted_at"] == int(row["postedAt"])
    assert normalized["posted_at_unknown"] is False


def test_spidercloud_jibeapply_hourly_job_detail_extracts_salary_location_and_posted_date():
    event, raw_html = _load_jibe_raw_event()
    json_ld = _extract_json_ld(raw_html)

    scraper = _make_scraper()
    markdown = scraper._extract_markdown(event) or ""  # noqa: SLF001
    normalized = scraper._normalize_job(  # noqa: SLF001
        "https://githubinc.jibeapply.com/jobs/4771?lang=en-us",
        markdown,
        [event],
        0,
        require_keywords=False,
    )

    assert normalized is not None
    assert normalized["title"] == "Product Manager Intern"
    assert normalized["company"] == "GitHub"
    assert normalized["remote"] is True
    assert "United States" in normalized["location"]

    expected_posted_at = parse_posted_at(json_ld.get("datePosted"))
    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is False

    cleaned = strip_known_nav_blocks(normalized["description"])
    assert "About GitHub" in cleaned
    assert "Compensation Range" in cleaned
    for junk in (
        "JOB_DESCRIPTION.SHARE.HTML",
        "CAROUSEL_PARAGRAPH",
        "mail_outline",
        "LoginorRegister",
        "Get future jobs matching this search",
        "<iframe",
    ):
        assert junk not in cleaned

    hints = parse_markdown_hints(normalized["description"])
    assert hints.get("location") == "United States"
    assert hints.get("remote") is True

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 68994
    assert comp_range.get("high") == 182894
