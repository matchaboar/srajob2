from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    normalize_company_hint,
    normalize_title_from_bar,
    parse_markdown_hints,
    parse_posted_at,
    strip_known_nav_blocks,
)
from job_scrape_application.workflows.site_handlers.ashby import AshbyHqHandler  # noqa: E402


FIXTURES = Path(__file__).parent.parent / "fixtures"
FIXTURE_PATH = FIXTURES / "convex_voltage-park_jobs.json"


def _load_job() -> dict:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    jobs = payload.get("jobs") or []
    assert jobs, "expected jobs in convex_voltage-park_jobs.json"
    return jobs[0]


def _split_location_components(location: Optional[str], country: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not location:
        return None, None, country
    normalized = location.strip().lower()
    if normalized in {"unknown", "n/a", "na", "unspecified", "not available"}:
        return None, None, country
    parts = [part.strip() for part in location.split(",") if part.strip()]
    if len(parts) == 1:
        return parts[0], None, country
    if len(parts) == 2:
        return parts[0], parts[1], country
    return parts[0], parts[-2], parts[-1]


def test_voltage_park_fixture_core_fields():
    job = _load_job()
    title = normalize_title_from_bar(job.get("title") or "")
    company = normalize_company_hint(job.get("company") or "")

    assert title == "Voltagepark.Com"
    assert company == "voltage park"
    assert job.get("location") == "Unknown"
    assert job.get("remote") is False

    city, state, country = _split_location_components(job.get("location"), job.get("country"))
    assert city is None
    assert state is None
    assert country == "United States"


def test_voltage_park_fixture_posted_at_parses():
    job = _load_job()
    posted_at = job.get("postedAt")
    assert posted_at is not None
    assert parse_posted_at(posted_at) == int(posted_at)


def test_voltage_park_fixture_compensation_range_is_empty():
    job = _load_job()
    cleaned = strip_known_nav_blocks(job.get("description") or "")
    hints = parse_markdown_hints(cleaned)
    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None


def test_voltage_park_fixture_strips_javascript_prompt():
    job = _load_job()
    cleaned = strip_known_nav_blocks(job.get("description") or "")
    assert "enable javascript to run this app" not in cleaned.lower()


def test_ashby_handler_marks_voltage_park_listing_url():
    job = _load_job()
    url = job.get("url") or ""
    handler = AshbyHqHandler()
    assert handler.is_listing_url(url)
    assert handler.is_listing_url(f"{url}/123") is False
