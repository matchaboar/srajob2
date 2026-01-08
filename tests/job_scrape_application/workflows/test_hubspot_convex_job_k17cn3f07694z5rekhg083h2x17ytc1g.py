from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.activities import _build_job_detail_heuristic_patch  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.site_handlers import HubspotCareersHandler  # noqa: E402

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k17cn3f07694z5rekhg083h2x17ytc1g.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _load_cleaned_description(row: dict) -> tuple[str, str | None]:
    handler = HubspotCareersHandler()
    markdown = row.get("description") or ""
    return handler.normalize_markdown(markdown)


def test_hubspot_convex_job_normalizes_markdown_and_removes_application_form():
    row = _load_fixture()
    cleaned, title = _load_cleaned_description(row)

    assert title == "Account Executive Mid Market - France"
    assert "Apply for This Job" not in cleaned
    assert "Submit Your Application" not in cleaned
    assert "Back to all openings" not in cleaned
    assert "Careers Menu" not in cleaned


def test_hubspot_convex_job_hints_extract_title_location_remote_and_comp():
    row = _load_fixture()
    cleaned, _title = _load_cleaned_description(row)
    hints = parse_markdown_hints(cleaned)

    assert hints.get("title") == "Account Executive Mid Market - France"

    location = hints.get("location") or ""
    assert location.lower().startswith("dublin")

    resolved = _resolve_location_from_dictionary(location)
    assert resolved is not None
    assert resolved.get("city") == "Dublin"
    assert resolved.get("state") == "Ireland"
    assert resolved.get("country") == "Ireland"

    assert hints.get("remote") is False

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None
    assert hints.get("compensation") is None


def test_hubspot_convex_job_extracts_company_and_posted_at():
    row = _load_fixture()
    handler = HubspotCareersHandler()

    assert handler.extract_company(row, row.get("url")) == "HubSpot"

    posted_at, posted_unknown = parse_posted_at_with_unknown(row.get("postedAt"))
    assert posted_at == int(row["postedAt"])
    assert posted_unknown is False


def test_hubspot_convex_job_heuristics_update_title_location_remote_and_description():
    row = _load_fixture()
    cleaned, _title = _load_cleaned_description(row)
    row["description"] = cleaned
    row.update(
        {
            "totalCompensation": 0,
            "compensationUnknown": True,
            "heuristicAttempts": 0,
        }
    )

    patch, _records = _build_job_detail_heuristic_patch(row, [], 0)

    assert patch.get("title") == "Account Executive Mid Market - France"
    assert patch.get("jobTitle") == "Account Executive Mid Market - France"

    location = patch.get("location") or ""
    assert location.lower().startswith("dublin")
    assert patch.get("remote") is False

    description = patch.get("description") or ""
    assert "Apply for This Job" not in description
    assert "Submit Your Application" not in description
    assert "Back to all openings" not in description
    assert "Careers Menu" not in description
