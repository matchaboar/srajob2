from __future__ import annotations

import orjson
from pathlib import Path


from job_scrape_application.workflows.normalizers import build_job_update as _build_job_detail_heuristic_patch  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.site_handlers import HubspotCareersHandler  # noqa: E402

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k170ypncb9s0rxc75xs4jgs2dh7yvhhd.json"
)


def _load_fixture() -> dict:
    return orjson.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_hubspot_convex_job_normalizes_markdown_and_removes_application_form():
    row = _load_fixture()
    handler = HubspotCareersHandler()
    markdown = row.get("description") or ""
    cleaned, title = handler.normalize_markdown(markdown)

    assert title == "Account Executive (English and Portuguese Fluency Required)"
    assert "Apply for This Job" not in cleaned
    assert "Submit Your Application" not in cleaned
    assert "Back to all openings" not in cleaned


def test_hubspot_convex_job_hints_extract_location_company_and_remote_state():
    row = _load_fixture()
    handler = HubspotCareersHandler()
    markdown = row.get("description") or ""
    cleaned, _title = handler.normalize_markdown(markdown)
    hints = parse_markdown_hints(cleaned)

    assert hints.get("title") == "Account Executive (English and Portuguese Fluency Required)"
    assert hints.get("company") == "HubSpot"

    location = hints.get("location") or ""
    assert location.lower().startswith("bogot")

    resolved = _resolve_location_from_dictionary(location)
    assert resolved is not None
    assert resolved.get("city") == "Bogota"
    assert resolved.get("state") == "Colombia"
    assert resolved.get("country") == "Colombia"

    assert hints.get("remote") is False

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None
    assert hints.get("compensation") is None


def test_hubspot_convex_job_posted_at_parses_from_convex():
    row = _load_fixture()
    posted_at, posted_unknown = parse_posted_at_with_unknown(row.get("postedAt"))

    assert posted_at == int(row["postedAt"])
    assert posted_unknown is False


def test_hubspot_convex_job_heuristics_update_title_location_and_description():
    row = _load_fixture()
    handler = HubspotCareersHandler()
    markdown = row.get("description") or ""
    cleaned, _title = handler.normalize_markdown(markdown)
    row["description"] = cleaned

    row.update(
        {
            "totalCompensation": 0,
            "compensationUnknown": True,
            "heuristicAttempts": 0,
        }
    )

    patch, _records = _build_job_detail_heuristic_patch(row, [], 0, use_extractors=False)

    assert patch.get("title") == "Account Executive (English and Portuguese Fluency Required)"
    assert patch.get("jobTitle") == "Account Executive (English and Portuguese Fluency Required)"

    location = patch.get("location") or ""
    assert location.lower().startswith("bogot")
    assert patch.get("remote") is False

    description = patch.get("description") or ""
    assert "Apply for This Job" not in description
    assert "Submit Your Application" not in description
