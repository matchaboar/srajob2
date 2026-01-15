from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.activities import _build_job_detail_heuristic_patch  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.site_handlers import HubspotCareersHandler  # noqa: E402

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k1706w2saf5mw428zpz1krzb157yyqsc.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _load_cleaned_description(row: dict) -> tuple[str, str | None]:
    handler = HubspotCareersHandler()
    markdown = row.get("description") or ""
    return handler.normalize_markdown(markdown)


def test_hubspot_convex_job_6281024_normalizes_markdown_removes_application_form():
    row = _load_fixture()
    cleaned, title = _load_cleaned_description(row)

    assert title == "Senior Software Engineer II"
    assert cleaned.lstrip().startswith("## Senior Software Engineer II")

    assert "Apply for This Job" not in cleaned
    assert "Submit Your Application" not in cleaned
    assert "Back to all openings" not in cleaned
    assert "Careers Menu" not in cleaned


def test_hubspot_convex_job_6281024_hints_extract_location_remote_and_comp():
    row = _load_fixture()
    cleaned, _title = _load_cleaned_description(row)
    hints = parse_markdown_hints(cleaned)

    assert hints.get("title") == "Senior Software Engineer II"
    assert hints.get("remote") is True
    assert hints.get("location") == "Remote"

    resolved = _resolve_location_from_dictionary(hints.get("location") or "")
    assert resolved is not None
    assert resolved.get("city") == "Remote"
    assert resolved.get("state") == "Remote"
    assert resolved.get("country") == "United States"
    assert resolved.get("remoteOnly") is True

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 186300
    assert comp_range.get("high") == 279500


def test_hubspot_convex_job_6281024_extracts_company_posted_at_and_description():
    row = _load_fixture()
    handler = HubspotCareersHandler()

    assert handler.extract_company(row, row.get("url")) == "HubSpot"

    posted_at, posted_unknown = parse_posted_at_with_unknown(row.get("postedAt"))
    assert posted_at == int(row["postedAt"])
    assert posted_unknown is False

    cleaned, _title = _load_cleaned_description(row)
    row["description"] = cleaned
    row.update(
        {
            "totalCompensation": 0,
            "compensationUnknown": True,
            "heuristicAttempts": 0,
        }
    )

    patch, _records = _build_job_detail_heuristic_patch(row, [], 0, use_extractors=False)

    assert patch.get("title") == "Senior Software Engineer II"
    assert patch.get("jobTitle") == "Senior Software Engineer II"
    assert (patch.get("location") or "").startswith("Remote")
    assert patch.get("remote") is True
    assert patch.get("totalCompensation") == 232900

    description = patch.get("description") or ""
    assert "Apply for This Job" not in description
    assert "Annual Cash Compensation Range" in description
    assert "HubSpot (NYSE: HUBS)" in description
