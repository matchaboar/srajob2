from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.activities import _build_job_detail_heuristic_patch  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k17dqpjv1jvrwah9yc3y030pyx7yvjdt.json"
)
EXPECTED_TITLE = "Senior Data Management Professional - Data Product Owner - Data AI"
EXPECTED_COMPANY = "Bloomberg"
EXPECTED_LOCATION = "London, United Kingdom"


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_bloomberg_convex_job_hints_extract_title_company_location_remote_and_comp():
    row = _load_fixture()
    hints = parse_markdown_hints(row.get("description") or "")

    assert hints.get("title") == EXPECTED_TITLE
    assert hints.get("company") == EXPECTED_COMPANY

    location_hint = hints.get("location") or ""
    resolved = _resolve_location_from_dictionary(location_hint)
    assert resolved is not None
    assert resolved.get("city") == "London"
    assert resolved.get("state") == "United Kingdom"
    assert resolved.get("country") == "United Kingdom"
    assert hints.get("remote") is False

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") is None
    assert comp_range.get("high") is None


def test_bloomberg_convex_job_heuristics_patch_updates_title_location_remote_and_description():
    row = _load_fixture()
    row.update(
        {
            "totalCompensation": 0,
            "compensationUnknown": True,
            "heuristicAttempts": 0,
        }
    )

    patch, _records = _build_job_detail_heuristic_patch(row, [], 0, use_extractors=False)

    assert patch.get("title") == EXPECTED_TITLE
    assert patch.get("jobTitle") == EXPECTED_TITLE
    assert patch.get("location") == EXPECTED_LOCATION
    assert patch.get("remote") is False

    description = patch.get("description") or ""
    assert description
    assert "Bloomberg runs on data." in description
    assert "Business Area" not in description
    assert "Ref #" not in description
    assert "- 12569" not in description


def test_bloomberg_convex_job_posted_at_parses():
    row = _load_fixture()
    posted_at, posted_unknown = parse_posted_at_with_unknown(row.get("postedAt"))

    assert posted_at == int(row["postedAt"])
    assert posted_unknown is False
