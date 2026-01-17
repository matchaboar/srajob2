from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.normalizers import build_job_update as _build_job_detail_heuristic_patch  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    _resolve_location_from_dictionary,
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k17bvzcwfk1vjtss5yah4t2j1d7ysbmd.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_snapchat_convex_hints_extract_title_location_company_salary_and_posted_at():
    row = _load_fixture()
    hints = parse_markdown_hints(row["description"])

    assert hints.get("title") == "Lead, Trust & Safety U.S. Operations"
    assert hints.get("company") == "Snap"
    assert hints.get("location") == "New York, NY"
    assert hints.get("remote") is False

    resolved = _resolve_location_from_dictionary(hints.get("location") or "")
    assert resolved is not None
    assert resolved.get("city") == "New York"
    assert resolved.get("state") == "New York"
    assert resolved.get("country") == "United States"

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 130000
    assert comp_range.get("high") == 196000

    posted_at, posted_at_unknown = parse_posted_at_with_unknown(row.get("postedAt"))
    assert posted_at == int(row["postedAt"])
    assert posted_at_unknown is False


def test_snapchat_job_detail_heuristic_patch_updates_title_location_comp_and_description():
    row = _load_fixture()
    row.update(
        {
            "totalCompensation": 0,
            "compensationUnknown": True,
            "heuristicAttempts": 0,
        }
    )
    patch, _records = _build_job_detail_heuristic_patch(row, [], 0, use_extractors=False)

    assert patch.get("title") == "Lead, Trust & Safety U.S. Operations"
    assert patch.get("jobTitle") == "Lead, Trust & Safety U.S. Operations"
    assert patch.get("location") == "New York, NY"
    assert patch.get("totalCompensation") == 163000
    assert patch.get("remote") is False

    description = patch.get("description") or ""
    assert "Life at Snap" not in description
    assert "Ready to join Team Snap" not in description
