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

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k172qx3rqbky0ywxg59r828fdn7ytbt3.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_microsoft_convex_hints_extract_title_location_company_salary_and_posted_at():
    row = _load_fixture()
    hints = parse_markdown_hints(row.get("description") or "")

    assert hints.get("title") == "Data Engineer II"
    assert hints.get("company") == "Microsoft"

    location_hint = hints.get("location")
    assert isinstance(location_hint, str) and location_hint
    resolved = _resolve_location_from_dictionary(location_hint)
    assert resolved is not None
    assert resolved.get("city") == "Redmond"
    assert resolved.get("state") == "Washington"
    assert resolved.get("country") == "United States"
    assert hints.get("remote") is False

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 131400
    assert comp_range.get("high") == 215400

    posted_at, posted_at_unknown = parse_posted_at_with_unknown(row.get("postedAt"))
    assert posted_at == int(row["postedAt"])
    assert posted_at_unknown is False


def test_microsoft_convex_heuristic_patch_updates_title_comp_remote_and_description():
    row = _load_fixture()
    row.update(
        {
            "totalCompensation": 0,
            "compensationUnknown": True,
            "heuristicAttempts": 0,
        }
    )
    patch, _records = _build_job_detail_heuristic_patch(row, [], 0)

    assert patch.get("title") == "Data Engineer II"
    assert patch.get("jobTitle") == "Data Engineer II"
    assert patch.get("totalCompensation") == 173400
    assert patch.get("remote") is False

    description = patch.get("description") or ""
    assert description
    assert not description.lstrip().startswith("{")
    assert "\"positionExtraDetails\"" not in description
    assert "\"status\": 200" not in description
