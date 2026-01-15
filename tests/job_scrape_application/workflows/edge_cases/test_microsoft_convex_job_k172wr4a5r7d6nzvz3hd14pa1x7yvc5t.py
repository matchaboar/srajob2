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
    "tests/job_scrape_application/workflows/fixtures/convex_job_k172wr4a5r7d6nzvz3hd14pa1x7yvc5t.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_microsoft_convex_job_hints_extract_core_fields():
    row = _load_fixture()
    hints = parse_markdown_hints(row.get("description") or "")

    assert hints.get("title") == "Senior Software Engineer, Minecraft"
    assert hints.get("company") == "Microsoft"
    assert (hints.get("location") or "").startswith("Redmond")
    assert hints.get("remote") is False

    resolved = _resolve_location_from_dictionary(hints.get("location") or "")
    assert resolved is not None
    assert resolved.get("city") == "Redmond"
    assert resolved.get("state") == "Washington"
    assert resolved.get("country") == "United States"

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 158400
    assert comp_range.get("high") == 258000


def test_microsoft_convex_job_heuristics_update_title_location_comp_and_description():
    row = _load_fixture()
    posted_at, posted_at_unknown = parse_posted_at_with_unknown(row.get("postedAt"))

    assert posted_at == int(row["postedAt"])
    assert posted_at_unknown is False

    row.update(
        {
            "totalCompensation": 0,
            "compensationUnknown": True,
            "heuristicAttempts": 0,
        }
    )
    patch, _records = _build_job_detail_heuristic_patch(row, [], 0, use_extractors=False)

    assert patch.get("title") == "Senior Software Engineer, Minecraft"
    assert patch.get("jobTitle") == "Senior Software Engineer, Minecraft"
    assert (patch.get("location") or "").startswith("Redmond")
    assert patch.get("totalCompensation") == 208200

    description = patch.get("description") or ""
    assert description and not description.lstrip().startswith("{")
    assert "positionExtraDetails" not in description
