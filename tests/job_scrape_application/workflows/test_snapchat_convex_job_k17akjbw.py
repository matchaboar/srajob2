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
    "tests/fixtures/convex_job_k17akjbwz4st03ezgaczm3kk717ys5cy.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_snapchat_convex_job_hints_extract_core_fields():
    row = _load_fixture()
    hints = parse_markdown_hints(row.get("description") or "")

    assert hints.get("title") == "HR Business Partner"
    assert hints.get("company") == "Snap"
    assert hints.get("location") == "Los Angeles, CA"
    assert hints.get("remote") is False

    resolved = _resolve_location_from_dictionary(hints.get("location") or "")
    assert resolved is not None
    assert resolved.get("city") == "Los Angeles"
    assert resolved.get("state") == "California"
    assert resolved.get("country") == "United States"

    comp_range = hints.get("compensation_range") or {}
    assert comp_range.get("low") == 130000
    assert comp_range.get("high") == 196000


def test_snapchat_convex_job_heuristics_update_title_posted_at_and_description():
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
    patch, _records = _build_job_detail_heuristic_patch(row, [], 0)

    assert patch.get("title") == "HR Business Partner"
    assert patch.get("jobTitle") == "HR Business Partner"
    assert patch.get("location") == "Los Angeles, CA"
    assert patch.get("totalCompensation") == 163000

    description = patch.get("description") or ""
    assert "Life at Snap" not in description
    assert "Ready to join Team Snap" not in description
