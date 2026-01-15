from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    parse_posted_at_with_unknown,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/convex_job_k17b53z6txnbwffc5ztyxf2cks7ytade.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_microsoft_convex_job_posted_at_is_known():
    row = _load_fixture()
    posted_at, posted_at_unknown = parse_posted_at_with_unknown(row.get("postedAt"))

    assert posted_at == int(row["postedAt"])
    assert posted_at_unknown is False
