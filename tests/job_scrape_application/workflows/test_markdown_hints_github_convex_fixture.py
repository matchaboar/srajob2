from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.helpers.scrape_utils import parse_markdown_hints  # noqa: E402


FIXTURES = Path(__file__).parent.parent / "fixtures"

EXPECTED_TITLES = {
    "https://www.github.careers/careers-home/jobs/4815?lang=en-us": "Senior Customer Success Architect",
    "https://www.github.careers/careers-home/jobs/4880?lang=en-us": "Customer Success Architect III",
    "https://www.github.careers/careers-home/jobs/4678?lang=en-us": "Software Engineer III, Codespaces and Actions Compute Platform",
}


def test_markdown_hints_github_convex_fixture_titles():
    payload = json.loads((FIXTURES / "convex_github_jobs.json").read_text(encoding="utf-8"))
    jobs = payload.get("jobs", [])
    assert jobs, "expected jobs in convex_github_jobs.json"

    for job in jobs:
        url = job.get("url")
        expected = EXPECTED_TITLES.get(url)
        assert expected, f"missing expected title mapping for url={url}"
        description = job.get("description") or ""
        hints = parse_markdown_hints(description)
        assert hints.get("title") == expected
