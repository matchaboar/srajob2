from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.helpers.scrape_utils import parse_markdown_hints  # noqa: E402


FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_markdown_hints_tokenization_fixture():
    markdown = (FIXTURES / "markdown_robinhood_tokenization.md").read_text(encoding="utf-8")
    hints = parse_markdown_hints(markdown)

    assert hints["title"].startswith("Senior Software Engineer")
    assert hints["location"] == "Toronto, Canada"
    assert hints["level"] == "senior"
    # No compensation in truncated sample; ensure it doesn't crash.
    assert "compensation" not in hints or hints["compensation"] is None


def test_markdown_hints_android_fixture():
    markdown = (FIXTURES / "markdown_robinhood_android.md").read_text(encoding="utf-8")
    hints = parse_markdown_hints(markdown)

    assert hints["title"].startswith("Senior Android Engineer")
    # Combined city pair should be captured from header line.
    assert hints["location"].startswith("Menlo Park")
    assert hints["level"] == "senior"


def test_markdown_hints_offsec_fixture():
    markdown = (FIXTURES / "markdown_robinhood_offsec.md").read_text(encoding="utf-8")
    hints = parse_markdown_hints(markdown)

    assert hints["title"] == "Senior Offensive Security Engineer"
    assert hints["location"] == "Menlo Park, CA"
    assert hints["level"] == "senior"
    assert hints.get("compensation") and hints["compensation"] >= 187000


def test_markdown_hints_github_locations_fixture():
    markdown = (FIXTURES / "markdown_github_locations.md").read_text(encoding="utf-8")
    hints = parse_markdown_hints(markdown)

    assert hints["title"].startswith("Senior Solutions Engineer")
    assert hints["location"] == "France"


def test_markdown_hints_bloomberg_avature_commonmark_fixture():
    markdown = (FIXTURES / "markdown_bloomberg_avature_commonmark.md").read_text(encoding="utf-8")
    hints = parse_markdown_hints(markdown)

    assert hints.get("compensation") == 240000
    assert hints.get("compensation_range") == {"low": 160000, "high": 240000}
