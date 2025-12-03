from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.helpers.scrape_utils import (
    normalize_single_row,
    parse_markdown_hints,
)


def test_parse_markdown_hints_extracts_fields():
    markdown = textwrap.dedent(
        """
        # Senior Software Engineer, Tokenization
        Toronto, Canada
        Base Pay Range: $145,000-$170,000 CAD
        Location: Toronto, Canada
        """
    )

    hints = parse_markdown_hints(markdown)

    assert hints["title"].startswith("Senior Software Engineer")
    assert hints["location"] == "Toronto, Canada"
    assert hints["level"] == "senior"
    assert hints["compensation"] == 157500  # average of range


def test_normalize_single_row_uses_markdown_hints():
    markdown = textwrap.dedent(
        """
        # Principal Engineer
        New York, NY
        Base salary: $200,000 - $240,000 per year
        Hybrid work environment.
        """
    )
    row = {
        "title": "Job Application for Principal Engineer at Example",
        "url": "https://boards.greenhouse.io/example/jobs/123",
        "description": markdown,
    }

    normalized = normalize_single_row(row)

    assert normalized is not None
    assert normalized["title"] == "Principal Engineer"
    assert normalized["location"] == "New York, NY"
    assert normalized["level"] == "principal"
    assert normalized["total_compensation"] == 220000
    assert normalized["compensation_reason"] == "parsed from description"


def test_normalize_single_row_strips_job_application_prefix():
    markdown_path = Path(__file__).parent.parent / "fixtures" / "markdown_robinhood_offsec.md"
    markdown = markdown_path.read_text(encoding="utf-8")
    row = {
        "title": "Job Application for Senior Offensive Security Engineer at Robinhood",
        "job_title": "Job Application for Senior Offensive Security Engineer at Robinhood",
        "url": "https://boards.greenhouse.io/robinhood/jobs/123",
        "description": markdown,
    }

    normalized = normalize_single_row(row)

    assert normalized is not None
    assert normalized["title"] == "Senior Offensive Security Engineer"
    assert normalized["location"] == "Menlo Park, CA"
    assert normalized["level"] == "senior"
    assert normalized["total_compensation"] >= 187000
