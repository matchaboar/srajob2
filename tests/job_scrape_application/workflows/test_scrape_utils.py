from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))

import json

from job_scrape_application.workflows.helpers.scrape_utils import (
    _jobs_from_scrape_items,
    normalize_firecrawl_items,
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


def test_normalize_firecrawl_items_handles_greenhouse_job_json():
    raw_json = """
    {"absolute_url":"https://www.pinterestcareers.com/jobs/?gh_jid=5572858","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":2745516,"location":{"name":"Toronto, ON, CA"},"metadata":[{"id":5955,"name":"Employment Type","value":"Regular","value_type":"single_select"},{"id":16373425,"name":"Career Track","value":null,"value_type":"single_select"},{"id":2110283,"name":"Careers Page Department","value":"Engineering","value_type":"single_select"}],"id":5572858,"updated_at":"2025-11-19T19:53:17-05:00","requisition_id":"Evergreen - Backend Engineer, IC15, Monetization, CAN","title":"Sr. Software Engineer, Backend","company_name":"Pinterest","first_published":"2023-12-15T14:26:24-05:00","language":"en","content":"<div class=\\"content-intro\\"><p><strong>About Pinterest:</strong></p><p>Millions of people around the world come to our platform to find creative ideas, dream about new possibilities and plan for memories that will last a lifetime.</p></div>","departments":[{"id":7789,"name":"Engineering and Product (L2)","child_ids":[71474,77118,84986,71470,71472,71473,77096,84413,71468,523,91068,285130,285128,285129],"parent_id":null}],"offices":[{"id":58564,"name":"Toronto","location":"Toronto, ON, CA","child_ids":[],"parent_id":78375}]}
    """

    payload = json.loads(raw_json)
    normalized = normalize_firecrawl_items({"json": payload})

    assert len(normalized) == 1
    row = normalized[0]
    assert row["title"] == payload["title"]
    assert row["url"] == payload["absolute_url"]
    assert row["company"] == "Pinterest"
    assert row["location"] == "Toronto, ON, CA"
    assert "Pinterest" in row["description"]


def test_jobs_from_scrape_items_uses_normalized_row():
    payload = {
        "absolute_url": "https://www.pinterestcareers.com/jobs/?gh_jid=5572858",
        "title": "Sr. Software Engineer, Backend",
        "company_name": "Pinterest",
        "location": {"name": "Toronto, ON, CA"},
        "content": "<p>About Pinterest</p>",
    }

    normalized = normalize_firecrawl_items({"json": payload})
    items = {"normalized": normalized}

    jobs = _jobs_from_scrape_items(items, default_posted_at=0, scraped_at=123, scraped_with="firecrawl")
    assert len(jobs) == 1
    job = jobs[0]
    assert job["title"] == "Sr. Software Engineer, Backend"
    assert job["url"] == payload["absolute_url"]
    assert job["scrapedAt"] == 123
    assert "{" not in job["title"]
