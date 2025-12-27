from __future__ import annotations

import os
import sys

import pytest


# Ensure repo root is importable
sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.components.models import (  # noqa: E402
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)


def test_greenhouse_board_parsing_and_urls():
    payload = {
        "jobs": [
            {
                "absolute_url": "https://boards.greenhouse.io/robinhood/jobs/7278362?t=gh_src=&gh_jid=7278362",
                "id": 7278362,
                "title": "AML Engineer, Crypto",
                "updated_at": "2025-11-24T15:20:56-05:00",
                "location": {"name": "Denver, CO; New York, NY; Westlake, TX"},
            },
            {
                "absolute_url": "https://boards.greenhouse.io/robinhood/jobs/7318478?t=gh_src=&gh_jid=7318478",
                "id": 7318478,
                "title": "Analytics Engineering",
                "company_name": "Robinhood",
                "location": {"name": "Menlo Park, CA"},
            },
            {
                "absolute_url": "https://boards.greenhouse.io/robinhood/jobs/5702135?t=gh_src=&gh_jid=5702135",
                "id": 5702135,
                "title": "Android Engineer",
                "location": {"name": "Toronto, ON"},
            },
        ]
    }

    board = load_greenhouse_board(payload)
    assert len(board.jobs) == 3
    assert board.jobs[0].location is not None

    urls = extract_greenhouse_job_urls(board)
    assert urls[0].startswith("https://boards.greenhouse.io/robinhood/jobs/7278362")
    assert urls[1].endswith("7318478")
    assert len(urls) == 3


def test_extract_greenhouse_job_urls_dedupes():
    payload = {
        "jobs": [
            {"absolute_url": "https://example.com/job/1", "id": 1, "title": "Engineer One"},
            {"absolute_url": "https://example.com/job/1", "id": 2, "title": "Engineer Two"},
            {"absolute_url": "https://example.com/job/2", "id": 3, "title": "Engineer Three"},
        ]
    }

    board = load_greenhouse_board(payload)
    urls = extract_greenhouse_job_urls(board)

    assert urls == ["https://example.com/job/1", "https://example.com/job/2"]


def test_extract_greenhouse_job_urls_filters_titles():
    payload = {
        "jobs": [
            {"absolute_url": "https://example.com/job/eng", "id": 1, "title": "QA Engineer"},
            {"absolute_url": "https://example.com/job/pm", "id": 2, "title": "Product Manager"},
            {"absolute_url": "https://example.com/job/unknown", "id": 3, "title": None},
        ]
    }

    board = load_greenhouse_board(payload)
    urls = extract_greenhouse_job_urls(board)

    assert "https://example.com/job/eng" in urls
    assert "https://example.com/job/unknown" in urls  # Unknown title should still be scraped
    assert "https://example.com/job/pm" not in urls


def test_load_greenhouse_board_rejects_invalid_json():
    with pytest.raises(ValueError, match="Greenhouse board payload was not valid JSON"):
        load_greenhouse_board("not-json")
