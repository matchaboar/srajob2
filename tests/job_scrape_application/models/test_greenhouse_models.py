from __future__ import annotations

import html as html_lib
import json
import os
import re
import sys
from pathlib import Path

import pytest


# Ensure repo root is importable
sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.components.models import (  # noqa: E402
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)


def _extract_first_html(payload: object) -> str:
    if isinstance(payload, dict):
        content = payload.get("content")
        if isinstance(content, dict):
            raw = content.get("raw")
            if isinstance(raw, str) and "<html" in raw.lower():
                return raw
        for key in ("raw_html", "html", "body", "text"):
            val = payload.get(key)
            if isinstance(val, str) and "<html" in val.lower():
                return val
        for value in payload.values():
            found = _extract_first_html(value)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _extract_first_html(value)
            if found:
                return found
    return ""


def _load_spidercloud_fixture(path: Path) -> object:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _extract_json_from_pre(html_text: str) -> dict:
    match = re.search(r"<pre[^>]*>(?P<content>.*?)</pre>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise AssertionError("Unable to locate <pre> JSON block in fixture HTML")
    content = html_lib.unescape(match.group("content")).strip()
    if not content:
        raise AssertionError("Empty <pre> content in fixture HTML")
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise AssertionError("Expected JSON object payload from fixture")
    return parsed


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

    # Disable keyword filtering so total aligns with Greenhouse "meta.total".
    urls = extract_greenhouse_job_urls(board, required_keywords=())
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
    # Disable keyword filtering so meta.total aligns with the full board listing.
    urls = extract_greenhouse_job_urls(board, required_keywords=())

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


def test_load_greenhouse_board_accepts_blank_payloads():
    board = load_greenhouse_board(" \n\t")
    assert board.jobs == []
    board = load_greenhouse_board(None)
    assert board.jobs == []


def test_load_greenhouse_board_strips_invalid_escapes():
    job_url = "https://boards.greenhouse.io/example/jobs/789"
    payload = {
        "jobs": [
            {
                "absolute_url": job_url,
                "id": 789,
                "title": "Staff \\q Engineer",
                "location": {"name": "Remote"},
            }
        ]
    }
    valid_json = json.dumps(payload)
    invalid_json = valid_json.replace("\\\\q", "\\q")

    board = load_greenhouse_board(invalid_json)

    assert board.jobs[0].absolute_url == job_url


def test_load_greenhouse_board_parses_html_with_pre():
    job_url = "https://boards.greenhouse.io/example/jobs/123"
    payload = {
        "jobs": [
            {
                "absolute_url": job_url,
                "id": 123,
                "title": "Software Engineer",
                "location": {"name": "Remote"},
            }
        ]
    }
    html_payload = f"<html><body><pre>{json.dumps(payload)}</pre></body></html>"

    board = load_greenhouse_board(html_payload)

    assert board.jobs[0].absolute_url == job_url


def test_load_greenhouse_board_parses_json_with_prefix():
    job_url = "https://boards.greenhouse.io/example/jobs/456"
    payload = {
        "jobs": [
            {
                "absolute_url": job_url,
                "id": 456,
                "title": "Data Scientist",
                "location": {"name": "Remote"},
            }
        ]
    }
    text_payload = f"blocked-response\n{json.dumps(payload)}\ntrailer"

    board = load_greenhouse_board(text_payload)

    assert board.jobs[0].absolute_url == job_url


def test_load_greenhouse_board_parses_xai_listing_fixture():
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_xai_greenhouse_listing.json"
    )
    payload = _load_spidercloud_fixture(fixture_path)
    html = _extract_first_html(payload)
    assert html
    board_payload = _extract_json_from_pre(html)
    board = load_greenhouse_board(board_payload)
    assert board.jobs
    assert any(job.company_name == "xAI" for job in board.jobs)
    # Disable keyword filtering so the fixture total matches the listing metadata.
    urls = extract_greenhouse_job_urls(board, required_keywords=())
    assert any("job-boards.greenhouse.io/xai/jobs/" in url for url in urls)
    total = board_payload.get("meta", {}).get("total")
    assert total is None or len(urls) == total


def test_greenhouse_handler_extracts_xai_detail_fields():
    from job_scrape_application.workflows.site_handlers import GreenhouseHandler

    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_xai_greenhouse_job_detail.json"
    )
    payload = _load_spidercloud_fixture(fixture_path)
    html = _extract_first_html(payload)
    assert html
    job_payload = _extract_json_from_pre(html)

    handler = GreenhouseHandler()
    posted_at = handler.extract_posted_at(job_payload, job_payload.get("absolute_url"))
    assert posted_at == job_payload.get("first_published")

    markdown, title = handler.normalize_markdown(json.dumps(job_payload))
    assert title == job_payload.get("title")
    assert "About xAI" in markdown
    assert "<div" not in markdown
