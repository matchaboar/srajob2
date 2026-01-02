from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers import (  # noqa: E402
    NotionCareersHandler,
    get_site_handler,
)


def _load_commonmark() -> str:
    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_notion_careers_commonmark.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    event = payload[0][0]
    return event.get("content", {}).get("commonmark", "")


def test_notion_handler_matches_and_extracts_links():
    handler = NotionCareersHandler()
    url = "https://www.notion.com/careers"
    assert handler.matches_url(url)
    assert handler.is_listing_url(url)
    assert isinstance(get_site_handler(url), NotionCareersHandler)

    markdown = _load_commonmark()
    links = handler.get_links_from_markdown(markdown)
    assert links
    assert all(link.startswith("https://jobs.ashbyhq.com/notion/") for link in links)
    assert "https://www.forbes.com/" not in links


def test_notion_handler_spidercloud_config_uses_commonmark():
    handler = NotionCareersHandler()
    config = handler.get_spidercloud_config("https://www.notion.com/careers")
    assert config.get("return_format") == ["commonmark"]
    assert config.get("request") == "basic"


def test_notion_handler_strips_trailing_markdown_junk():
    handler = NotionCareersHandler()
    markdown = """
## Open Positions
* Role A (https://jobs.ashbyhq.com/notion/a003d9b2-bc51-4f5b-8bca-068f10114308)
* Role B https://jobs.ashbyhq.com/notion/87b03f55-c420-44ed-a9db-61519ea03fa5)
* Role C https://jobs.ashbyhq.com/notion/c49b5c9b-6646-4a13-af57-ed522d15cdf7)\n###
[External](https://example.com)
"""
    links = handler.get_links_from_markdown(markdown)
    assert sorted(links) == sorted(
        [
            "https://jobs.ashbyhq.com/notion/a003d9b2-bc51-4f5b-8bca-068f10114308",
            "https://jobs.ashbyhq.com/notion/87b03f55-c420-44ed-a9db-61519ea03fa5",
            "https://jobs.ashbyhq.com/notion/c49b5c9b-6646-4a13-af57-ed522d15cdf7",
        ]
    )
