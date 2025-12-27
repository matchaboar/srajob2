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
