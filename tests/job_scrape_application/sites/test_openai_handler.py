from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers import (  # noqa: E402
    OpenAICareersHandler,
    get_site_handler,
)

FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_openai_careers_listing.json"
)


def _gather_strings(node: Any) -> Iterable[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for val in node.values():
            yield from _gather_strings(val)
    elif isinstance(node, list):
        for val in node:
            yield from _gather_strings(val)


def _extract_first_html(payload: Any) -> Optional[str]:
    def _candidate(value: Any) -> Optional[str]:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            return value
        return None

    def _looks_like_html(text: str) -> bool:
        lowered = text.lower()
        return "<html" in lowered or "<body" in lowered or "<div" in lowered or "<section" in lowered

    if isinstance(payload, list):
        events = payload
    else:
        events = [payload]

    for event in events:
        if isinstance(event, dict):
            for key in ("raw_html", "html", "content", "body", "text", "result"):
                text = _candidate(event.get(key))
                if text and _looks_like_html(text):
                    return text
        text = _candidate(event)
        if text and _looks_like_html(text):
            return text

    for text in _gather_strings(payload):
        if text and _looks_like_html(text):
            return text

    return None


def _load_html(path: Path) -> str:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    html = _extract_first_html(payload)
    if not html:
        raise AssertionError(f"Unable to extract raw HTML from {path}")
    return html


def test_openai_handler_matches_and_extracts_links():
    handler = OpenAICareersHandler()
    listing_url = (
        "https://openai.com/careers/search/?q=engineer&l=e8062547-b090-4206-8f1e-"
        "7329e0014e98"
    )
    detail_url = "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco/"

    assert handler.matches_url(listing_url)
    assert handler.is_listing_url(listing_url)
    assert handler.matches_url(detail_url)
    assert not handler.is_listing_url(detail_url)
    assert isinstance(get_site_handler(listing_url), OpenAICareersHandler)

    html = _load_html(FIXTURE)
    links = handler.get_links_from_raw_html(html)

    assert any(link.endswith("/careers/ai-support-engineer-san-francisco-san-francisco/") for link in links)
    assert links
    assert all(link.startswith("https://openai.com/careers/") for link in links)
    assert not any("jobs.ashbyhq.com" in link for link in links)
    assert not any("/careers/search" in link for link in links)


def test_openai_handler_spidercloud_config():
    handler = OpenAICareersHandler()
    listing_url = "https://openai.com/careers/search/?q=engineer"
    detail_url = "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco/"

    listing_config = handler.get_spidercloud_config(listing_url)
    assert listing_config.get("return_format") == ["raw_html"]

    detail_config = handler.get_spidercloud_config(detail_url)
    assert detail_config.get("return_format") == ["commonmark"]
