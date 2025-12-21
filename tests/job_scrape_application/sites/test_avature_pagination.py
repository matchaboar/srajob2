from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional

from job_scrape_application.workflows.site_handlers import AvatureHandler

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
PAGE_1 = FIXTURE_DIR / "spidercloud_bloomberg_avature_search_page_1.json"
PAGE_2 = FIXTURE_DIR / "spidercloud_bloomberg_avature_search_page_2.json"
PAGE_3 = FIXTURE_DIR / "spidercloud_bloomberg_avature_search_page_3.json"


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

    for event in payload if isinstance(payload, list) else [payload]:
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
    html = _extract_first_html(payload)
    if not html:
        raise AssertionError(f"Unable to extract raw HTML from {path}")
    return html


def _pagination_links(handler: AvatureHandler, html: str) -> list[str]:
    links = handler.get_links_from_raw_html(html)
    return [link for link in links if "joboffset=" in link.lower()]


def test_avature_pagination_fixtures_traverse_three_pages():
    handler = AvatureHandler()

    page_1_links = _pagination_links(handler, _load_html(PAGE_1))
    assert any("joboffset=12" in link.lower() for link in page_1_links)

    page_2_links = _pagination_links(handler, _load_html(PAGE_2))
    assert any("joboffset=24" in link.lower() for link in page_2_links)

    page_3_links = _pagination_links(handler, _load_html(PAGE_3))
    assert page_3_links == []
