from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional

from job_scrape_application.workflows.site_handlers import WorkdayHandler

FIXTURE = Path("tests/fixtures/workday_broadcom_listing_rendered.json")


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


def test_workday_handler_extracts_job_links_and_pagination():
    handler = WorkdayHandler()
    html = _load_html(FIXTURE)
    links = handler.get_links_from_raw_html(html)

    job_links = [link for link in links if "/job/" in link.lower()]
    pagination_links = [link for link in links if "offset=" in link.lower()]

    assert any("/external_career/job/" in link.lower() for link in job_links)
    assert pagination_links
    assert not any("/job/" in link.lower() for link in pagination_links)

    offsets: set[int] = set()
    for link in pagination_links:
        parsed = link.split("?", 1)
        if len(parsed) == 2:
            query = parsed[1]
            for part in query.split("&"):
                if part.lower().startswith("offset="):
                    try:
                        offsets.add(int(part.split("=", 1)[1]))
                    except ValueError:
                        pass

    assert 0 in offsets
    assert 20 in offsets
    assert 180 in offsets
    assert len(offsets) <= 10
