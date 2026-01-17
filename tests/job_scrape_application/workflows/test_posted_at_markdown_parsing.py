from __future__ import annotations

import orjson
from pathlib import Path
from typing import Any

from job_scrape_application.workflows.site_handlers.base import BaseSiteHandler


class _DummyHandler(BaseSiteHandler):
    @classmethod
    def matches_url(cls, url: str) -> bool:
        return False


def _extract_raw_html(payload: Any) -> str | None:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("raw_html", "raw", "html"):
            value = payload.get(key)
            if isinstance(value, str):
                return value
        content = payload.get("content")
        if isinstance(content, dict):
            value = content.get("raw")
            if isinstance(value, str):
                return value
        for value in payload.values():
            found = _extract_raw_html(value)
            if found:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _extract_raw_html(item)
            if found:
                return found
    return None


def test_posted_at_from_html_label_ignores_placeholder_json_ld():
    handler = _DummyHandler()
    html = (
        "<html><head>"
        "<script type=\"application/ld+json\">"
        "{\"datePosted\":\"1970-01-01T00:00:00\"}"
        "</script></head><body>"
        "<div class=\"detailLabel\">Date posted</div>"
        "<div class=\"detailValue\">Dec 26, 2025</div>"
        "</body></html>"
    )
    parsed = handler.extract_posted_at_from_markdown(html)
    assert parsed == "2025-12-26T00:00:00+00:00"


def test_posted_at_from_inline_label_parses_iso():
    handler = _DummyHandler()
    parsed = handler.extract_posted_at_from_markdown("Date posted: 2024-03-02")
    assert parsed == "2024-03-02T00:00:00+00:00"


def test_posted_at_from_relative_line():
    handler = _DummyHandler()
    parsed = handler.extract_posted_at_from_markdown("Posted 3 days ago")
    assert parsed is not None
    assert parsed.lower() == "posted 3 days ago"


def test_posted_at_from_relative_plus_line():
    handler = _DummyHandler()
    parsed = handler.extract_posted_at_from_markdown("Posted 30+ Days Ago")
    assert parsed is not None
    assert parsed.lower() == "posted 30 days ago"


def test_posted_at_placeholder_iso_ignored():
    handler = _DummyHandler()
    parsed = handler.extract_posted_at_from_markdown("datePosted 1970-01-01T00:00:00")
    assert parsed is None


def test_posted_at_from_html_body_without_json_ld():
    handler = _DummyHandler()
    html = (
        "<html><body>"
        "<div class=\"detailLabel\">Date posted</div>"
        "<div class=\"detailValue\">Dec 26, 2025</div>"
        "</body></html>"
    )
    parsed = handler.extract_posted_at(html)
    assert parsed == "2025-12-26T00:00:00+00:00"


def test_share_job_k17d5s24_has_no_posted_at() -> None:
    handler = _DummyHandler()
    raw_html = Path(
        "tests/job_scrape_application/workflows/fixtures/convex_share_job_k17d5s24_raw.html"
    ).read_text(encoding="utf-8")
    parsed = handler.extract_posted_at(raw_html)
    assert parsed is None


def test_workday_job_detail_extracts_date_posted() -> None:
    handler = _DummyHandler()
    payload = orjson.loads(
        Path(
            "tests/job_scrape_application/workflows/fixtures/broadcom_workday_r024197_raw.json"
        ).read_text(encoding="utf-8")
    )
    raw_html = _extract_raw_html(payload)
    assert raw_html is not None
    parsed = handler.extract_posted_at(raw_html)
    assert parsed == "2025-11-11"


def test_share_job_query_id_selects_posted_at() -> None:
    handler = _DummyHandler()
    payload = orjson.loads(
        Path("tests/job_scrape_application/workflows/fixtures/convex_share_job_k176k6n2_payload.json").read_text(
            encoding="utf-8"
        )
    )
    url = (
        "https://affable-kiwi-46.convex.site/share/job"
        "?id=k176k6n2jtg2bqdb728twg315s7yvw2e"
        "&app=https%3A%2F%2Fsrajob.netlify.app"
    )
    parsed = handler.extract_posted_at(payload, url)
    assert parsed == "2026-01-07T00:00:00+00:00"
