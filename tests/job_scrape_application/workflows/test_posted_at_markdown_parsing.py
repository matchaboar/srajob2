from __future__ import annotations

from job_scrape_application.workflows.site_handlers.base import BaseSiteHandler


class _DummyHandler(BaseSiteHandler):
    @classmethod
    def matches_url(cls, url: str) -> bool:
        return False


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


def test_posted_at_placeholder_iso_ignored():
    handler = _DummyHandler()
    parsed = handler.extract_posted_at_from_markdown("datePosted 1970-01-01T00:00:00")
    assert parsed is None
