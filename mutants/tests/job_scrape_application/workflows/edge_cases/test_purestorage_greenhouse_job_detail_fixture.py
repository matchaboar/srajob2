from __future__ import annotations

import html as html_lib
import orjson
import re
from pathlib import Path
from typing import Any, Dict

import pytest


from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    parse_markdown_hints,
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers import GreenhouseHandler  # noqa: E402


FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/"
    "spidercloud_purestorage_greenhouse_job_detail_7490609_raw_api.json"
)
JOB_URL = "https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7490609"


class _FakeClient:
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        self.calls.append({"url": url, "params": params, "stream": stream, "content_type": content_type})
        if stream:
            return self._stream_response()
        return self._sync_response()

    async def _stream_response(self):
        yield self.payload

    async def _sync_response(self):
        return self.payload


def _make_scraper() -> SpiderCloudScraper:
    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=lambda *_args, **_kwargs: [],
    )
    return SpiderCloudScraper(deps)


def _load_fixture() -> Dict[str, Any]:
    payload = orjson.loads(FIXTURE.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        payload = payload[0][0]
    if not isinstance(payload, dict):
        raise AssertionError("Expected spidercloud fixture to yield a dict payload")
    return payload


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


def _extract_job_payload(html_text: str) -> Dict[str, Any]:
    match = re.search(r"<pre[^>]*>(?P<content>.*?)</pre>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise AssertionError("Unable to locate <pre> JSON block in fixture HTML")
    content = html_lib.unescape(match.group("content")).strip()
    if not content:
        raise AssertionError("Empty <pre> content in fixture HTML")
    parsed = orjson.loads(content)
    if not isinstance(parsed, dict):
        raise AssertionError("Expected JSON object payload from fixture")
    return parsed


@pytest.mark.asyncio
async def test_purestorage_greenhouse_job_detail_fixture_normalizes_job():
    scraper = _make_scraper()
    payload = _load_fixture()
    html = _extract_first_html(payload)
    assert html
    job_payload = _extract_job_payload(html)

    result = await scraper._scrape_single_url_sync(  # noqa: SLF001
        _FakeClient(payload),
        JOB_URL,
        {"return_format": ["raw_html"]},
    )

    normalized = result.get("normalized")
    assert normalized is not None, "expected normalized job payload"
    assert normalized.get("title") == "System Engineer, Enterprise (Germany South)"
    assert normalized.get("company") == "Pure Storage"
    assert normalized.get("location") == "Remote, Germany"
    assert normalized.get("remote") is True

    expected_posted_at, expected_unknown = parse_posted_at_with_unknown(job_payload.get("first_published"))
    assert normalized.get("posted_at") == expected_posted_at
    assert normalized.get("posted_at_unknown") == expected_unknown

    description = normalized.get("description") or ""
    assert "THE ROLE" in description
    assert "<div" not in description
    assert "&lt;" not in description
    assert normalized.get("total_compensation") == 0
    assert normalized.get("compensation_unknown") is True


def test_purestorage_greenhouse_job_detail_fixture_strips_html_and_salary_noise():
    payload = _load_fixture()
    html = _extract_first_html(payload)
    assert html
    job_payload = _extract_job_payload(html)

    handler = GreenhouseHandler()
    markdown, title = handler.normalize_markdown(orjson.dumps(job_payload).decode("utf-8"))

    assert title == "System Engineer, Enterprise (Germany South)"
    assert "<div" not in markdown
    assert "&lt;" not in markdown

    hints = parse_markdown_hints(markdown)
    comp_range = hints.get("compensation_range") or {}
    assert not comp_range
