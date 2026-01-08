from __future__ import annotations

import html as html_lib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/"
    "spidercloud_verkada_greenhouse_job_detail_4991227007_raw_api.json"
)
JOB_URL = "https://boards-api.greenhouse.io/v1/boards/verkada/jobs/4991227007"


class _FakeClient:
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    async def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        self.calls.append({"url": url, "params": params, "stream": stream, "content_type": content_type})
        yield self.payload


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
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
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
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise AssertionError("Expected JSON object payload from fixture")
    return parsed


@pytest.mark.asyncio
async def test_verkada_greenhouse_job_detail_fixture_normalizes_job():
    scraper = _make_scraper()
    payload = _load_fixture()
    html = _extract_first_html(payload)
    assert html
    job_payload = _extract_job_payload(html)

    result = await scraper._scrape_single_url(  # noqa: SLF001
        _FakeClient(payload),
        JOB_URL,
        {"return_format": ["raw_html"]},
    )

    normalized = result.get("normalized")
    assert normalized is not None, "expected normalized job payload"
    assert normalized.get("title") == "Account Executive, ASEAN"
    assert normalized.get("company") == "Verkada"
    assert normalized.get("location") == "Singapore"

    expected_posted_at, expected_unknown = parse_posted_at_with_unknown(job_payload.get("first_published"))
    assert normalized.get("posted_at") == expected_posted_at
    assert normalized.get("posted_at_unknown") == expected_unknown

    description = normalized.get("description") or ""
    assert "Who We Are" in description
    assert "<div" not in description
    assert "&lt;" not in description
