from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import re

import pytest
from spider import AsyncSpider

GITHUB_API_URL = (
    "https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100"
)

SPIDER_PARAMS: Dict[str, Any] = {
    "return_format": ["raw_html"],
    "metadata": True,
    "request": "chrome",
    "follow_redirects": True,
    "redirect_policy": "Loose",
    "external_domains": ["*"],
    "preserve_host": True,
    "limit": 1,
}

API_KEY = "stubbed-key"


async def _collect_response(response: Any) -> List[Any]:
    if hasattr(response, "__aiter__"):
        items = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        result = await response
        return [result] if result is not None else []
    return [response] if response is not None else []


def _gather_strings(node: Any) -> Iterable[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for val in node.values():
            yield from _gather_strings(val)
    elif isinstance(node, list):
        for val in node:
            yield from _gather_strings(val)


def _find_jobs_payload(node: Any) -> Optional[Dict[str, Any]]:
    if isinstance(node, dict) and isinstance(node.get("jobs"), list):
        return node
    if isinstance(node, dict):
        for val in node.values():
            found = _find_jobs_payload(val)
            if found:
                return found
    if isinstance(node, list):
        for val in node:
            found = _find_jobs_payload(val)
            if found:
                return found
    return None


def _extract_payload(events: List[Any]) -> Optional[Dict[str, Any]]:
    found = _find_jobs_payload(events)
    if found:
        return found
    for text in _gather_strings(events):
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        found = _find_jobs_payload(parsed)
        if found:
            return found
    return None


def _extract_json_from_html(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    if "<pre" not in text.lower():
        return None
    match = re.search(r"<pre>(?P<content>.*?)</pre>", text, flags=re.IGNORECASE | re.DOTALL)
    content = match.group("content") if match else text
    content = content.strip()
    if not content:
        return None
    raw_candidate = None
    if content.startswith("{") and content.endswith("}"):
        raw_candidate = content
    else:
        brace_match = re.search(r"{.*}", content, flags=re.DOTALL)
        if brace_match:
            raw_candidate = brace_match.group(0)
    if not raw_candidate:
        return None
    try:
        return json.loads(raw_candidate)
    except Exception:
        try:
            unescaped = raw_candidate.encode("utf-8", errors="ignore").decode("unicode_escape")
            return json.loads(unescaped)
        except Exception:
            return None


def _summarize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    job_count = len(jobs) if isinstance(jobs, list) else 0
    summary = {
        "jobs_count": job_count,
        "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
    }
    for key in ("total", "count", "page", "per_page", "pageSize", "page_size"):
        if key in payload:
            summary[key] = payload.get(key)
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if isinstance(meta, dict):
        summary["meta_keys"] = sorted(meta.keys())
        for key in ("total", "count", "page", "page_size", "per_page", "limit"):
            if key in meta:
                summary[f"meta.{key}"] = meta.get(key)
    return summary


@pytest.mark.asyncio
async def test_github_api_payload_is_html_wrapped_and_yields_more_than_10_jobs(monkeypatch) -> None:
    fixture_path = Path("tests/fixtures/github_careers_api_jobs_12.json")
    payload_full = json.loads(fixture_path.read_text(encoding="utf-8"))
    payload_default = {
        "jobs": payload_full["jobs"][:10],
        "count": 10,
        "totalCount": 10,
    }

    class FakeAsyncSpider:
        def __init__(self, api_key: str):  # noqa: D401
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def scrape_url(
            self,
            url: str,
            params: Dict[str, Any] | None = None,
            stream: bool = False,
            content_type: str | None = None,
        ):
            return [
                {"content": json.dumps(payload_default)},
                {"raw_html": f"<pre>{json.dumps(payload_full)}</pre>"},
            ]

    monkeypatch.setattr(f"{__name__}.AsyncSpider", FakeAsyncSpider)

    async with AsyncSpider(api_key=API_KEY) as client:
        response = await _collect_response(
            client.scrape_url(
                GITHUB_API_URL,
                params=SPIDER_PARAMS,
                stream=False,
                content_type="application/json",
            )
        )

    payload = _extract_payload(response)
    parsed_count = len(payload.get("jobs", [])) if isinstance(payload, dict) else 0

    html_payload: Optional[Dict[str, Any]] = None
    for text in _gather_strings(response):
        html_payload = _extract_json_from_html(text)
        if html_payload:
            break

    assert html_payload is not None, "Expected HTML-wrapped JSON payload but none was found."
    html_summary = _summarize_payload(html_payload)
    html_count = html_summary.get("jobs_count", 0)

    assert parsed_count <= 10, (
        "Current JSON extraction unexpectedly produced >10 jobs. "
        f"parsed_count={parsed_count}"
    )
    assert html_count > 10, (
        "HTML-wrapped API payload did not yield >10 jobs; check API response. "
        f"summary={html_summary}"
    )
