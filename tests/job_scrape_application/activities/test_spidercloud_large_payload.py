from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities


class _StubSpidercloud:
    def __init__(self, raw_payload: str) -> None:
        self.raw_payload = raw_payload

    async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        urls = [u for u in payload.get("urls", []) if isinstance(u, str)]
        normalized = [
            {"url": url, "title": "Engineer", "description": "Example description"} for url in urls
        ]
        raw_items = [self.raw_payload for _ in urls]
        base_payload = {
            "sourceUrl": payload.get("source_url") or (urls[0] if urls else ""),
            "startedAt": 0,
            "completedAt": 1,
            "items": {"normalized": normalized, "raw": raw_items, "provider": "spidercloud"},
            "provider": "spidercloud",
        }
        return {"scrape": base_payload}


def _batch_for_urls(urls: List[str]) -> Dict[str, Any]:
    return {
        "urls": [
            {"url": url, "sourceUrl": "https://jobs.ashbyhq.com/lambda", "pattern": None}
            for url in urls
        ]
    }


@pytest.mark.asyncio
async def test_spidercloud_job_batch_payload_is_reasonably_capped(monkeypatch):
    raw_html = "<html>" + ("x" * (1024 * 1024)) + "</html>"
    monkeypatch.setattr(activities, "_make_spidercloud_scraper", lambda: _StubSpidercloud(raw_html))

    batch = _batch_for_urls(["https://jobs.ashbyhq.com/lambda/1"])
    result = await activities.process_spidercloud_job_batch(batch)

    scrapes = result.get("scrapes") or []
    assert scrapes, "expected at least one scrape payload"
    payload_size = len(json.dumps(scrapes[0]).encode("utf-8"))

    # Expect detail payloads to be aggressively capped to avoid worker OOMs.
    assert payload_size <= 256_000


@pytest.mark.asyncio
async def test_spidercloud_job_batch_total_payload_is_reasonably_capped(monkeypatch):
    raw_html = "<html>" + ("y" * (512 * 1024)) + "</html>"
    monkeypatch.setattr(activities, "_make_spidercloud_scraper", lambda: _StubSpidercloud(raw_html))

    urls = [f"https://jobs.ashbyhq.com/lambda/{i}" for i in range(10)]
    batch = _batch_for_urls(urls)
    result = await activities.process_spidercloud_job_batch(batch)

    total_size = len(json.dumps(result).encode("utf-8"))

    # Expect total batch payloads to remain small enough for Temporal history limits.
    assert total_size <= 1_000_000
