from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

import pytest

# Ensure repo root is importable for job_scrape_application
sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_scrape_site_prefers_firecrawl_when_available(monkeypatch):
    site = {"_id": "1", "url": "https://example.com", "pattern": None}
    calls = {"fire": 0, "fetch": 0}

    async def fake_fire(site_arg: Dict[str, Any], skip_urls: list[str] | None = None) -> Dict[str, Any]:
        calls["fire"] += 1
        return {"ok": "fire"}

    async def fake_fetch(site_arg: Dict[str, Any]) -> Dict[str, Any]:
        calls["fetch"] += 1
        return {"ok": "fetch"}

    monkeypatch.setattr(acts, "scrape_site_firecrawl", fake_fire)
    monkeypatch.setattr(acts, "scrape_site_fetchfox", fake_fetch)
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test-key")
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "ff-test-key")

    res = await acts.scrape_site(site)

    assert res["ok"] == "fire"
    assert calls["fire"] == 1
    assert calls["fetch"] == 0


@pytest.mark.asyncio
async def test_scrape_site_firecrawl_normalizes_jobs(monkeypatch):
    site = {
        "_id": "site-123",
        "url": "https://example.com/jobs",
        "pattern": "https://example.com/jobs/**",
    }

    # Skip any network calls for seen URLs
    async def _skip_seen(*_args, **_kwargs):
        return []

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", _skip_seen)
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test-key")
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", None)

    calls: Dict[str, Any] = {}

    class FakeDoc:
        def __init__(self, rows: List[Dict[str, Any]]):
            self.rows = rows

        def model_dump(self, mode: str = "json", exclude_none: bool = True) -> Dict[str, Any]:
            return {"json": self.rows}

    class FakeJob:
        def __init__(self, docs: List[FakeDoc]):
            self.data = docs

        def model_dump(self, mode: str = "json", exclude_none: bool = True) -> Dict[str, Any]:
            return {"data": [doc.model_dump(mode=mode, exclude_none=exclude_none) for doc in self.data]}

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            calls["api_key"] = api_key
            calls["api_url"] = api_url

        def crawl(self, url: str, **kwargs: Any) -> FakeJob:
            calls["url"] = url
            calls["kwargs"] = kwargs
            rows = [
                {
                    "job_title": "Sr Software Engineer",
                    "company": "ACME Corp",
                    "location": "Remote",
                    "url": "https://example.com/jobs/123",
                    "salary": "200000",
                }
            ]
            return FakeJob([FakeDoc(rows)])

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    result = await acts.scrape_site_firecrawl(site)

    normalized = result["items"]["normalized"]
    assert len(normalized) == 1
    row = normalized[0]
    assert row["company"] == "ACME Corp"
    assert row["url"] == "https://example.com/jobs/123"
    assert row["remote"] is True  # derived from location
    assert isinstance(row["total_compensation"], int)

    # Ensure crawl was invoked with the pattern and limit settings we expect
    assert calls["url"] == site["url"]
    kwargs = calls["kwargs"]
    assert kwargs["include_paths"] == [site["pattern"]]
    assert kwargs["limit"] == acts.MAX_FIRECRAWL_VISITS
