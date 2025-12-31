from __future__ import annotations

import os
import sys
from typing import Any, Dict

import pytest

# Ensure repo root is importable for job_scrape_application
sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_scrape_site_prefers_fetchfox_when_set(monkeypatch):
    site = {"_id": "1", "url": "https://example.com", "pattern": None, "scrapeProvider": "fetchfox"}
    calls = {"fire": 0, "fetch": 0}

    async def fake_fire(self, site_arg: Dict[str, Any], skip_urls: list[str] | None = None) -> Dict[str, Any]:
        calls["fire"] += 1
        return {"ok": "fire"}

    async def fake_fetch(self, site_arg: Dict[str, Any], skip_urls: list[str] | None = None) -> Dict[str, Any]:
        calls["fetch"] += 1
        assert skip_urls is None
        return {"ok": "fetch"}

    monkeypatch.setattr(acts.FirecrawlScraper, "scrape_site", fake_fire)
    monkeypatch.setattr(acts.FetchfoxScraper, "scrape_site", fake_fetch)
    monkeypatch.setattr(acts.settings, "enable_firecrawl", True)
    monkeypatch.setattr(acts.settings, "enable_fetchfox", True)
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test-key")
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "ff-test-key")

    res = await acts.scrape_site(site)

    assert res["ok"] == "fetch"
    assert calls["fire"] == 0
    assert calls["fetch"] == 1


@pytest.mark.asyncio
async def test_scrape_site_uses_firecrawl_provider(monkeypatch):
    site = {"_id": "1", "url": "https://example.com", "pattern": None, "scrapeProvider": "firecrawl"}
    calls = {"fire": 0, "fetch": 0}

    async def fake_fire(self, site_arg: Dict[str, Any], skip_urls: list[str] | None = None) -> Dict[str, Any]:
        calls["fire"] += 1
        assert skip_urls == ["seen-one"]
        return {"ok": "fire"}

    async def fake_fetch(self, site_arg: Dict[str, Any], skip_urls: list[str] | None = None) -> Dict[str, Any]:
        calls["fetch"] += 1
        return {"ok": "fetch"}

    async def fake_seen(url: str, pattern: str | None) -> list[str]:
        return ["seen-one"]

    monkeypatch.setattr(acts.FirecrawlScraper, "scrape_site", fake_fire)
    monkeypatch.setattr(acts.FetchfoxScraper, "scrape_site", fake_fetch)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)
    monkeypatch.setattr(acts.settings, "enable_firecrawl", True)
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test-key")
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "ff-test-key")

    res = await acts.scrape_site(site)

    assert res["ok"] == "fire"
    assert calls["fire"] == 1
    assert calls["fetch"] == 0


@pytest.mark.asyncio
async def test_scrape_site_firecrawl_normalizes_jobs(monkeypatch):
    site = {"_id": "site-123", "url": "https://example.com/jobs", "pattern": "https://example.com/jobs/**"}

    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test-key")

    async def fake_start(site_arg):
        return {"jobId": "job-999", "statusUrl": "https://status/job-999"}

    monkeypatch.setattr(acts, "start_firecrawl_webhook_scrape", fake_start)

    result = await acts.scrape_site_firecrawl(site)

    assert result["items"]["queued"] is True
    assert result["items"]["jobId"] == "job-999"
    assert result["items"]["statusUrl"] == "https://status/job-999"
    assert result["items"]["normalized"] == []
