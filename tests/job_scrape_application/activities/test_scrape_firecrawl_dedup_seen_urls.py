from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities


class _FirecrawlCatcher:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def start_batch_scrape(self, urls: List[str], **kwargs: Any):  # noqa: D401, ANN001
        # Firecrawl client signature used by start_firecrawl_webhook_scrape
        self.calls.append({"urls": urls, **kwargs})
        return type("Job", (), {"id": "job-abc", "status_url": "https://status"})()


@pytest.mark.asyncio
async def test_start_firecrawl_respects_seen_urls_from_store(monkeypatch):
    catcher = _FirecrawlCatcher()

    # Worker has no memory; dedup relies on Convex store
    async def _fetch_seen(url: str, pattern: str | None):
        return ["https://example.com/old"]

    monkeypatch.setattr(activities, "fetch_seen_urls_for_site", _fetch_seen)
    monkeypatch.setattr(activities, "Firecrawl", lambda api_key=None: catcher)

    # Required config
    monkeypatch.setenv("FIRECRAWL_API_KEY", "key")
    monkeypatch.setenv("CONVEX_HTTP_URL", "https://demo.convex.site")
    monkeypatch.setattr(activities.settings, "firecrawl_api_key", "key")
    monkeypatch.setattr(activities.settings, "convex_http_url", "https://demo.convex.site")
    monkeypatch.setattr(activities.settings, "convex_url", None)

    site = {"_id": "site-1", "url": "https://example.com/jobs", "type": "general", "pattern": "/jobs"}

    res = await activities.start_firecrawl_webhook_scrape(site)

    assert res["jobId"] == "job-abc"
    assert catcher.calls, "Firecrawl start_batch_scrape should be invoked"
    call = catcher.calls[0]
    assert call.get("urls") == [site["url"]]
    assert "webhook" in call
