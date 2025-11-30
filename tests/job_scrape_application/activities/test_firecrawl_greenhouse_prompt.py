from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities  # noqa: E402


class _FakeFirecrawl:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.calls: List[Dict[str, Any]] = []

    # Emulate start_batch_scrape signature used in activities
    def start_batch_scrape(self, urls, formats=None, webhook=None, proxy=None, max_age=None, store_in_cache=None):  # noqa: D401, ANN001
        self.calls.append(
            {
                "urls": urls,
                "formats": formats,
                "webhook": webhook,
                "proxy": proxy,
                "max_age": max_age,
                "store_in_cache": store_in_cache,
            }
        )
        return type("Job", (), {"id": "job-123", "status_url": "https://status"})()


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_uses_json_prompt_for_greenhouse(monkeypatch):
    fake_client_holder: Dict[str, _FakeFirecrawl] = {}

    def fake_firecrawl(api_key: str):  # type: ignore[override]
        client = _FakeFirecrawl(api_key)
        fake_client_holder["client"] = client
        return client

    monkeypatch.setattr(activities, "Firecrawl", fake_firecrawl)

    # Ensure env/config present
    monkeypatch.setenv("FIRECRAWL_API_KEY", "test-key")
    monkeypatch.setenv("CONVEX_HTTP_URL", "https://example.convex.site")
    monkeypatch.setattr(activities.settings, "firecrawl_api_key", "test-key")
    monkeypatch.setattr(activities.settings, "convex_http_url", "https://example.convex.site")
    monkeypatch.setattr(activities.settings, "convex_url", None)

    site = {
        "_id": "site-1",
        "url": "https://api.greenhouse.io/v1/boards/demo/jobs",
        "type": "greenhouse",
    }

    res = await activities.start_firecrawl_webhook_scrape(site)

    client = fake_client_holder["client"]
    assert client.api_key == "test-key"
    assert client.calls, "Firecrawl start_batch_scrape should be called"
    call = client.calls[0]

    fmt = call["formats"][0]
    assert fmt["type"] == "json"
    assert "Return the full Greenhouse board JSON payload" in fmt["prompt"]
    assert fmt["schema"]["required"] == ["jobs"]

    # Webhook config should target convex .site domain and include events
    webhook = call["webhook"].model_dump(exclude_none=True)  # type: ignore[arg-type]
    assert webhook["events"] == ["completed", "failed"]
    assert webhook["url"].endswith("/api/firecrawl/webhook")

    # Returned payload should carry job id and kind
    assert res["jobId"] == "job-123"
    assert res["kind"] == "greenhouse_listing"
