from __future__ import annotations

import os
import sys
from typing import Any

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_propagates_firecrawl_error(monkeypatch):
    site = {"_id": "s1", "url": "https://example.com"}
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    class BadFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            pass

        def start_batch_scrape(self, urls, **kwargs: Any):
            raise RuntimeError("firecrawl boom")

    async def fake_seen(*_args, **_kwargs):
        return []

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)
    monkeypatch.setattr(acts, "Firecrawl", BadFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    with pytest.raises(acts.ApplicationError):
        await acts.start_firecrawl_webhook_scrape(site)


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_uses_site_type(monkeypatch):
    site = {"_id": "sgh", "url": "https://example.com/gh", "type": "greenhouse"}
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    calls = {"batch": 0}

    class FakeJob:
        jobId = "gh-1"
        statusUrl = "https://status/gh-1"

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            pass

        def start_batch_scrape(self, urls, **kwargs):
            calls["batch"] += 1
            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.start_firecrawl_webhook_scrape(site)

    assert res["kind"] == "greenhouse_listing"
    assert calls["batch"] == 1


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_flattens_metadata(monkeypatch):
    site = {
        "_id": "sgreen",
        "url": "https://api.greenhouse.io/v1/boards/demo/jobs",
        "type": "greenhouse",
    }

    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    captured: dict[str, Any] = {}

    class FakeJob:
        jobId = "job-123"
        statusUrl = "https://status/job-123"

    class StrictFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            pass

        def start_batch_scrape(self, urls, **kwargs):
            webhook_arg = kwargs.get("webhook")
            captured["urls"] = urls
            captured["webhook"] = webhook_arg
            payload = webhook_arg.model_dump(exclude_none=True)
            metadata = payload.get("metadata", {})
            captured["metadata"] = metadata

            if "seedUrls" in metadata:
                raise RuntimeError("metadata.seedUrls should not be sent to Firecrawl")
            if isinstance(metadata.get("urls"), list):
                raise RuntimeError("metadata.urls must be string for Firecrawl")

            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    async def fake_record_pending(*_args, **_kwargs):
        return "webhook-123"

    monkeypatch.setattr(acts, "Firecrawl", StrictFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(acts, "_record_pending_firecrawl_webhook", fake_record_pending)

    res = await acts.start_firecrawl_webhook_scrape(site)

    meta = captured["metadata"]
    assert "seedUrls" not in meta
    assert isinstance(meta.get("urls"), str)
    assert "https://api.greenhouse.io/v1/boards/demo/jobs" in meta.get("urls", "")
    provider_req = res.get("providerRequest")
    assert isinstance(provider_req, dict)
    webhook_meta = provider_req.get("webhook", {}).get("metadata", {})
    assert "seedUrls" not in webhook_meta
    assert res["jobId"] == "job-123"
