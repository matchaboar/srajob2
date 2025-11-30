from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_collect_firecrawl_handles_status_exception(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            self.api_key = api_key

        def get_batch_scrape_status(self, job_id: str, pagination_config: Any = None):
            raise RuntimeError("boom")

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    event: acts.FirecrawlWebhookEvent = {
        "id": "job-err",
        "jobId": "job-err",
        "type": "completed",
        "success": True,
        "data": [],
        "metadata": {},
    }

    with pytest.raises(acts.ApplicationError):
        await acts.collect_firecrawl_job_result(event)


@pytest.mark.asyncio
async def test_collect_firecrawl_logs_429(monkeypatch, caplog):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            self.api_key = api_key

        def get_batch_scrape_status(self, job_id: str, pagination_config: Any = None):
            raise acts.ApplicationError("429 Too Many Requests")

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    event: acts.FirecrawlWebhookEvent = {
        "id": "job-429",
        "jobId": "job-429",
        "type": "completed",
        "success": True,
        "data": [],
        "metadata": {},
    }

    caplog.set_level(logging.WARNING, logger="temporal.worker.activities")

    with pytest.raises(acts.ApplicationError):
        await acts.collect_firecrawl_job_result(event)

    messages = [rec.message for rec in caplog.records if "collect_firecrawl_job_result" in rec.message]
    assert any("429" in msg for msg in messages), "Expected 429 error to be logged"


@pytest.mark.asyncio
async def test_collect_firecrawl_skips_expired_jobs(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")

    # If the job is older than 24h, we should not ask Firecrawl for status.
    class ExplosiveFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            raise AssertionError("Firecrawl should not be instantiated for expired jobs")

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", ExplosiveFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    old_ms = int(time.time() * 1000) - acts.FIRECRAWL_STATUS_EXPIRATION_MS - 1000
    event: acts.FirecrawlWebhookEvent = {
        "id": "job-old",
        "jobId": "job-old",
        "type": "completed",
        "success": True,
        "data": [],
        "metadata": {"queuedAt": old_ms, "siteId": "s1", "siteUrl": "https://example.com"},
    }

    res = await acts.collect_firecrawl_job_result(event)

    assert res["status"] == "cancelled_expired"
    assert res["jobsScraped"] == 0


@pytest.mark.asyncio
async def test_collect_firecrawl_404_after_23h_cancels(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            self.api_key = api_key

        def get_batch_scrape_status(self, job_id: str, pagination_config: Any = None):
            raise RuntimeError("404 Not Found")

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    old_ms = int(time.time() * 1000) - acts.FIRECRAWL_STATUS_WARN_MS - 1000
    event: acts.FirecrawlWebhookEvent = {
        "id": "job-warn",
        "jobId": "job-warn",
        "type": "completed",
        "success": True,
        "data": [],
        "metadata": {"queuedAt": old_ms, "siteId": "s1", "siteUrl": "https://example.com"},
    }

    res = await acts.collect_firecrawl_job_result(event)

    assert res["status"] == "cancelled_expired"
    assert res["jobsScraped"] == 0
