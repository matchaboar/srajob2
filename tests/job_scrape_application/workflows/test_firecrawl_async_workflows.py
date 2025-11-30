from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

import pytest

# Ensure repo root importable
sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_runs_via_to_thread(monkeypatch):
    site = {"_id": "s1", "url": "https://example.com", "pattern": "/jobs/**"}

    # Env + config
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    # Skip URL fetching
    async def fake_seen(url: str, pattern: str | None):
        return ["https://example.com/jobs/old"]

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)

    calls: Dict[str, Any] = {"to_thread": 0, "start_batch": None}

    class FakeJob:
        id = "job-123"
        status_url = "https://status.example.com/job-123"

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            calls["api_key"] = api_key
            calls["api_url"] = api_url

        def start_batch_scrape(self, urls: List[str], **kwargs: Any):
            calls["start_batch"] = {"urls": urls, "kwargs": kwargs}
            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        calls["to_thread"] += 1
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.start_firecrawl_webhook_scrape(site)

    assert res["jobId"] == "job-123"
    assert res["kind"] == "site_crawl"
    assert calls["to_thread"] == 1
    assert calls["start_batch"]
    assert calls["start_batch"]["urls"] == [site["url"]]
    kwargs = calls["start_batch"]["kwargs"]
    assert "webhook" in kwargs


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_greenhouse_batch(monkeypatch):
    site = {"_id": "s2", "url": "https://example.com/gh", "type": "greenhouse"}

    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    calls: Dict[str, Any] = {"to_thread": 0, "start_batch": None}

    class FakeJob:
        jobId = "gh-456"
        statusUrl = "https://status.example.com/gh-456"

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            calls["api_key"] = api_key
            calls["api_url"] = api_url

        def start_batch_scrape(self, urls: List[str], **kwargs: Any):
            calls["start_batch"] = {"urls": urls, "kwargs": kwargs}
            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        calls["to_thread"] += 1
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.start_firecrawl_webhook_scrape(site)

    assert res["jobId"] == "gh-456"
    assert res["kind"] == "greenhouse_listing"
    assert calls["to_thread"] == 1
    assert calls["start_batch"]["urls"] == [site["url"]]


@pytest.mark.asyncio
async def test_collect_firecrawl_job_result_async(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")

    calls: Dict[str, Any] = {"to_thread": 0, "get_status": 0}

    class FakeDoc:
        def model_dump(self, mode: str = "json", exclude_none: bool = True):
            return {"json": {"items": [{"job_title": "Engineer", "company": "ACME", "url": "https://e"}]}}

    class FakeStatus:
        status = "completed"
        data = [FakeDoc()]

        def model_dump(self, mode: str = "json", exclude_none: bool = True):
            return {"data": [doc.model_dump(mode=mode, exclude_none=exclude_none) for doc in self.data]}

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            calls["api_key"] = api_key
            calls["api_url"] = api_url

        def get_batch_scrape_status(self, job_id: str, pagination_config: Any = None):
            calls["get_status"] += 1
            calls["job_id"] = job_id
            return FakeStatus()

    async def fake_to_thread(func, *args, **kwargs):
        calls["to_thread"] += 1
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    event: acts.FirecrawlWebhookEvent = {
        "id": "job-789",
        "jobId": "job-789",
        "type": "completed",
        "success": True,
        "data": [],
        "metadata": {"siteId": "s3", "siteUrl": "https://example.com"},
    }

    res = await acts.collect_firecrawl_job_result(event)

    assert res["jobsScraped"] == 1
    assert res["scrape"]["items"]["normalized"][0]["company"] == "ACME"
    assert calls["to_thread"] == 1
    assert calls["get_status"] == 1
    assert calls["job_id"] == "job-789"


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_requires_api_key(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", None)
    site = {"_id": "s1", "url": "https://example.com"}

    with pytest.raises(acts.ApplicationError):
        await acts.start_firecrawl_webhook_scrape(site)


@pytest.mark.asyncio
async def test_collect_firecrawl_job_result_requires_job_id(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    event: acts.FirecrawlWebhookEvent = {
        "id": "",
        "type": "completed",
        "success": True,
        "data": [],
        "metadata": {},
    }

    with pytest.raises(acts.ApplicationError):
        await acts.collect_firecrawl_job_result(event)


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_wraps_webhook_for_batch(monkeypatch):
    site = {"_id": "s-ghi", "url": "https://example.com/gh", "type": "greenhouse"}
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    captured: Dict[str, Any] = {}

    class FakeJob:
        jobId = "gh-999"
        statusUrl = "https://status/gh-999"

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            captured["api_key"] = api_key
            captured["api_url"] = api_url

        def start_batch_scrape(self, urls, *, webhook=None, **kwargs):
            captured["urls"] = urls
            captured["webhook"] = webhook
            # Firecrawl expects a model_dump method; assert it's present
            assert hasattr(webhook, "model_dump")
            data = webhook.model_dump()
            assert data["url"].endswith("/api/firecrawl/webhook")
            assert "pattern" not in data["metadata"]  # should strip nulls
            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.start_firecrawl_webhook_scrape(site)

    assert res["jobId"] == "gh-999"
    assert captured["urls"] == [site["url"]]
    assert captured["webhook"] is not None


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_scrape_wraps_webhook_for_crawl(monkeypatch):
    site = {"_id": "s-crawl", "url": "https://example.com", "pattern": "/jobs/**"}
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    async def fake_seen(url: str, pattern: str | None):
        return []

    captured: Dict[str, Any] = {}

    class FakeJob:
        id = "crawl-321"
        status_url = "https://status/crawl-321"

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            captured["api_key"] = api_key

        def start_batch_scrape(self, urls: List[str], *, webhook=None, **kwargs):
            captured["urls"] = urls
            captured["webhook"] = webhook
            assert hasattr(webhook, "model_dump")
            data = webhook.model_dump()
            assert data["metadata"]["kind"] == "site_crawl"
            assert data["metadata"]["pattern"] == site["pattern"]
            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)
    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.start_firecrawl_webhook_scrape(site)

    assert res["jobId"] == "crawl-321"
    assert captured["urls"] == [site["url"]]
    assert captured["webhook"] is not None


@pytest.mark.asyncio
async def test_webhook_model_dump_excludes_none(monkeypatch):
    site = {"_id": "s-nil", "url": "https://example.com/gh", "type": "greenhouse", "pattern": None}
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    captured: Dict[str, Any] = {}

    class FakeJob:
        jobId = "gh-000"
        statusUrl = "https://status/gh-000"

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            captured["api_key"] = api_key
            captured["api_url"] = api_url

        def start_batch_scrape(self, urls, *, webhook=None, **kwargs):
            data_default = webhook.model_dump()
            data_strip = webhook.model_dump(exclude_none=True)
            captured["default"] = data_default
            captured["strip"] = data_strip
            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.start_firecrawl_webhook_scrape(site)

    assert res["jobId"] == "gh-000"
    # pattern was None, so should be omitted in both dumps
    assert "pattern" not in captured["default"]["metadata"]
    assert "pattern" not in captured["strip"]["metadata"]


@pytest.mark.asyncio
async def test_start_firecrawl_webhook_retries_model_dump_error(monkeypatch):
    site = {"_id": "s-retry", "url": "https://example.com/gh", "type": "greenhouse"}
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts.settings, "convex_http_url", "https://demo.convex.site")

    class FakeJob:
        jobId = "gh-retry"
        statusUrl = "https://status/gh-retry"

    class FlakyFirecrawl:
        instances: List["FlakyFirecrawl"] = []

        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            self.api_key = api_key
            self.api_url = api_url
            self.calls = 0
            self.webhooks: List[Any] = []
            FlakyFirecrawl.instances.append(self)

        def start_batch_scrape(self, urls, *, webhook=None, **kwargs):
            self.calls += 1
            self.webhooks.append(webhook)
            if self.calls == 1:
                raise AttributeError("'dict' object has no attribute 'model_dump'")
            return FakeJob()

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    FlakyFirecrawl.instances = []
    monkeypatch.setattr(acts, "Firecrawl", FlakyFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.start_firecrawl_webhook_scrape(site)

    client = FlakyFirecrawl.instances[0]
    assert client.calls == 2
    assert hasattr(client.webhooks[-1], "model_dump")
    assert res["jobId"] == "gh-retry"


@pytest.mark.asyncio
async def test_scrape_greenhouse_jobs_firecrawl_batches_urls(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    urls = ["https://jobs/1", "https://jobs/2", "https://jobs/3"]

    calls: Dict[str, Any] = {"batch": 0, "urls": None, "idempotency": None}

    class FakeResult:
        def __init__(self, data):
            self.data = data

        def model_dump(self, mode: str = "json", exclude_none: bool = True):
            return {"data": [d.model_dump(mode=mode, exclude_none=exclude_none) for d in self.data]}

    class FakeDoc:
        def __init__(self, idx: int):
            self.idx = idx

        def model_dump(self, mode: str = "json", exclude_none: bool = True):
            return {
                "json": {
                    "items": [
                        {
                            "job_title": f"Engineer Role {self.idx}",
                            "company": "ACME",
                            "url": f"https://jobs/{self.idx}",
                        }
                    ]
                }
            }

    class FakeFirecrawl:
        def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
            calls["api_key"] = api_key

        def batch_scrape(self, urls_arg, **kwargs):
            calls["batch"] += 1
            calls["urls"] = urls_arg
            calls["idempotency"] = kwargs.get("idempotency_key")
            return FakeResult([FakeDoc(i) for i in range(len(urls_arg))])

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", fake_to_thread)

    res = await acts.scrape_greenhouse_jobs(
        {"urls": urls, "source_url": "https://board", "idempotency_key": "webhook-123"}
    )

    assert calls["batch"] == 1
    assert calls["urls"] == urls
    assert calls["idempotency"] == "webhook-123"
    assert res["jobsScraped"] == 3
