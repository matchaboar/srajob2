from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.testing.firecrawl_mock import (  # noqa: E402
    MockFirecrawl,
    MockFirecrawlScenario,
    MockWebhookQueue,
)


@pytest.mark.asyncio
async def test_mock_firecrawl_success_sends_webhook():
    queue = MockWebhookQueue()
    client = MockFirecrawl(webhook_queue=queue, webhook_delay=0.01)
    webhook = SimpleNamespace(
        url="https://demo.convex.site/api/firecrawl/webhook",
        metadata={"siteId": "site-1", "siteUrl": "https://example.com"},
    )

    job = client.start_crawl("https://example.com", webhook=webhook)

    assert job.jobId == job.job_id
    assert await queue.wait_for(1, timeout=0.2)
    events = queue.drain()
    assert events[0]["jobId"] == job.job_id
    assert events[0]["metadata"]["siteId"] == "site-1"


@pytest.mark.asyncio
async def test_mock_firecrawl_callable_returns_200_and_sends_webhook():
    queue = MockWebhookQueue()
    client = MockFirecrawl(webhook_queue=queue, webhook_delay=0.01)

    resp = client(
        site_url="https://example.com",
        webhook={
            "url": "https://demo.convex.site/api/firecrawl/webhook",
            "metadata": {"siteId": "s-call"},
        },
    )

    assert resp.status_code == 200
    assert resp.json()["jobId"]
    assert await queue.wait_for(1, timeout=0.2)
    events = queue.drain()
    assert events[0]["metadata"]["siteId"] == "s-call"


def test_mock_firecrawl_start_failure():
    client = MockFirecrawl(scenario=MockFirecrawlScenario.START_FAILS)
    with pytest.raises(RuntimeError):
        client.start_crawl("https://example.com", webhook=None)


def test_mock_firecrawl_callable_returns_500_on_failure():
    client = MockFirecrawl(scenario=MockFirecrawlScenario.START_FAILS)

    resp = client(site_url="https://example.com")

    assert resp.status_code == 500
    assert "error" in resp.json()


@pytest.mark.asyncio
async def test_mock_firecrawl_webhook_failure_recorded():
    queue = MockWebhookQueue()
    client = MockFirecrawl(
        scenario=MockFirecrawlScenario.WEBHOOK_POST_FAILS,
        webhook_queue=queue,
        webhook_delay=0.01,
    )

    webhook = {
        "url": "https://demo.convex.site/api/firecrawl/webhook",
        "metadata": {"siteId": "s2"},
    }
    client.start_crawl("https://example.com", webhook=webhook)

    await asyncio.sleep(0.05)
    assert queue.drain() == []
    assert client.webhook_failures


@pytest.mark.asyncio
async def test_mock_firecrawl_callable_webhook_failure_still_returns_200():
    queue = MockWebhookQueue()
    client = MockFirecrawl(
        scenario=MockFirecrawlScenario.WEBHOOK_POST_FAILS,
        webhook_queue=queue,
        webhook_delay=0.01,
    )

    resp = client(
        site_url="https://example.com",
        webhook={"url": "https://demo.convex.site/api/firecrawl/webhook"},
    )

    assert resp.status_code == 200
    await asyncio.sleep(0.05)
    assert queue.drain() == []
    assert client.webhook_failures
