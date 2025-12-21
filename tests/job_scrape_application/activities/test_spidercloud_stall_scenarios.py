from __future__ import annotations

import os
import sys
import types
from typing import Any, Dict, List

import pytest

firecrawl_mod = types.ModuleType("firecrawl")
firecrawl_mod.Firecrawl = type("Firecrawl", (), {})
sys.modules.setdefault("firecrawl", firecrawl_mod)
firecrawl_v2 = types.ModuleType("firecrawl.v2")
firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
firecrawl_v2_types.PaginationConfig = type("PaginationConfig", (), {})
firecrawl_v2_types.ScrapeOptions = type("ScrapeOptions", (), {})
sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)
firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
firecrawl_v2_utils_error.PaymentRequiredError = type("PaymentRequiredError", (Exception,), {})
firecrawl_v2_utils_error.RequestTimeoutError = type("RequestTimeoutError", (Exception,), {})
sys.modules.setdefault("firecrawl.v2.utils", firecrawl_v2_utils)
sys.modules.setdefault("firecrawl.v2.utils.error_handler", firecrawl_v2_utils_error)
firecrawl_v2_utils.error_handler = firecrawl_v2_utils_error
fetchfox_mod = types.ModuleType("fetchfox_sdk")
fetchfox_mod.FetchFox = type("FetchFox", (), {})
sys.modules.setdefault("fetchfox_sdk", fetchfox_mod)

try:
    import temporalio  # noqa: F401
except ImportError:  # pragma: no cover
    temporalio = types.ModuleType("temporalio")
    sys.modules.setdefault("temporalio", temporalio)

    class _Activity:
        def defn(self, fn=None, **kwargs):
            if fn is None:
                def wrapper(func):
                    return func

                return wrapper
            return fn

    temporalio.activity = _Activity()
    sys.modules.setdefault("temporalio.activity", temporalio)

    temporalio_exceptions = types.ModuleType("temporalio.exceptions")
    temporalio_exceptions.ApplicationError = type("ApplicationError", (Exception,), {})
    sys.modules.setdefault("temporalio.exceptions", temporalio_exceptions)

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


class _FakeFetchFox:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.last_request: Dict[str, Any] | None = None

    def crawl(self, request: Dict[str, Any]) -> Dict[str, Any]:
        self.last_request = request
        return {"items": []}


@pytest.mark.asyncio
async def test_fetchfox_should_not_skip_stale_processing_urls(monkeypatch):
    """
    Prod symptom: URLs remain stuck in processing for hours, and subsequent site
    crawls keep skipping them. We want stale processing rows to be eligible for
    re-queueing even if they still appear in the queue.
    """

    now_ms = 1_700_000_000_000
    stale_ms = now_ms - (3 * 60 * 60 * 1000)

    async def fake_convex_query(name: str, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        if name == "router:listQueuedScrapeUrls":
            status = args.get("status")
            if status == "processing":
                return [
                    {
                        "url": "https://example.com/detail/stale-1",
                        "status": "processing",
                        "updatedAt": stale_ms,
                    }
                ]
            return []
        return []

    async def fake_fetch_seen(_source: str, _pattern: str | None):
        return []

    fake_fetchfox = _FakeFetchFox("fake")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_convex_query)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)
    monkeypatch.setattr(acts, "FetchFox", lambda api_key: fake_fetchfox)
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "fake-key")
    monkeypatch.setattr(acts.time, "time", lambda: now_ms / 1000)

    site = {"_id": "site-1", "url": "https://example.com/jobs", "pattern": None}
    await acts.crawl_site_fetchfox(site)

    assert fake_fetchfox.last_request is not None
    skip_list = fake_fetchfox.last_request["priority"]["skip"]

    # Expectation: stale processing URLs should not be skipped (so they can be re-queued).
    # Current behavior: they ARE skipped, so this assertion fails and describes the prod stall.
    assert "https://example.com/detail/stale-1" not in skip_list


@pytest.mark.asyncio
async def test_fetchfox_should_only_skip_recent_processing_urls(monkeypatch):
    """
    Prod scenario: Fresh processing rows should be skipped, but stale ones should be retried.
    """

    now_ms = 1_700_000_000_000
    stale_ms = now_ms - (3 * 60 * 60 * 1000)
    fresh_ms = now_ms - (2 * 60 * 1000)

    async def fake_convex_query(name: str, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        if name == "router:listQueuedScrapeUrls" and args.get("status") == "processing":
            return [
                {
                    "url": "https://example.com/detail/stale-2",
                    "status": "processing",
                    "updatedAt": stale_ms,
                },
                {
                    "url": "https://example.com/detail/fresh-1",
                    "status": "processing",
                    "updatedAt": fresh_ms,
                },
            ]
        return []

    async def fake_fetch_seen(_source: str, _pattern: str | None):
        return []

    fake_fetchfox = _FakeFetchFox("fake")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_convex_query)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)
    monkeypatch.setattr(acts, "FetchFox", lambda api_key: fake_fetchfox)
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "fake-key")
    monkeypatch.setattr(acts.time, "time", lambda: now_ms / 1000)

    site = {"_id": "site-1", "url": "https://example.com/jobs", "pattern": None}
    await acts.crawl_site_fetchfox(site)

    assert fake_fetchfox.last_request is not None
    skip_list = fake_fetchfox.last_request["priority"]["skip"]

    # Expectation: fresh processing rows are skipped; stale ones are retried.
    # Current behavior: both are skipped, so this should fail.
    assert "https://example.com/detail/fresh-1" in skip_list
    assert "https://example.com/detail/stale-2" not in skip_list
