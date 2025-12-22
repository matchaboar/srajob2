from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Ensure repo root importable
sys.path.insert(0, os.path.abspath("."))

# Stub firecrawl dependency for tests that don't exercise it
import types

try:
    import firecrawl  # noqa: F401
    import firecrawl.v2.types  # noqa: F401
    import firecrawl.v2.utils.error_handler  # noqa: F401
except Exception:
    firecrawl_mod = types.ModuleType("firecrawl")
    firecrawl_mod.Firecrawl = type("Firecrawl", (), {})  # dummy class
    sys.modules.setdefault("firecrawl", firecrawl_mod)
    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
    firecrawl_v2_types.PaginationConfig = type("PaginationConfig", (), {})
    firecrawl_v2_types.ScrapeOptions = type("ScrapeOptions", (), {})
    sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
    sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)
    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils.error_handler = types.SimpleNamespace(
        PaymentRequiredError=type("PaymentRequiredError", (Exception,), {}),
        RequestTimeoutError=type("RequestTimeoutError", (Exception,), {}),
    )
    sys.modules.setdefault("firecrawl.v2.utils", firecrawl_v2_utils)
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
    firecrawl_v2_utils_error.PaymentRequiredError = firecrawl_v2_utils.error_handler.PaymentRequiredError
    firecrawl_v2_utils_error.RequestTimeoutError = firecrawl_v2_utils.error_handler.RequestTimeoutError
    sys.modules.setdefault("firecrawl.v2.utils.error_handler", firecrawl_v2_utils_error)

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows.activities.types import Site  # noqa: E402
import job_scrape_application.services.convex_client as convex_client  # noqa: E402


@pytest.fixture
def datadog_crawl_payload() -> Dict[str, Any]:
    fixture_path = Path("tests/job_scrape_application/fixtures/fetchfox_datadog_crawl.json")
    return json.loads(fixture_path.read_text())


def _dedupe(urls: List[str]) -> List[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


@pytest.mark.asyncio
async def test_fetchfox_crawl_queues_urls_and_passes_skip_list(monkeypatch, datadog_crawl_payload):
    # Capture crawl request body passed to FetchFox.crawl
    captured_request: Dict[str, Any] = {}

    class FakeFox:
        def __init__(self, api_key: str):
            self.api_key = api_key

        def crawl(self, payload: Dict[str, Any]):
            captured_request["payload"] = payload
            return datadog_crawl_payload

    # Ensure crawl path is used and API key is present
    monkeypatch.setattr(acts, "FetchFox", FakeFox)
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "test-key")

    seen_urls = ["https://careers.datadoghq.com/detail/7179866/?gh_jid=7179866"]
    queued_urls = [
        {"url": "https://careers.datadoghq.com/detail/6092052/?gh_jid=6092052", "status": "pending"},
    ]

    async def fake_fetch_seen(source_url: str, pattern: str | None):
        return seen_urls

    async def fake_filter_existing(urls: List[str]):
        return []

    async def fake_convex_query(name: str, args: Dict[str, Any]):
        if name == "router:listQueuedScrapeUrls":
            return queued_urls
        return []

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)
    monkeypatch.setattr(convex_client, "convex_query", fake_convex_query)

    site: Site = {
        "_id": "site-1",
        "url": "https://careers.datadoghq.com/all-jobs/?s=software%20developer",
        "pattern": "https://careers.datadoghq.com/detail/**",
    }

    result = await acts.crawl_site_fetchfox(site)

    hits = datadog_crawl_payload["results"]["hits"]
    unique_hits = _dedupe(hits)
    skip = set(seen_urls + [q["url"] for q in queued_urls])
    expected_urls = [u for u in unique_hits if u not in skip]

    assert result["items"]["job_urls"] == expected_urls
    assert result["items"]["rawUrls"] == unique_hits
    assert captured_request["payload"]["priority"]["skip"] == list(skip)


@pytest.mark.asyncio
async def test_store_scrape_enqueues_urls_when_no_jobs(monkeypatch, datadog_crawl_payload):
    # Use crawl payload but strip to minimal crawl response shape
    crawl_hits = datadog_crawl_payload["results"]["hits"]
    crawl_payload = {
        "provider": "fetchfox-crawl",
        "workflowName": "FetchfoxSpidercloud",
        "sourceUrl": "https://careers.datadoghq.com/all-jobs/?s=software%20developer",
        "pattern": "https://careers.datadoghq.com/detail/**",
        "items": {
            "provider": "spidercloud",
            "crawlProvider": "fetchfox",
            "job_urls": crawl_hits,
            "rawUrls": crawl_hits,
            "queued": False,
            "queuedCount": 0,
            "existing": [],
            "request": {"url": "https://api.fetchfox.ai/crawl"},
            "seedUrls": ["https://careers.datadoghq.com/all-jobs/?s=software%20developer"],
        },
        "skippedUrls": [],
        "response": {"queued": 0, "urls": crawl_hits[:25], "totalUrls": len(crawl_hits)},
    }

    calls: list[tuple[str, Dict[str, Any]]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        calls.append((name, args))
        if name == "router:insertScrapeRecord":
            return "scrape-123"
        return {"queued": args.get("urls", [])}

    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    await acts.store_scrape(crawl_payload)

    # We should insert the scrape record and enqueue all URLs to spidercloud
    mutation_names = [name for name, _ in calls]
    assert "router:insertScrapeRecord" in mutation_names
    enqueue_calls = [args for name, args in calls if name == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "Expected enqueueScrapeUrls to be called"
    enqueued_urls = enqueue_calls[-1]["urls"]
    assert len(enqueued_urls) == len(_dedupe(crawl_hits))
    assert set(enqueued_urls) == set(_dedupe(crawl_hits))

    # No jobs were ingested; this ensures URLs without matching keywords later get marked via queue rather than dropped
    assert "router:ingestJobsFromScrape" not in mutation_names


@pytest.mark.asyncio
async def test_fetchfox_crawl_omits_invalid_site_id(monkeypatch, datadog_crawl_payload):
    captured_queries: list[Dict[str, Any]] = []
    captured_mutations: list[Dict[str, Any]] = []

    class FakeFox:
        def __init__(self, api_key: str):
            self.api_key = api_key

        def crawl(self, payload: Dict[str, Any]):
            return datadog_crawl_payload

    monkeypatch.setattr(acts, "FetchFox", FakeFox)
    monkeypatch.setattr(acts.settings, "fetchfox_api_key", "test-key")

    async def fake_fetch_seen(source_url: str, pattern: str | None):
        return []

    async def fake_filter_existing(urls: List[str]):
        return []

    async def fake_convex_query(name: str, args: Dict[str, Any]):
        captured_queries.append({"name": name, "args": args})
        return []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        captured_mutations.append({"name": name, "args": args})
        return {"queued": args.get("urls", [])}

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)
    monkeypatch.setattr(convex_client, "convex_query", fake_convex_query)
    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    site: Site = {
        "_id": "site-1",
        "url": "https://careers.datadoghq.com/all-jobs/?s=software%20developer",
        "pattern": "https://careers.datadoghq.com/detail/**",
    }

    await acts.crawl_site_fetchfox(site)

    assert captured_queries, "listQueuedScrapeUrls should be invoked"
    assert all("siteId" not in q["args"] for q in captured_queries if q["name"] == "router:listQueuedScrapeUrls")

    enqueue_calls = [m for m in captured_mutations if m["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls
    assert all("siteId" not in call["args"] for call in enqueue_calls)
