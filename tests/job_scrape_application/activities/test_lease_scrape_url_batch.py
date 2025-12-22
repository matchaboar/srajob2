from __future__ import annotations

import os
import sys
import types
from typing import Any, Dict, List

import pytest

try:
    import firecrawl  # noqa: F401
    import firecrawl.v2.types  # noqa: F401
    import firecrawl.v2.utils.error_handler  # noqa: F401
except Exception:
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


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_filters_skip_and_marks_failed(monkeypatch):
    leased = {
        "urls": [
            {"url": "https://example.com/skip-me", "sourceUrl": "https://example.com", "pattern": None},
            {"url": "https://example.com/process-me", "sourceUrl": "https://example.com", "pattern": None},
        ]
    }

    mutation_calls: List[Dict[str, Any]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        mutation_calls.append({"name": name, "args": args})
        if name == "router:leaseScrapeUrlBatch":
            return leased
        if name == "router:completeScrapeUrls":
            return {"updated": len(args.get("urls") or [])}
        raise RuntimeError(f"unexpected mutation {name}")

    async def fake_fetch_seen(source_url: str, pattern: str | None):
        assert source_url == "https://example.com"
        return ["https://example.com/skip-me"]

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)

    res = await acts.lease_scrape_url_batch("spidercloud", 5)

    assert res["urls"] == [leased["urls"][1]]
    assert res["skippedUrls"] == ["https://example.com/skip-me"]

    skip_call = next(call for call in mutation_calls if call["name"] == "router:completeScrapeUrls")
    assert skip_call["args"]["urls"] == ["https://example.com/skip-me"]
    assert skip_call["args"]["status"] == "failed"
    assert "skip_listed_url" in (skip_call["args"].get("error") or "")


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_handles_non_dict_response(monkeypatch):
    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        if name == "router:leaseScrapeUrlBatch":
            return None
        return {}

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)

    res = await acts.lease_scrape_url_batch("spidercloud", 2)

    assert res == {"urls": [], "skippedUrls": []}


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_retries_when_all_skipped(monkeypatch):
    lease_payloads = [
        {
            "urls": [
                {"url": "https://example.com/skip-me", "sourceUrl": "https://example.com", "pattern": None},
            ]
        },
        {
            "urls": [
                {"url": "https://example.com/process-me", "sourceUrl": "https://example.com", "pattern": None},
            ]
        },
    ]
    mutation_calls: List[str] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        mutation_calls.append(name)
        if name == "router:leaseScrapeUrlBatch":
            return lease_payloads.pop(0) if lease_payloads else {"urls": []}
        if name == "router:completeScrapeUrls":
            return {"updated": len(args.get("urls") or [])}
        raise RuntimeError(f"unexpected mutation {name}")

    async def fake_fetch_seen(source_url: str, pattern: str | None):
        assert source_url == "https://example.com"
        return ["https://example.com/skip-me"]

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)

    res = await acts.lease_scrape_url_batch("spidercloud", 1)

    assert res["urls"] == [{"url": "https://example.com/process-me", "sourceUrl": "https://example.com", "pattern": None}]
    assert "https://example.com/skip-me" in res.get("skippedUrls", [])
    assert mutation_calls.count("router:leaseScrapeUrlBatch") == 2
