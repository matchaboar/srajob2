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
async def test_fail_listing_batch_urls_marks_failed(monkeypatch):
    batch = {
        "urls": [
            {
                "url": "https://example.com/jobs?page=1",
                "sourceUrl": "https://example.com/jobs",
                "pattern": None,
                "_id": "01hzconvexqueueidlisting00000001",
                "provider": "spidercloud",
                "siteId": "01hzconvexsiteid000000000001",
                "attempts": 2.5,
            },
            {
                "url": "https://example.com/jobs?page=2",
                "_id": "01hzconvexqueueidlisting00000002",
            },
        ]
    }

    mutation_calls: List[Dict[str, Any]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        mutation_calls.append({"name": name, "args": args})
        if name == "router:completeScrapeUrls":
            return {"updated": len(args.get("items") or [])}
        raise RuntimeError(f"unexpected mutation {name}")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)

    res = await acts.fail_listing_batch_urls(batch, "batch_failed")

    assert res["updated"] == 2
    call = mutation_calls[0]
    assert call["name"] == "router:completeScrapeUrls"
    assert call["args"]["status"] == "failed"
    assert call["args"]["error"] == "batch_failed"
    items = call["args"]["items"]
    assert all(item.get("isListingUrl") is True for item in items)
    assert items[0]["attempts"] == 2
    assert items[0]["provider"] == "spidercloud"
    assert items[0]["siteId"] == "01hzconvexsiteid000000000001"


@pytest.mark.asyncio
async def test_fail_listing_batch_urls_noop_when_empty(monkeypatch):
    calls: list[dict[str, Any]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        return {"updated": 1}

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)

    res = await acts.fail_listing_batch_urls({"urls": []})

    assert res == {"updated": 0}
    assert calls == []
