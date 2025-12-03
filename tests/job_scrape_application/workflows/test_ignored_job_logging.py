from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Tuple

import pytest

# Ensure repo root importable
sys.path.insert(0, os.path.abspath("."))

# Stub firecrawl dependency for tests that don't exercise it
import types

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
import job_scrape_application.services.convex_client as convex_client  # noqa: E402


@pytest.mark.asyncio
async def test_store_scrape_logs_ignored_with_title_and_description(monkeypatch):
    calls: List[Tuple[str, Dict[str, Any]]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        calls.append((name, args))
        if name == "router:insertScrapeRecord":
            return "scrape-ignored-1"
        return {"queued": args.get("urls", [])}

    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    payload = {
        "provider": "spidercloud",
        "workflowName": "SpidercloudJobDetails",
        "sourceUrl": "https://example.com/list",
        "items": {
          "provider": "spidercloud",
          "normalized": [],
          "ignored": [
            {
              "url": "https://example.com/jobs/1",
              "reason": "missing_required_keyword",
              "title": "Product Manager",
              "description": "raw description",
            }
          ]
        },
    }

    await acts.store_scrape(payload)

    inserted = [args for name, args in calls if name == "router:insertIgnoredJob"]
    assert inserted, "Expected insertIgnoredJob to be called"
    assert inserted[0]["title"] == "Product Manager"
    assert inserted[0]["description"] == "raw description"


@pytest.mark.asyncio
async def test_store_scrape_logs_ignored_with_unknown_title(monkeypatch):
    calls: List[Tuple[str, Dict[str, Any]]] = []

    async def fake_convex_mutation(name: str, args: Dict[str, Any]):
        calls.append((name, args))
        if name == "router:insertScrapeRecord":
            return "scrape-ignored-2"
        return {"queued": args.get("urls", [])}

    monkeypatch.setattr(convex_client, "convex_mutation", fake_convex_mutation)

    payload = {
        "provider": "spidercloud",
        "workflowName": "SpidercloudJobDetails",
        "sourceUrl": "https://example.com/list",
        "items": {
          "provider": "spidercloud",
          "normalized": [],
          "ignored": [
            {
              "url": "https://example.com/jobs/2",
              "reason": "missing_required_keyword",
            }
          ]
        },
    }

    await acts.store_scrape(payload)

    inserted = [args for name, args in calls if name == "router:insertIgnoredJob"]
    assert inserted, "Expected insertIgnoredJob to be called"
    assert inserted[0]["title"] == "Unknown"
    assert inserted[0]["description"] is None
