from __future__ import annotations

import os
import sys
import types
from typing import cast

import pytest

sys.path.insert(0, os.path.abspath("."))

# Stub firecrawl dependency so activities can import without installing it
firecrawl_mod = types.ModuleType("firecrawl")
firecrawl_mod.Firecrawl = type("Firecrawl", (), {})
sys.modules.setdefault("firecrawl", firecrawl_mod)
firecrawl_v2 = types.ModuleType("firecrawl.v2")
firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
firecrawl_v2_types.PaginationConfig = type("PaginationConfig", (), {})
sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)

try:
    import temporalio  # noqa: F401
except ImportError:  # pragma: no cover
    pytest.skip("temporalio not installed", allow_module_level=True)

from job_scrape_application.workflows import activities  # noqa: E402
from job_scrape_application.workflows.activities import (  # noqa: E402
    ScrapeErrorInput,
    _clean_scrape_error_payload,
)


@pytest.mark.asyncio
async def test_log_scrape_error_strips_null_values(monkeypatch):
    recorded = {}

    async def fake_convex_mutation(name, args=None):  # type: ignore[override]
        recorded["name"] = name
        recorded["args"] = args

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation",
        fake_convex_mutation,
    )
    monkeypatch.setattr(activities.time, "time", lambda: 1234)

    await activities._log_scrape_error(
        cast(
            ScrapeErrorInput,
            {
                "jobId": None,
                "sourceUrl": "https://example.com",
                "siteId": None,
                "status": None,
                "event": "start_batch_scrape",
                "error": "boom",
            },
        )
    )

    assert recorded["name"] == "router:insertScrapeError"
    payload = recorded["args"]
    assert payload["error"] == "boom"
    assert payload["sourceUrl"] == "https://example.com"
    assert payload["createdAt"] == 1234 * 1000
    assert "jobId" not in payload
    assert "siteId" not in payload
    assert "status" not in payload


def test_clean_scrape_error_payload_preserves_valid_strings():
    payload: ScrapeErrorInput = {
        "error": "failed",
        "createdAt": 111,
        "jobId": "job-1",
        "sourceUrl": "https://source",
        "siteId": "site-1",
        "event": "batch_scrape",
        "status": "error",
    }

    cleaned = _clean_scrape_error_payload(payload)

    assert cleaned["error"] == "failed"
    assert cleaned["createdAt"] == 111
    assert cleaned["jobId"] == "job-1"
    assert cleaned["sourceUrl"] == "https://source"
    assert cleaned["siteId"] == "site-1"
    assert cleaned["event"] == "batch_scrape"
    assert cleaned["status"] == "error"
