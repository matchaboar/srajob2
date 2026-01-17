from __future__ import annotations

import sys
import types
from typing import cast

import pytest


# Stub firecrawl dependency so activities can import without installing it
try:
    import firecrawl  # noqa: F401
    import firecrawl.v2.types  # noqa: F401
except Exception:
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

from job_scrape_application.workflows.activities.errors import (  # noqa: E402
    ScrapeErrorInput,
    clean_scrape_error_payload as _clean_scrape_error_payload,
)
from job_scrape_application.workflows.activities.step import (  # noqa: E402
    log_scrape_error as _log_scrape_error,
)


def test_log_scrape_error_strips_null_values(monkeypatch):
    recorded = {}

    def fake_convex_mutation(name, args=None):  # type: ignore[override]
        recorded["name"] = name
        recorded["args"] = args

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation",
        fake_convex_mutation,
    )

    # _log_scrape_error is now sync - call directly without await
    _log_scrape_error(
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
