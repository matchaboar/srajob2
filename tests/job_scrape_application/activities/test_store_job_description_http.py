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
    setattr(firecrawl_mod, "Firecrawl", type("Firecrawl", (), {}))
    sys.modules.setdefault("firecrawl", firecrawl_mod)
    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
    setattr(firecrawl_v2_types, "PaginationConfig", type("PaginationConfig", (), {}))
    setattr(firecrawl_v2_types, "ScrapeOptions", type("ScrapeOptions", (), {}))
    sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
    sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)
    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
    setattr(
        firecrawl_v2_utils_error,
        "PaymentRequiredError",
        type("PaymentRequiredError", (Exception,), {}),
    )
    setattr(
        firecrawl_v2_utils_error,
        "RequestTimeoutError",
        type("RequestTimeoutError", (Exception,), {}),
    )
    sys.modules.setdefault("firecrawl.v2.utils", firecrawl_v2_utils)
    sys.modules.setdefault("firecrawl.v2.utils.error_handler", firecrawl_v2_utils_error)
    setattr(firecrawl_v2_utils, "error_handler", firecrawl_v2_utils_error)
fetchfox_mod = types.ModuleType("fetchfox_sdk")
setattr(fetchfox_mod, "FetchFox", type("FetchFox", (), {}))
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

    setattr(temporalio, "activity", _Activity())
    sys.modules.setdefault("temporalio.activity", temporalio)

    temporalio_exceptions = types.ModuleType("temporalio.exceptions")
    setattr(
        temporalio_exceptions,
        "ApplicationError",
        type("ApplicationError", (Exception,), {}),
    )
    sys.modules.setdefault("temporalio.exceptions", temporalio_exceptions)

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_store_job_descriptions_via_http_posts_payload(monkeypatch):
    calls: List[Dict[str, Any]] = []
    looked_up: List[Dict[str, Any]] = []

    async def fake_convex_query(name: str, args: Dict[str, Any] | None = None):
        looked_up.append({"name": name, "args": args})
        return "job-123"

    class FakeResponse:
        def __init__(self, status_code: int, text: str):
            self.status_code = status_code
            self.text = text

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url: str, json: Dict[str, Any]):
            calls.append({"url": url, "json": json})
            return FakeResponse(200, "ok")

    monkeypatch.setattr(acts.settings, "convex_http_url", "https://example.convex.site")
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_convex_query)
    monkeypatch.setattr(acts.httpx, "AsyncClient", FakeClient)

    jobs = [
        {
            "url": "https://example.com/job/123/",
            "description": "Full description",
        },
        {
            "url": "https://example.com/job/empty",
            "description": " ",
        },
    ]

    await acts._store_job_descriptions_via_http(
        jobs,
        "https://example.com",
        "spidercloud",
        "workflow",
    )

    assert looked_up[0]["name"] == "jobs:getJobIdByUrl"
    assert looked_up[0]["args"] == {"url": "https://example.com/job/123"}
    assert calls == [
        {
            "url": "https://example.convex.site/api/job-description",
            "json": {"jobId": "job-123", "description": "Full description"},
        }
    ]
