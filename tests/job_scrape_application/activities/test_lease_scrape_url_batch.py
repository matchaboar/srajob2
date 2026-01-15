from __future__ import annotations

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


from job_scrape_application.dbos_runtime.queue import LeaseResult  # noqa: E402
from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_filters_skip_and_marks_failed(monkeypatch):
    leased = {
        "urls": [
            {
                "url": "https://example.com/skip-me",
                "sourceUrl": "https://example.com",
                "pattern": None,
                "_id": "01hzconvexqueueidskip0000000001",
            },
            {
                "url": "https://example.com/process-me",
                "sourceUrl": "https://example.com",
                "pattern": None,
                "_id": "01hzconvexqueueidok00000000001",
            },
        ]
    }

    queue_calls: List[Dict[str, Any]] = []

    def fake_lease_scrape_url_batch(**_kwargs):
        return LeaseResult(urls=leased["urls"], skipped_urls=[])

    def fake_complete_scrape_urls(payload: Dict[str, Any]):
        queue_calls.append(payload)
        return {"updated": len(payload.get("items") or [])}

    async def fake_fetch_seen(source_url: str, pattern: str | None):
        assert source_url == "https://example.com"
        return ["https://example.com/skip-me"]

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.lease_scrape_url_batch",
        fake_lease_scrape_url_batch,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.complete_scrape_urls",
        fake_complete_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.queue_management.fetch_seen_urls_for_site",
        fake_fetch_seen,
    )

    res = await acts.lease_scrape_url_batch("spidercloud", 5)

    assert res["urls"] == [leased["urls"][1]]
    assert res["skippedUrls"] == ["https://example.com/skip-me"]

    skip_call = queue_calls[0]
    items = skip_call.get("items") or []
    assert any(item.get("url") == "https://example.com/skip-me" for item in items)
    assert skip_call["status"] == "failed"
    assert "skip_listed_url" in (skip_call.get("error") or "")


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_keeps_listing_urls_out_of_seen(monkeypatch):
    listing_url = "https://www.metacareers.com/jobsearch?page=2"
    detail_url = "https://www.metacareers.com/profile/job_details/1770681236847041"
    leased = {
        "urls": [
            {
                "url": listing_url,
                "sourceUrl": "https://www.metacareers.com/jobsearch",
                "pattern": None,
                "_id": "01hzconvexqueueidlisting00000001",
            },
            {
                "url": detail_url,
                "sourceUrl": "https://www.metacareers.com/jobsearch",
                "pattern": None,
                "_id": "01hzconvexqueueiddetail00000002",
            },
        ]
    }

    queue_calls: List[Dict[str, Any]] = []

    def fake_lease_scrape_url_batch(**_kwargs):
        return LeaseResult(urls=leased["urls"], skipped_urls=[])

    def fake_complete_scrape_urls(payload: Dict[str, Any]):
        queue_calls.append(payload)
        return {"updated": len(payload.get("items") or [])}

    async def fake_fetch_seen(source_url: str, pattern: str | None):
        assert source_url == "https://www.metacareers.com/jobsearch"
        return [listing_url, detail_url]

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.lease_scrape_url_batch",
        fake_lease_scrape_url_batch,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.complete_scrape_urls",
        fake_complete_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.queue_management.fetch_seen_urls_for_site",
        fake_fetch_seen,
    )

    res = await acts.lease_scrape_url_batch("spidercloud", 5)

    assert res["urls"] == [leased["urls"][0]]
    assert res["skippedUrls"] == [detail_url]
    skip_call = queue_calls[0]
    assert any(item.get("url") == detail_url for item in (skip_call.get("items") or []))


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_handles_non_dict_response(monkeypatch):
    def fake_lease_scrape_url_batch(**_kwargs):
        return None

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.lease_scrape_url_batch",
        fake_lease_scrape_url_batch,
    )

    res = await acts.lease_scrape_url_batch("spidercloud", 2)

    assert res == {"urls": [], "skippedUrls": []}


@pytest.mark.asyncio
async def test_lease_scrape_url_batch_retries_when_all_skipped(monkeypatch):
    lease_payloads = [
        {
            "urls": [
                {
                    "url": "https://example.com/skip-me",
                    "sourceUrl": "https://example.com",
                    "pattern": None,
                    "_id": "01hzconvexqueueidskip0000000002",
                },
            ]
        },
        {
            "urls": [
                {
                    "url": "https://example.com/process-me",
                    "sourceUrl": "https://example.com",
                    "pattern": None,
                    "_id": "01hzconvexqueueidok00000000002",
                },
            ]
        },
    ]
    lease_calls: List[int] = []

    def fake_lease_scrape_url_batch(**_kwargs):
        lease_calls.append(1)
        payload = lease_payloads.pop(0) if lease_payloads else {"urls": []}
        return LeaseResult(urls=payload["urls"], skipped_urls=[])

    def fake_complete_scrape_urls(payload: Dict[str, Any]):
        return {"updated": len(payload.get("items") or [])}

    async def fake_fetch_seen(source_url: str, pattern: str | None):
        assert source_url == "https://example.com"
        return ["https://example.com/skip-me"]

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.lease_scrape_url_batch",
        fake_lease_scrape_url_batch,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.complete_scrape_urls",
        fake_complete_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.queue_management.fetch_seen_urls_for_site",
        fake_fetch_seen,
    )

    res = await acts.lease_scrape_url_batch("spidercloud", 1)

    assert len(res["urls"]) == 1
    url_entry = res["urls"][0]
    assert url_entry["url"] == "https://example.com/process-me"
    assert url_entry["sourceUrl"] == "https://example.com"
    assert url_entry["pattern"] is None
    assert "https://example.com/skip-me" in res.get("skippedUrls", [])
    assert len(lease_calls) == 2
