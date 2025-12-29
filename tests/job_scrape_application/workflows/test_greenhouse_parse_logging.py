from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio.exceptions import ApplicationError


if "fetchfox_sdk" not in sys.modules:
    fetchfox_mod = types.ModuleType("fetchfox_sdk")
    fetchfox_mod.FetchFox = type("FetchFox", (), {})
    sys.modules["fetchfox_sdk"] = fetchfox_mod

if "firecrawl" not in sys.modules:
    firecrawl_mod = types.ModuleType("firecrawl")
    firecrawl_mod.Firecrawl = type("Firecrawl", (), {})
    sys.modules["firecrawl"] = firecrawl_mod
    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
    firecrawl_v2_types.PaginationConfig = type("PaginationConfig", (), {})
    firecrawl_v2_types.ScrapeOptions = type("ScrapeOptions", (), {})
    sys.modules["firecrawl.v2"] = firecrawl_v2
    sys.modules["firecrawl.v2.types"] = firecrawl_v2_types
    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
    firecrawl_v2_utils_error.PaymentRequiredError = type("PaymentRequiredError", (Exception,), {})
    firecrawl_v2_utils_error.RequestTimeoutError = type("RequestTimeoutError", (Exception,), {})
    sys.modules["firecrawl.v2.utils"] = firecrawl_v2_utils
    sys.modules["firecrawl.v2.utils.error_handler"] = firecrawl_v2_utils_error


from job_scrape_application.components.models import (  # noqa: E402
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)
from job_scrape_application.workflows.scrapers.fetchfox_scraper import (  # noqa: E402
    FetchfoxDependencies,
    FetchfoxScraper,
)
from job_scrape_application.workflows.scrapers.firecrawl_scraper import (  # noqa: E402
    FirecrawlDependencies,
    FirecrawlScraper,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)


@pytest.mark.asyncio
async def test_fetchfox_parse_failure_emits_posthog_exception(monkeypatch):
    emitted: list[dict[str, object]] = []

    def _emit_exception(exc: Exception, properties: dict[str, object] | None = None) -> None:
        emitted.append({"exc": exc, "properties": properties or {}})

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.fetchfox_scraper.telemetry.emit_posthog_exception",
        _emit_exception,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.fetchfox_scraper.telemetry.emit_posthog_log",
        lambda *_a, **_k: None,
    )

    async def fake_to_thread(_fn, *_a, **_k):
        return {"raw": "not-json"}

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.fetchfox_scraper.asyncio.to_thread",
        fake_to_thread,
    )

    deps = FetchfoxDependencies(
        fetch_seen_urls_for_site=lambda *_a, **_k: [],
        build_job_template=lambda: {},
        build_request_snapshot=lambda *_a, **_k: {},
        log_provider_dispatch=lambda *_a, **_k: None,
        log_sync_response=lambda *_a, **_k: None,
        normalize_fetchfox_items=lambda *_a, **_k: [],
        trim_scrape_for_convex=lambda payload, **_k: payload,
        settings=SimpleNamespace(fetchfox_api_key="ff-test-key"),
        load_greenhouse_board=load_greenhouse_board,
        extract_greenhouse_job_urls=extract_greenhouse_job_urls,
        extract_raw_body_from_fetchfox_result=lambda _result: "not-json",
    )
    scraper = FetchfoxScraper(deps)
    site = {"_id": "site-1", "url": "https://api.greenhouse.io/v1/boards/example/jobs"}

    with pytest.raises(ApplicationError):
        await scraper.fetch_greenhouse_listing(site)

    assert emitted
    assert emitted[0]["properties"].get("event") == "scrape.greenhouse_listing.parse_failed"


@pytest.mark.asyncio
async def test_firecrawl_parse_failure_emits_posthog_exception(monkeypatch):
    emitted: list[dict[str, object]] = []

    def _emit_exception(exc: Exception, properties: dict[str, object] | None = None) -> None:
        emitted.append({"exc": exc, "properties": properties or {}})

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.firecrawl_scraper.telemetry.emit_posthog_exception",
        _emit_exception,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.firecrawl_scraper.telemetry.emit_posthog_log",
        lambda *_a, **_k: None,
    )

    async def fake_to_thread(_fn, *_a, **_k):
        return {"data": [{"text": "not-json"}]}

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.firecrawl_scraper.asyncio.to_thread",
        fake_to_thread,
    )

    deps = FirecrawlDependencies(
        start_firecrawl_webhook_scrape=lambda *_a, **_k: {},
        build_request_snapshot=lambda *_a, **_k: {},
        settings=SimpleNamespace(firecrawl_api_key="fc-test-key"),
        firecrawl_cls=lambda *_a, **_k: None,
        build_firecrawl_schema=lambda: {},
        log_provider_dispatch=lambda *_a, **_k: None,
        log_sync_response=lambda *_a, **_k: None,
        trim_scrape_for_convex=lambda payload, **_k: payload,
        normalize_firecrawl_items=lambda *_a, **_k: [],
        log_scrape_error=lambda *_a, **_k: None,
        load_greenhouse_board=load_greenhouse_board,
        extract_greenhouse_job_urls=extract_greenhouse_job_urls,
        firecrawl_cache_max_age_ms=0,
    )
    scraper = FirecrawlScraper(deps)
    site = {"_id": "site-2", "url": "https://api.greenhouse.io/v1/boards/example/jobs"}

    with pytest.raises(ApplicationError):
        await scraper.fetch_greenhouse_listing(site)

    assert emitted
    assert emitted[0]["properties"].get("event") == "scrape.greenhouse_listing.parse_failed"


@pytest.mark.asyncio
async def test_spidercloud_empty_html_emits_parse_failed(monkeypatch):
    emitted: list[dict[str, object]] = []

    def _emit_log(payload: dict[str, object]) -> None:
        emitted.append(payload)

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.telemetry.emit_posthog_log",
        _emit_log,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.telemetry.emit_posthog_exception",
        lambda *_a, **_k: None,
    )

    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *_a, **_k: {},
        log_dispatch=lambda *_a, **_k: None,
        log_sync_response=lambda *_a, **_k: None,
        trim_scrape_for_convex=lambda payload, **_k: payload,
        settings=SimpleNamespace(spider_api_key="spider-test-key"),
        fetch_seen_urls_for_site=lambda *_a, **_k: [],
    )
    scraper = SpiderCloudScraper(deps)
    site = {"_id": "site-3", "url": "https://api.greenhouse.io/v1/boards/example/jobs"}
    empty_html = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_empty_listing.html"
    ).read_text(encoding="utf-8")
    raw_events = [
        [
            {
                "content": {"raw": empty_html},
                "metadata": {
                    "raw": {
                        "domain": "api.greenhouse.io",
                        "resource_type": ".html",
                        "file_size": len(empty_html),
                        "original_url": site["url"],
                        "pathname": "/v1/boards/example/jobs",
                    }
                },
                "status": 200,
                "url": site["url"],
            }
        ]
    ]

    async def _fake_fetch(_api_url, _handler):
        return empty_html, raw_events

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", _fake_fetch)

    result = await scraper.fetch_greenhouse_listing(site)

    assert result.get("parseFailed") is True
    assert emitted
    assert emitted[0].get("event") == "scrape.greenhouse_listing.parse_failed"
