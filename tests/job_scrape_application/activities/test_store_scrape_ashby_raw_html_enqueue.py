from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows.site_handlers.ashby import (  # noqa: E402
    AshbyHqHandler,
)


FIXTURE_PATH = Path("tests/fixtures/ashby_lambda_spidercloud_raw.html")
API_FIXTURE_PATH = Path("tests/fixtures/ashby_lambda_spidercloud_api.json")
SERVAL_API_FIXTURE_PATH = Path("tests/fixtures/ashby_serval_listing_payload.json")
RAMP_API_FIXTURE_PATH = Path("tests/fixtures/ashby_ramp_spidercloud_api.json")
LISTING_RAW_FIXTURE_PATH = Path("tests/fixtures/ashby_lambda_spidercloud_listing_raw.json")
ASHBY_RAW_HTML_URL_COUNT = 41
SERVAL_API_URL_COUNT = 16
RAMP_API_URL_COUNT = 127


def _load_html() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def _load_api_payload() -> Dict[str, Any]:
    payload = json.loads(API_FIXTURE_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    return payload


def _load_serval_api_payload() -> Dict[str, Any]:
    payload = json.loads(SERVAL_API_FIXTURE_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    return payload


def _load_ramp_api_payload() -> Dict[str, Any]:
    payload = json.loads(RAMP_API_FIXTURE_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    return payload


def _load_listing_raw_payload() -> Any:
    payload = json.loads(LISTING_RAW_FIXTURE_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    if payload and isinstance(payload, list) and isinstance(payload[0], list):
        return payload[0]
    return payload


def _expected_ashby_job_urls(payload: Dict[str, Any]) -> list[str]:
    urls: set[str] = set()
    for job in payload.get("jobs", []) if isinstance(payload, dict) else []:
        if not isinstance(job, dict):
            continue
        for key in ("jobUrl", "applyUrl", "jobPostingUrl", "postingUrl", "url"):
            value = job.get(key)
            if isinstance(value, str) and value.strip():
                urls.add(acts._strip_ashby_application_url(value.strip()))
    return sorted(urls)


def _split_jobs_payload(payload: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    jobs = jobs if isinstance(jobs, list) else []
    midpoint = max(1, len(jobs) // 2)
    return (
        {**payload, "jobs": jobs[:midpoint]},
        {**payload, "jobs": jobs[midpoint:]},
    )


def _build_scrape(html: str) -> Dict[str, Any]:
    return {
        "sourceUrl": "https://jobs.ashbyhq.com/lambda",
        "startedAt": 1,
        "completedAt": 2,
        "items": {"raw": [{"raw_html": html}], "provider": "spidercloud"},
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }


def test_extract_job_urls_from_ashby_raw_html_fixture():
    html = _load_html()
    scrape = _build_scrape(html)

    urls = acts._extract_job_urls_from_scrape(scrape)

    assert len(urls) == ASHBY_RAW_HTML_URL_COUNT
    assert all(url.startswith("https://jobs.ashbyhq.com/lambda/") for url in urls)


def test_extract_job_urls_from_ashby_api_fixture_prefers_listing_api():
    payload = _load_api_payload()
    expected_urls = _expected_ashby_job_urls(payload)
    scrape = {
        "sourceUrl": "https://jobs.ashbyhq.com/lambda",
        "startedAt": 1,
        "completedAt": 2,
        "items": {
            "raw": payload,
            "provider": "spidercloud",
            "page_links": ["https://lambda.ai/careers"],
        },
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }

    urls = acts._extract_job_urls_from_scrape(scrape)

    assert urls == expected_urls
    assert expected_urls, "Expected listing API payload to contain job URLs"


def test_extract_job_urls_from_ashby_listing_raw_payload_matches_api(monkeypatch):
    api_payload = _load_api_payload()
    expected_urls = set(_expected_ashby_job_urls(api_payload))
    listing_payload = _load_listing_raw_payload()
    scrape = {
        "sourceUrl": "https://jobs.ashbyhq.com/lambda",
        "items": {"raw": listing_payload, "provider": "spidercloud"},
    }

    monkeypatch.setattr(acts, "title_matches_required_keywords", lambda *_args, **_kwargs: True)

    urls = acts._extract_job_urls_from_scrape(scrape)

    assert set(urls) == expected_urls
    assert expected_urls, "Expected listing HTML payload to yield job URLs"


def test_extract_job_urls_from_ashby_api_fixture_handles_capitalized_slug():
    payload = _load_serval_api_payload()
    scrape = {
        "sourceUrl": "https://jobs.ashbyhq.com/Serval",
        "startedAt": 1,
        "completedAt": 2,
        "items": {
            "raw": payload,
            "provider": "spidercloud",
            "page_links": ["https://serval.ai/careers"],
        },
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }

    urls = acts._extract_job_urls_from_scrape(scrape)

    assert len(urls) == SERVAL_API_URL_COUNT
    assert not any(url.endswith("/application") for url in urls)
    assert all(url.startswith("https://jobs.ashbyhq.com/Serval/") for url in urls)


def test_extract_job_urls_from_ashby_ramp_api_fixture():
    payload = _load_ramp_api_payload()
    scrape = {
        "sourceUrl": "https://jobs.ashbyhq.com/ramp",
        "startedAt": 1,
        "completedAt": 2,
        "items": {
            "raw": payload,
            "provider": "spidercloud",
            "page_links": ["https://ramp.com/careers"],
        },
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }

    urls = acts._extract_job_urls_from_scrape(scrape)

    assert len(urls) == RAMP_API_URL_COUNT
    assert not any(url.endswith("/application") for url in urls)
    assert all(url.startswith("https://jobs.ashbyhq.com/ramp/") for url in urls)


@pytest.mark.asyncio
async def test_store_scrape_enqueues_ashby_api_pagination_and_extracts_page_two(monkeypatch):
    payload = _load_api_payload()
    page_1, page_2 = _split_jobs_payload(payload)
    page_1_urls = set(_expected_ashby_job_urls(page_1))
    page_2_urls = set(_expected_ashby_job_urls(page_2))
    pagination_url = "https://api.ashbyhq.com/posting-api/job-board/lambda?page=2"

    seen: Dict[str, Any] = {}

    async def fake_convex_mutation(name: str, args: Dict[str, Any] | None = None):
        args = args or {}
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:enqueueScrapeUrls":
            seen["enqueue"] = args
            return {"queued": args.get("urls", [])}
        if name == "router:ingestJobsFromScrape":
            seen["ingest"] = args
            return {"inserted": 0}
        return None

    async def fake_fetch_seen(_source: str, _pattern: str | None):
        return []

    def fake_pagination(_self: AshbyHqHandler, _payload: Any, _source_url: str | None = None) -> list[str]:
        return [pagination_url]

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation",
        fake_convex_mutation,
    )
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)
    monkeypatch.setattr(AshbyHqHandler, "get_pagination_urls_from_json", fake_pagination)

    scrape = {
        "sourceUrl": "https://jobs.ashbyhq.com/lambda",
        "startedAt": 1,
        "completedAt": 2,
        "items": {"raw": page_1, "provider": "spidercloud"},
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }

    await acts.store_scrape(scrape)

    enqueue_args = seen.get("enqueue")
    assert enqueue_args, "enqueueScrapeUrls should be called"
    enqueued = enqueue_args.get("urls") if isinstance(enqueue_args, dict) else []
    assert isinstance(enqueued, list)
    assert pagination_url in enqueued
    assert page_1_urls.issubset(set(enqueued))
    assert page_1_urls
    assert page_2_urls.isdisjoint(set(enqueued))

    page_2_scrape = {
        "sourceUrl": pagination_url,
        "startedAt": 1,
        "completedAt": 2,
        "items": {"raw": page_2, "provider": "spidercloud"},
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }

    page_2_extracted = acts._extract_job_urls_from_scrape(page_2_scrape)
    assert set(page_2_extracted) == page_2_urls
    assert page_2_urls


@pytest.mark.asyncio
async def test_store_scrape_enqueues_ashby_urls_from_raw_html(monkeypatch):
    html = _load_html()
    scrape = _build_scrape(html)

    seen: Dict[str, Any] = {}

    async def fake_convex_mutation(name: str, args: Dict[str, Any] | None = None):
        args = args or {}
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:enqueueScrapeUrls":
            seen["enqueue"] = args
            return {"queued": args.get("urls", [])}
        if name == "router:ingestJobsFromScrape":
            seen["ingest"] = args
            return {"inserted": 0}
        return None

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation",
        fake_convex_mutation,
    )

    await acts.store_scrape(scrape)

    enqueue_args = seen.get("enqueue")
    assert enqueue_args, "enqueueScrapeUrls should be called"
    urls = enqueue_args.get("urls") if isinstance(enqueue_args, dict) else []
    assert isinstance(urls, list) and len(urls) == ASHBY_RAW_HTML_URL_COUNT
    assert all(url.startswith("https://jobs.ashbyhq.com/lambda/") for url in urls)


@pytest.mark.asyncio
async def test_store_scrape_enqueues_ashby_ramp_urls_from_api(monkeypatch):
    payload = _load_ramp_api_payload()
    scrape = {
        "sourceUrl": "https://jobs.ashbyhq.com/ramp",
        "startedAt": 1,
        "completedAt": 2,
        "items": {"raw": payload, "provider": "spidercloud"},
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }

    seen: Dict[str, Any] = {}

    async def fake_convex_mutation(name: str, args: Dict[str, Any] | None = None):
        args = args or {}
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:enqueueScrapeUrls":
            seen["enqueue"] = args
            return {"queued": args.get("urls", [])}
        if name == "router:ingestJobsFromScrape":
            seen["ingest"] = args
            return {"inserted": 0}
        return None

    async def fake_fetch_seen(_source: str, _pattern: str | None):
        return []

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation",
        fake_convex_mutation,
    )
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)

    await acts.store_scrape(scrape)

    enqueue_args = seen.get("enqueue")
    assert enqueue_args, "enqueueScrapeUrls should be called"
    urls = enqueue_args.get("urls") if isinstance(enqueue_args, dict) else []
    assert isinstance(urls, list) and len(urls) == RAMP_API_URL_COUNT
    assert all(url.startswith("https://jobs.ashbyhq.com/ramp/") for url in urls)

    pagination_urls = AshbyHqHandler().get_pagination_urls_from_json(
        payload,
        "https://api.ashbyhq.com/posting-api/job-board/ramp?includeCompensation=false",
    )
    assert set(pagination_urls).issubset(set(urls))
