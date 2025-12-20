from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


FIXTURE_PATH = Path("tests/fixtures/ashby_lambda_spidercloud_raw.html")
API_FIXTURE_PATH = Path("tests/fixtures/ashby_lambda_spidercloud_api.json")
ASHBY_RAW_HTML_URL_COUNT = 41
ASHBY_API_URL_COUNT = 186


def _load_html() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def _load_api_payload() -> Dict[str, Any]:
    return json.loads(API_FIXTURE_PATH.read_text(encoding="utf-8"))


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
    scrape = {
        "sourceUrl": "https://jobs.ashbyhq.com/lambda",
        "startedAt": 1,
        "completedAt": 2,
        "items": {"raw": payload, "provider": "spidercloud"},
        "provider": "spidercloud",
        "workflowName": "ScraperSpidercloud",
    }

    urls = acts._extract_job_urls_from_scrape(scrape)

    assert len(urls) == ASHBY_API_URL_COUNT
    assert any(url.endswith("/application") for url in urls)


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
