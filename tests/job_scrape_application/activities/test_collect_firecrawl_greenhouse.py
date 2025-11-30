from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402

# Raw JSON snapshot (Firecrawl rawHtml response) for the Robinhood Greenhouse board.
RAW_GREENHOUSE_DATA: Dict[str, Any] = {
    "jobs": [
        {
            "absolute_url": "https://boards.greenhouse.io/robinhood/jobs/7379020",
            "id": 7379020,
            "title": "AML Investigator, Crypto",
            "company_name": "Robinhood",
        },
        {
            "absolute_url": "https://boards.greenhouse.io/robinhood/jobs/7318478",
            "id": 7318478,
            "title": "Analytics Engineering",
            "company_name": "Robinhood",
        },
        {
            "absolute_url": "https://boards.greenhouse.io/robinhood/jobs/6669758",
            "id": 6669758,
            "title": "Android Engineer",
            "company_name": "Robinhood",
        },
        {
            "absolute_url": "https://boards.greenhouse.io/robinhood/jobs/7158345",
            "id": 7158345,
            "title": "Brokerage Operations Intern",
            "company_name": "Robinhood",
        },
    ]
}

RAW_GREENHOUSE_JSON = json.dumps(RAW_GREENHOUSE_DATA)
EXPECTED_URLS = [job["absolute_url"] for job in RAW_GREENHOUSE_DATA["jobs"]]


class FakeBatchStatus:
    def __init__(self, raw: str, status: str = "completed"):
        self.status = status
        self.data = [{"raw_html": raw}]

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {"status": self.status, "data": [json.loads(self.data[0]["raw_html"])]}


class FakeFirecrawl:
    def __init__(self, api_key: str | None = None, api_url: str = "https://api.firecrawl.dev"):
        self.api_key = api_key

    def get_batch_scrape_status(self, job_id: str, pagination_config: Any = None):
        return FakeBatchStatus(RAW_GREENHOUSE_JSON)


async def _fake_to_thread(func, *args, **kwargs):
    return func(*args, **kwargs)


def _make_event(job_id: str = "job-greenhouse-workflow") -> acts.FirecrawlWebhookEvent:
    return {
        "id": job_id,
        "jobId": job_id,
        "type": "completed",
        "success": True,
        "data": [],
        "metadata": {
            "kind": "greenhouse_listing",
            "siteUrl": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
        },
    }


@pytest.mark.asyncio
async def test_collect_firecrawl_extracts_greenhouse_urls(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", _fake_to_thread)

    result = await acts.collect_firecrawl_job_result(_make_event())

    assert result["kind"] == "greenhouse_listing"
    assert result["httpStatus"] == "ok"
    assert result["itemsCount"] == len(EXPECTED_URLS)
    assert set(result["job_urls"]) == set(EXPECTED_URLS)
    assert result["response"]["status"] == "completed"
    assert result["asyncResponse"]["jobId"] == "job-greenhouse-workflow"


@pytest.mark.asyncio
async def test_collect_firecrawl_sets_raw_payload(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", _fake_to_thread)

    result = await acts.collect_firecrawl_job_result(_make_event("job-raw"))

    assert "raw" in result
    assert "jobs" in result["raw"]
    assert EXPECTED_URLS[0] in result["raw"]
    assert "response" in result and "raw" in result["response"]
    assert "asyncResponse" in result and result["asyncResponse"]["status"] == "completed"


@pytest.mark.asyncio
async def test_collect_firecrawl_returns_unique_urls(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", _fake_to_thread)

    result = await acts.collect_firecrawl_job_result(_make_event("job-unique"))
    urls = result["job_urls"]

    assert len(urls) == len(set(urls))
    assert len(urls) == len(EXPECTED_URLS)


@pytest.mark.asyncio
async def test_collect_firecrawl_items_count_matches(monkeypatch):
    monkeypatch.setattr(acts.settings, "firecrawl_api_key", "fc-test")
    monkeypatch.setattr(acts, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(acts.asyncio, "to_thread", _fake_to_thread)

    result = await acts.collect_firecrawl_job_result(_make_event("job-count"))

    assert result["itemsCount"] == len(EXPECTED_URLS)
    assert result["jobsScraped"] == len(EXPECTED_URLS)
