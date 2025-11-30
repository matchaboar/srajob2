from __future__ import annotations

import os
import sys
from typing import Any, Dict, List

import pytest
from firecrawl.v2.utils.error_handler import PaymentRequiredError
from temporalio.exceptions import ApplicationError

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities
from job_scrape_application.workflows.exceptions import PaymentRequiredWorkflowError


class _MockBatchResult:
    def __init__(self, urls: List[str]):
        self.data = [{"json": {"job_title": f"Engineer Title {i}", "url": url}} for i, url in enumerate(urls)]

    def model_dump(self, *, mode: str = "json", exclude_none: bool = True) -> Dict[str, Any]:
        return {"data": self.data}


class _MockFirecrawlBatch:
    def __init__(self):
        self.calls: List[List[str]] = []

    def batch_scrape(self, urls: List[str], **kwargs: Any):  # noqa: D401, ANN001
        self.calls.append(list(urls))
        return _MockBatchResult(urls)


@pytest.mark.asyncio
async def test_scrape_greenhouse_jobs_firecrawl_batches_urls(monkeypatch):
    monkeypatch.setenv("FIRECRAWL_API_KEY", "key")
    monkeypatch.setattr(activities.settings, "firecrawl_api_key", "key")

    mock = _MockFirecrawlBatch()
    monkeypatch.setattr(activities, "Firecrawl", lambda api_key=None: mock)

    payload = {"urls": ["https://example.com/j1", "https://example.com/j2"], "source_url": "https://example.com"}

    res = await activities.scrape_greenhouse_jobs_firecrawl(payload)

    # batch_scrape should be called once with all URLs to avoid rate limits
    assert len(mock.calls) == 1
    assert mock.calls[0] == payload["urls"]
    assert res["jobsScraped"] == 2


@pytest.mark.asyncio
async def test_scrape_greenhouse_jobs_firecrawl_payment_required_is_non_retryable(monkeypatch):
    monkeypatch.setenv("FIRECRAWL_API_KEY", "key")
    monkeypatch.setattr(activities.settings, "firecrawl_api_key", "key")

    class _PaymentRequiredFirecrawl:
        def batch_scrape(self, *_args: Any, **_kwargs: Any):  # noqa: ANN001
            raise PaymentRequiredError("Payment Required: insufficient credits", status_code=402, response=None)

    monkeypatch.setattr(activities, "Firecrawl", lambda api_key=None: _PaymentRequiredFirecrawl())

    payload = {"urls": ["https://example.com/j1"], "source_url": "https://example.com"}

    with pytest.raises(ApplicationError) as excinfo:
        await activities.scrape_greenhouse_jobs_firecrawl(payload)

    err = excinfo.value
    assert isinstance(err, PaymentRequiredWorkflowError)
    assert err.non_retryable is True
    assert "payment required" in str(err).lower()
