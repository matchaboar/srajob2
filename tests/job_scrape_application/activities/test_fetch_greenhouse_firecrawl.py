import os
import sys

import pytest
from temporalio.exceptions import ApplicationError

sys.path.insert(0, os.path.abspath("."))
from job_scrape_application.workflows import activities  # noqa: E402
from firecrawl.v2.utils.error_handler import RequestTimeoutError


@pytest.mark.asyncio
async def test_fetch_greenhouse_listing_firecrawl_handles_format_error(monkeypatch):
    """Ensure Firecrawl format errors are surfaced as non-retryable application errors."""

    class StubFirecrawl:
        def __init__(self, api_key: str | None = None):
            self.api_key = api_key

        def batch_scrape(self, urls, formats=None, **_kwargs):  # noqa: ANN001, D401
            raise ValueError("json format must be an object with 'type', 'prompt', and 'schema' fields")

    monkeypatch.setattr(activities, "Firecrawl", StubFirecrawl)

    site = {"url": "https://boards.greenhouse.io/example"}

    with pytest.raises(ApplicationError) as excinfo:
        await activities.fetch_greenhouse_listing_firecrawl(site)

    message = str(excinfo.value)
    assert "Firecrawl scrape failed" in message
    assert "json format must be an object" in message
    # Confirm Temporal treats it as non-retryable to avoid noisy retries
    assert excinfo.value.non_retryable is True


@pytest.mark.asyncio
async def test_fetch_greenhouse_listing_firecrawl_handles_timeout(monkeypatch):
    """Surface Firecrawl timeouts as non-retryable with clear context."""

    class StubFirecrawl:
        def __init__(self, api_key: str | None = None):
            self.api_key = api_key

        def batch_scrape(self, urls, formats=None, **_kwargs):  # noqa: ANN001, D401
            raise RequestTimeoutError("Request Timeout: Failed to scrape as the request timed out.", 408, None)

    monkeypatch.setattr(activities, "Firecrawl", StubFirecrawl)

    site = {"url": "https://boards.greenhouse.io/example"}

    with pytest.raises(ApplicationError) as excinfo:
        await activities.fetch_greenhouse_listing_firecrawl(site)

    message = str(excinfo.value)
    assert "timed out" in message
    assert "greenhouse.io/example" in message
    assert excinfo.value.non_retryable is True
