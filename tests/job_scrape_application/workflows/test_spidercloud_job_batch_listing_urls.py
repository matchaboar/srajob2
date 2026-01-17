from __future__ import annotations

import sys
from typing import Any

import pytest


from job_scrape_application.workflows.workflow import process_spidercloud_job_batch  # noqa: E402


@pytest.mark.asyncio
async def test_spidercloud_job_batch_skips_listing_urls(reset_dbos, monkeypatch: pytest.MonkeyPatch) -> None:
    # Mock record_scrape_url_attempts to avoid real Convex calls
    def mock_record_attempts(entries):
        pass

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.record_scrape_url_attempts",
        mock_record_attempts,
    )
    # Mock filter_new_job_urls to return all URLs as new
    def mock_filter_new(urls):
        return urls

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.step.filter_new_job_urls",
        mock_filter_new,
    )

    listing_url = (
        "https://apply.careers.microsoft.com/api/pcsx/search"
        "?domain=microsoft.com&query=software%20engineer&start=10"
    )
    detail_url = "https://apply.careers.microsoft.com/careers/job/1970393556653560"
    source_url = (
        "https://apply.careers.microsoft.com/careers?query=software+engineer&start=0"
    )

    captured: list[list[str]] = []

    class FakeScraper:
        async def scrape_greenhouse_jobs(self, payload: dict[str, Any]) -> dict[str, Any]:
            captured.append(list(payload.get("urls", [])))
            return {
                "scrape": {
                    "provider": "spidercloud",
                    "sourceUrl": payload.get("source_url"),
                    "items": {"normalized": [], "raw": []},
                }
            }

    # Patch the scraper creation in the step module where it's actually used
    # Access the actual module (not the function exported via __init__.py)
    scrape_module = sys.modules.get(
        "job_scrape_application.workflows.activities.step.scrape_job_details"
    )
    if scrape_module is None:
        import importlib
        scrape_module = importlib.import_module(
            "job_scrape_application.workflows.activities.step.scrape_job_details"
        )
    monkeypatch.setattr(scrape_module, "_make_scraper", lambda: FakeScraper())

    batch = {
        "urls": [
            {"url": listing_url, "sourceUrl": source_url, "urlType": "listing"},
            {"url": detail_url, "sourceUrl": source_url},
        ]
    }

    result = await process_spidercloud_job_batch(batch, persist_scrapes=False)

    # Check that the workflow completed successfully
    assert result.get("provider") == "spidercloud", "Expected spidercloud provider"
    assert captured, "Expected scraper to be invoked"
    flattened = [url for group in captured for url in group]
    assert listing_url not in flattened, "Listing URL should be skipped"
    assert detail_url in flattened, "Detail URL should be included"
