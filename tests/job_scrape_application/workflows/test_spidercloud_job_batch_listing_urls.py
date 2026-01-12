from __future__ import annotations

import os
import sys
from typing import Any

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_spidercloud_job_batch_skips_listing_urls(monkeypatch: pytest.MonkeyPatch) -> None:
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

    monkeypatch.setattr(acts, "_make_spidercloud_scraper", lambda: FakeScraper())

    batch = {
        "urls": [
            {"url": listing_url, "sourceUrl": source_url},
            {"url": detail_url, "sourceUrl": source_url},
        ]
    }

    result = await acts.process_spidercloud_job_batch(batch, persist_scrapes=False)

    assert result.get("scrapes"), "Expected scrape payloads to be returned"
    assert captured, "Expected scraper to be invoked"
    flattened = [url for group in captured for url in group]
    assert listing_url not in flattened
    assert detail_url in flattened
