from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.activities import (  # noqa: E402
    _extract_job_urls_from_scrape,
)

BAD_URL = "https://careers.snap.com/job?id=R0041979)|Engineering|Regular|Bellevue"
EXPECTED_URL = "https://careers.snap.com/job?id=R0041979"


def _build_scrape(provider: str | None) -> dict:
    items = {"job_urls": [BAD_URL]}
    scrape = {"items": items}
    if provider is not None:
        scrape["provider"] = provider
    return scrape


def test_extract_job_urls_strips_table_tail_across_providers():
    for provider in (None, "spidercloud", "firecrawl", "fetchfox-crawl"):
        scrape = _build_scrape(provider)
        urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

        assert EXPECTED_URL in urls
        assert not any("|Engineering" in url or "Bellevue" in url for url in urls)
