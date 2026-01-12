from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.activities import (  # noqa: E402
    _extract_job_urls_from_scrape,
    _filter_job_urls,
)

BAD_URL = "https://careers.snap.com/job?id=R0041979)|Engineering|Regular|Bellevue"
EXPECTED_URL = "https://careers.snap.com/job?id=R0041979"


def _build_scrape(provider: str | None) -> dict[str, object]:
    items = {"job_urls": [BAD_URL]}
    scrape: dict[str, object] = {"items": items}
    if provider is not None:
        scrape["provider"] = provider
    return scrape


def test_extract_job_urls_strips_table_tail_across_providers():
    for provider in (None, "spidercloud", "firecrawl", "fetchfox-crawl"):
        scrape = _build_scrape(provider)
        urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

        assert EXPECTED_URL in urls
        assert not any("|Engineering" in url or "Bellevue" in url for url in urls)


def test_filter_job_urls_falls_back_to_base_rules():
    urls = [
        "https://elegant-magpie-239.convex.site/share/job?id=abc&app=https%3A%2F%2Flocalhost%3A5173",
        "https://boards.greenhouse.io/coreweave/jobs/4607747006",
    ]
    filtered = _filter_job_urls(urls, None)  # noqa: SLF001

    assert "https://boards.greenhouse.io/coreweave/jobs/4607747006" in filtered
    assert "https://elegant-magpie-239.convex.site/share/job?id=abc&app=https%3A%2F%2Flocalhost%3A5173" not in filtered


def test_extract_job_urls_from_scrape_filters_non_job_direct_urls():
    scrape = {
        "items": {
            "job_urls": [
                "https://affable-kiwi-46.convex.site/share/job?id=k17583qrd0qbncmjhmnkn4djx17z0c6r&app=https%3A%2F%2Fsrajob.netlify.app",
                "https://www.linkedin.com/company/adobe",
                "https://careers.adobe.com/us/en/job/123456/Senior-Engineer",
            ]
        }
    }

    urls = _extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://careers.adobe.com/us/en/job/123456/Senior-Engineer" in urls
    assert "https://affable-kiwi-46.convex.site/share/job?id=k17583qrd0qbncmjhmnkn4djx17z0c6r&app=https%3A%2F%2Fsrajob.netlify.app" not in urls
    assert "https://www.linkedin.com/company/adobe" not in urls
