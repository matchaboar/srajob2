from __future__ import annotations

from pathlib import Path

import pytest

from job_scrape_application.workflows import activities as acts


ENGINEERING_LISTING_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_engineering_page1.html")
NO_RANGE_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_page1_no_range.html")
ENGINEERING_SOURCE_URL = (
    "https://www.hubspot.com/careers/jobs?page=1#department=product-ux-engineering;"
)


def _build_scrape_payload(source_url: str, html: str) -> dict:
    return {
        "sourceUrl": source_url,
        "provider": "spidercloud",
        "items": {"provider": "spidercloud", "raw": [{"content": html}]},
    }


def test_hubspot_engineering_listing_extracts_engineer_jobs():
    html = ENGINEERING_LISTING_FIXTURE.read_text(encoding="utf-8")
    scrape = _build_scrape_payload(ENGINEERING_SOURCE_URL, html)

    urls = acts._extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://www.hubspot.com/careers/jobs/7294272?hubs_signup-cta=careers-apply" in urls
    assert any("hubs_signup-cta=careers-apply" in url for url in urls)


def test_hubspot_listing_fallback_pagination_includes_first_four_pages():
    html = NO_RANGE_FIXTURE.read_text(encoding="utf-8")
    scrape = _build_scrape_payload(
        "https://www.hubspot.com/careers/jobs?page=1",
        html,
    )

    urls = acts._extract_job_urls_from_scrape(scrape)  # noqa: SLF001

    assert "https://www.hubspot.com/careers/jobs?page=2" in urls
    assert "https://www.hubspot.com/careers/jobs?page=3" in urls
    assert "https://www.hubspot.com/careers/jobs?page=4" in urls


@pytest.mark.asyncio
async def test_store_scrape_enqueues_hubspot_engineering_jobs(monkeypatch):
    html = ENGINEERING_LISTING_FIXTURE.read_text(encoding="utf-8")
    scrape_payload = {
        "sourceUrl": ENGINEERING_SOURCE_URL,
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": [{"content": html}]},
    }

    calls: list[dict] = []

    async def fake_mutation(name: str, args: dict):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    async def fake_seen(*_args, **_kwargs):
        return []

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)

    await acts.store_scrape(scrape_payload)

    enqueue_calls = [call for call in calls if call["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "store_scrape should enqueue HubSpot engineering job URLs"

    urls = enqueue_calls[0]["args"]["urls"]
    assert "https://www.hubspot.com/careers/jobs/7294272?hubs_signup-cta=careers-apply" in urls
