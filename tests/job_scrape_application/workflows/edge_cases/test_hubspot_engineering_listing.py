from __future__ import annotations

from pathlib import Path



from job_scrape_application.workflows.helpers.job_url_extractor import (
    extract_job_urls_from_scrape as _extract_job_urls_from_scrape,
)
from job_scrape_application.workflows.workflow import store_scrape


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

    urls = _extract_job_urls_from_scrape(scrape)

    assert "https://www.hubspot.com/careers/jobs/7294272" in urls
    assert all("hubs_signup-cta=careers-apply" not in url for url in urls)


def test_hubspot_listing_fallback_pagination_includes_first_four_pages():
    html = NO_RANGE_FIXTURE.read_text(encoding="utf-8")
    scrape = _build_scrape_payload(
        "https://www.hubspot.com/careers/jobs?page=1",
        html,
    )

    urls = _extract_job_urls_from_scrape(scrape)

    assert "https://www.hubspot.com/careers/jobs?page=2" in urls
    assert "https://www.hubspot.com/careers/jobs?page=3" in urls
    assert "https://www.hubspot.com/careers/jobs?page=4" in urls


def test_store_scrape_enqueues_hubspot_engineering_jobs(monkeypatch):
    html = ENGINEERING_LISTING_FIXTURE.read_text(encoding="utf-8")
    scrape_payload = {
        "sourceUrl": ENGINEERING_SOURCE_URL,
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": [{"content": html}]},
    }

    calls: list[dict] = []
    queue_calls: list[dict] = []

    def fake_mutation(name: str, args: dict):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    def fake_enqueue_scrape_urls(payload: dict, *, force_refresh: bool = False) -> dict:
        queue_calls.append(payload)
        return {"queued": len(payload.get("urls", []))}

    def fake_seen(*_args, **_kwargs):
        return []

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.helpers.scrape_utils.fetch_seen_urls_for_site",
        fake_seen,
    )

    store_scrape(scrape_payload)

    assert queue_calls, "store_scrape should enqueue HubSpot engineering job URLs"

    urls = queue_calls[0]["urls"]
    assert 'https://www.hubspot.com/careers/jobs/7442960' in urls
