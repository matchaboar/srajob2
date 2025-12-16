from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest

from job_scrape_application.workflows import activities as acts
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (
    SpiderCloudScraper,
    SpidercloudDependencies,
)


def _make_spidercloud_scraper() -> SpiderCloudScraper:
    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=lambda *_args, **_kwargs: [],
    )
    return SpiderCloudScraper(deps)


@pytest.mark.asyncio
async def test_store_scrape_ingests_greenhouse_json_as_markdown(monkeypatch):
    scraper = _make_spidercloud_scraper()
    raw_json = Path("tests/fixtures/greenhouse_api_job.json").read_text(encoding="utf-8")
    desc, title = scraper._extract_greenhouse_json_markdown(raw_json)  # noqa: SLF001

    normalized = {
        "url": "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/5001698007",
        "title": title,
        "job_title": title,
        "company": "The Trade Desk",
        "location": "Bellevue",
        "remote": False,
        "level": "mid",
        "description": desc,
        "posted_at": 0,
    }
    scrape_payload = {
        "provider": "spidercloud",
        "sourceUrl": "https://api.greenhouse.io/v1/boards/thetradedesk/jobs",
        "completedAt": 0,
        "items": {
            "provider": "spidercloud",
            "normalized": [normalized],
            "raw": [{"markdown": desc}],
        },
    }

    calls: List[Dict[str, Any]] = []

    async def fake_convex_mutation(name: str, payload: Dict[str, Any]):
        calls.append({"name": name, "payload": payload})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": len(payload.get("jobs", []))}
        return None

    # Bypass remote Convex calls
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)
    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_query",
        lambda *_, **__: [],
    )

    await acts.store_scrape(scrape_payload)

    ingest_calls = [c for c in calls if c["name"] == "router:ingestJobsFromScrape"]
    assert ingest_calls, "ingest should be called"
    jobs = ingest_calls[0]["payload"]["jobs"]
    assert len(jobs) == 1
    job = jobs[0]
    assert job["url"] == normalized["url"]
    assert job["title"] == title
    assert "Job Title: Senior Software Engineer" in job["description"]
    assert "<" not in job["description"]
    # Markdown-friendly newlines preserved
    assert job["description"].count("\n") >= 5
