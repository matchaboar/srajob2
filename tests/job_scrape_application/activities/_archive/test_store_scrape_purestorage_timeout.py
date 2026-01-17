from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)


FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/"
    "spidercloud_purestorage_greenhouse_listing.json"
)
LISTING_URL = "https://api.greenhouse.io/v1/boards/purestorage/jobs"


def _load_fixture() -> Any:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        payload = payload.get("response")
    return payload


def _extract_listing_raw_html(payload: Any) -> str:
    if isinstance(payload, list) and payload and isinstance(payload[0], list) and payload[0]:
        item = payload[0][0]
        if isinstance(item, dict):
            content = item.get("content")
            if isinstance(content, dict):
                raw = content.get("raw")
                if isinstance(raw, str):
                    return raw
    return ""


def _make_scraper() -> SpiderCloudScraper:
    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=lambda *_a, **_k: [],
    )
    return SpiderCloudScraper(deps)


@pytest.mark.asyncio
async def test_store_scrape_purestorage_ingest_should_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _load_fixture()
    raw_html = _extract_listing_raw_html(payload)
    assert raw_html, "expected raw HTML content in Pure Storage listing fixture"
    raw_events = payload if isinstance(payload, list) else []

    scraper = _make_scraper()

    async def _fake_fetch(_api_url: str, _handler: Any) -> tuple[str, list[Any]]:
        return raw_html, raw_events

    monkeypatch.setattr(scraper, "_fetch_greenhouse_listing_payload", _fake_fetch)

    listing = await scraper.fetch_greenhouse_listing(
        {"_id": "site-purestorage", "url": LISTING_URL, "type": "greenhouse"}
    )
    job_urls = listing.get("job_urls") if isinstance(listing, dict) else []
    assert len(job_urls) >= 150, "fixture should provide a large listing payload"

    normalized: list[dict[str, Any]] = []
    for idx, url in enumerate(job_urls[:150]):
        normalized.append(
            {
                "url": url,
                "title": f"Test Role {idx}",
                "job_title": f"Test Role {idx}",
                "company": "Pure Storage",
                "location": "Remote, Germany",
                "remote": True,
                "level": "mid",
                "description": "Role description.",
                "posted_at": 0,
            }
        )

    ingest_calls: list[dict[str, Any]] = []

    def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            ingest_calls.append(args)
            return {"inserted": len(args.get("jobs") or [])}
        return None

    def fake_query(name: str, args: Dict[str, Any] | None = None):
        if name == "router:listJobDetailConfigs":
            return []
        return []

    def fail_get_client():
        raise RuntimeError("get_client should not be called with patched functions")

    # Reset global convex client and patch all convex functions
    monkeypatch.setattr("job_scrape_application.services.convex_client._client", None)
    monkeypatch.setattr("job_scrape_application.services.convex_client.get_client", fail_get_client)
    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_query)

    acts.store_scrape(
        {
            "sourceUrl": LISTING_URL,
            "provider": "spidercloud",
            "startedAt": 0,
            "completedAt": 1,
            "items": {"provider": "spidercloud", "normalized": normalized},
        }
    )

    assert ingest_calls, "expected ingestJobsFromScrape to be called"
    max_jobs = max(len(call.get("jobs") or []) for call in ingest_calls)
    assert max_jobs <= 100, "expected ingest to be chunked to avoid timeouts"
