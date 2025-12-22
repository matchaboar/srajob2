from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.activities import store_scrape  # noqa: E402
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
LISTING_FIXTURE = FIXTURE_DIR / "spidercloud_godaddy_search_page_1.json"
DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_godaddy_job_detail_commonmark.json"
ASHBY_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_ashby_lambda_job_commonmark.json"


def _load_fixture(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_source_url(payload: Any) -> str:
    if isinstance(payload, list) and payload and isinstance(payload[0], list) and payload[0]:
        item = payload[0][0]
        if isinstance(item, dict):
            url = item.get("url")
            if isinstance(url, str):
                return url
    return ""


def _extract_first_event(payload: Any) -> Dict[str, Any] | None:
    if isinstance(payload, list) and payload and isinstance(payload[0], list) and payload[0]:
        item = payload[0][0]
        if isinstance(item, dict):
            return item
    return None


def _extract_commonmark(payload: Any) -> str:
    if isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, list):
                for item in entry:
                    if isinstance(item, dict):
                        content = item.get("content")
                        if isinstance(content, dict) and isinstance(content.get("commonmark"), str):
                            return content["commonmark"]
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
        fetch_seen_urls_for_site=lambda *_args, **_kwargs: [],
    )
    return SpiderCloudScraper(deps)


async def _run_store_scrape(
    raw_payload: Any,
    source_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[str], list[Dict[str, Any]]]:
    calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    await store_scrape(
        {
            "sourceUrl": source_url,
            "provider": "spidercloud",
            "startedAt": 0,
            "completedAt": 1,
            "items": {"provider": "spidercloud", "raw": raw_payload},
        }
    )

    enqueue_calls = [c for c in calls if c["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "store_scrape should enqueue URLs from the GoDaddy listing payload"
    return enqueue_calls[0]["args"]["urls"], calls


@pytest.mark.asyncio
async def test_spidercloud_godaddy_listing_extracts_job_links(monkeypatch: pytest.MonkeyPatch):
    raw_payload = _load_fixture(LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, calls = await _run_store_scrape(raw_payload, source_url, monkeypatch)
    insert_calls = [c for c in calls if c["name"] == "router:insertScrapeRecord"]
    assert insert_calls, "store_scrape should insert the scrape record in Convex"
    assert insert_calls[0]["args"].get("sourceUrl") == source_url

    assert urls, "expected job URLs to be extracted from GoDaddy listing HTML"
    job_urls = [
        url
        for url in urls
        if "careers.godaddy/jobs/" in url and "jobs/search" not in url
    ]
    assert job_urls, "expected job detail URLs from GoDaddy listing page"


def test_spidercloud_godaddy_job_detail_normalizes_description():
    payload = _load_fixture(DETAIL_FIXTURE)
    url = _extract_source_url(payload)
    commonmark = _extract_commonmark(payload)

    scraper = _make_scraper()
    normalized = scraper._normalize_job(url, commonmark, [], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Principal Security Engineer" in normalized["title"]
    assert len(normalized["description"]) > 200
    assert "GoDaddy" in normalized["description"]


def test_spidercloud_ashby_job_detail_prefers_metadata_description():
    payload = _load_fixture(ASHBY_DETAIL_FIXTURE)
    event = _extract_first_event(payload)

    assert event is not None, "expected a spidercloud event for the Ashby job detail fixture"

    scraper = _make_scraper()
    markdown = scraper._extract_markdown(event)  # noqa: SLF001

    assert markdown is not None
    assert "Data Center Operations Engineer" in markdown
    assert "Operations team is at the heart" in markdown
    assert len(markdown) > 500
