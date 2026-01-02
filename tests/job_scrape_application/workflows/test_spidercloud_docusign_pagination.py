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

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers.docusign import (  # noqa: E402
    DocusignHandler,
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
PAGE_1 = FIXTURE_DIR / "spidercloud_docusign_api_page_1.json"
PAGE_2 = FIXTURE_DIR / "spidercloud_docusign_api_page_2.json"

LISTING_URL_PAGE_1 = (
    "https://careers.docusign.com/api/jobs"
    "?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations"
    "&page=1"
    "&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States"
    "&sortBy=relevance&descending=false&internal=false"
)
LISTING_URL_PAGE_2 = LISTING_URL_PAGE_1.replace("page=1", "page=2")


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


def _load_fixture(path: Path) -> Any:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _parse_payload(raw_payload: Any) -> Dict[str, Any]:
    scraper = _make_scraper()
    parsed = scraper._extract_json_payload(raw_payload)
    assert isinstance(parsed, dict)
    return parsed


def _extract_first_job_url(payload: Dict[str, Any]) -> str:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise AssertionError("expected jobs list in payload")
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_data = job.get("data") if isinstance(job.get("data"), dict) else job
        if not isinstance(job_data, dict):
            continue
        meta = job_data.get("meta_data")
        if isinstance(meta, dict):
            canonical = meta.get("canonical_url")
            if isinstance(canonical, str) and canonical.strip():
                return canonical.strip()
        for key in ("canonical_url", "jobUrl", "postingUrl", "url"):
            value = job_data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    raise AssertionError("no job url found in payload")


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

    async def fake_fetch_seen(_source: str, _pattern: str | None):
        return []

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen)

    scrape_payload: Dict[str, Any] = {
        "sourceUrl": source_url,
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": raw_payload},
    }

    await acts.store_scrape(scrape_payload)

    enqueue_calls = [c for c in calls if c["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "store_scrape should enqueue URLs from Docusign listing payload"
    return enqueue_calls[0]["args"]["urls"], calls


@pytest.mark.asyncio
async def test_store_scrape_enqueues_docusign_page_1_jobs_and_pagination(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(PAGE_1)
    parsed = _parse_payload(raw_payload)

    expected_job_url = _extract_first_job_url(parsed)
    pagination_urls = DocusignHandler().get_pagination_urls_from_json(parsed, LISTING_URL_PAGE_1)

    urls, calls = await _run_store_scrape(raw_payload, LISTING_URL_PAGE_1, monkeypatch)
    insert_calls = [c for c in calls if c["name"] == "router:insertScrapeRecord"]
    assert insert_calls, "store_scrape should insert the scrape record in Convex"
    assert insert_calls[0]["args"].get("sourceUrl") == LISTING_URL_PAGE_1

    assert expected_job_url in urls, "expected job detail URL from Docusign listing payload"
    assert pagination_urls, "expected pagination URLs for Docusign page 1"
    for url in pagination_urls:
        assert url in urls, f"expected pagination URL queued: {url}"


@pytest.mark.asyncio
async def test_store_scrape_enqueues_docusign_page_2_jobs_and_pagination(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(PAGE_2)
    parsed = _parse_payload(raw_payload)

    expected_job_url = _extract_first_job_url(parsed)
    pagination_urls = DocusignHandler().get_pagination_urls_from_json(parsed, LISTING_URL_PAGE_2)

    urls, calls = await _run_store_scrape(raw_payload, LISTING_URL_PAGE_2, monkeypatch)
    insert_calls = [c for c in calls if c["name"] == "router:insertScrapeRecord"]
    assert insert_calls, "store_scrape should insert the scrape record in Convex"
    assert insert_calls[0]["args"].get("sourceUrl") == LISTING_URL_PAGE_2

    assert expected_job_url in urls, "expected job detail URL from Docusign page 2 payload"
    assert pagination_urls, "expected pagination URLs for Docusign page 2"
    for url in pagination_urls:
        assert url in urls, f"expected pagination URL queued: {url}"
