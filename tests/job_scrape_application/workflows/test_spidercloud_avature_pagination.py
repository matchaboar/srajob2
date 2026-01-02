from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from job_scrape_application.workflows.activities import store_scrape
from job_scrape_application.workflows.helpers.scrape_utils import trim_scrape_for_convex

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
PAGE_1 = FIXTURE_DIR / "spidercloud_bloomberg_avature_search_page_1.json"
PAGE_2 = FIXTURE_DIR / "spidercloud_bloomberg_avature_search_page_2.json"


def _load_fixture(path: Path) -> Any:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _extract_url(payload: Any) -> str:
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        first = payload[0][0] if payload[0] else {}
        if isinstance(first, dict):
            url = first.get("url")
            if isinstance(url, str):
                return url
    return ""


async def _run_store_scrape(
    raw_payload: Any,
    source_url: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    trim_payload: bool = False,
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

    scrape_payload: Dict[str, Any] = {
        "sourceUrl": source_url,
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": raw_payload},
    }
    if trim_payload:
        scrape_payload = trim_scrape_for_convex(scrape_payload)

    await store_scrape(scrape_payload)

    enqueue_calls = [c for c in calls if c["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "store_scrape should enqueue URLs from Avature listing payload"
    return enqueue_calls[0]["args"]["urls"], calls


@pytest.mark.asyncio
async def test_spidercloud_avature_page_1_adds_joboffset_zero_and_next(monkeypatch: pytest.MonkeyPatch):
    raw_payload = _load_fixture(PAGE_1)
    source_url = _extract_url(raw_payload)

    urls, calls = await _run_store_scrape(raw_payload, source_url, monkeypatch)
    insert_calls = [c for c in calls if c["name"] == "router:insertScrapeRecord"]
    assert insert_calls, "store_scrape should insert the scrape record in Convex"
    assert insert_calls[0]["args"].get("sourceUrl") == source_url

    assert urls, "expected Avature listing URLs to be extracted"
    assert any("joboffset=0" in url.lower() for url in urls)
    assert any("joboffset=12" in url.lower() for url in urls)
    assert any("/careers/JobDetail/" in url for url in urls), "expected job detail URLs from page links"


@pytest.mark.asyncio
async def test_spidercloud_avature_page_2_adds_next_offset(monkeypatch: pytest.MonkeyPatch):
    raw_payload = _load_fixture(PAGE_2)
    source_url = _extract_url(raw_payload)

    urls, calls = await _run_store_scrape(raw_payload, source_url, monkeypatch)
    insert_calls = [c for c in calls if c["name"] == "router:insertScrapeRecord"]
    assert insert_calls, "store_scrape should insert the scrape record in Convex"
    assert insert_calls[0]["args"].get("sourceUrl") == source_url

    assert urls, "expected Avature listing URLs to be extracted"
    assert any("joboffset=24" in url.lower() for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_avature_trimmed_payload_preserves_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(PAGE_1)
    source_url = _extract_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch, trim_payload=True)

    assert urls, "expected Avature listing URLs to be extracted from trimmed payload"
    assert any("/careers/JobDetail/" in url for url in urls), "expected job detail URLs from trimmed payload"
