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
FIXTURE_PAGE_1 = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_hubspot_listing_page_1.json"
)
FIXTURE_PAGE_2 = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_hubspot_listing_page_2.json"
)
FIXTURE_PAGE_3 = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_hubspot_listing_page_3.json"
)


def _load_fixture(path: Path) -> Any:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _extract_source_url(payload: Any) -> str:
    if isinstance(payload, list) and payload and isinstance(payload[0], list) and payload[0]:
        item = payload[0][0]
        if isinstance(item, dict):
            url = item.get("url")
            if isinstance(url, str):
                return url
    return ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_path", "expected_pages", "expected_job_ids"),
    [
        (
            FIXTURE_PAGE_1,
            ("page=2", "page=3", "page=4"),
            ("5986323", "7294272"),
        ),
        (
            FIXTURE_PAGE_2,
            ("page=3", "page=4"),
            ("5986324", "5986325"),
        ),
        (
            FIXTURE_PAGE_3,
            ("page=4",),
            ("5986326", "5986327"),
        ),
    ],
)
async def test_hubspot_listing_batch_enqueues_raw_html_and_pagination(
    monkeypatch: pytest.MonkeyPatch,
    fixture_path: Path,
    expected_pages: tuple[str, ...],
    expected_job_ids: tuple[str, ...],
) -> None:
    raw_payload = _load_fixture(fixture_path)
    source_url = _extract_source_url(raw_payload)
    assert source_url

    class FakeScraper:
        async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "scrape": {
                    "sourceUrl": payload.get("source_url") or source_url,
                    "provider": "spidercloud",
                    "items": {"provider": "spidercloud", "raw": raw_payload},
                }
            }

    calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        if name == "router:enqueueScrapeUrls":
            return {"queued": len(args.get("urls", []))}
        if name == "router:completeScrapeUrls":
            return {"updated": len(args.get("items", []))}
        return None

    async def fake_query(*_args, **_kwargs):
        return None

    async def fake_seen(*_args, **_kwargs):
        return []

    async def fake_filter_existing(_urls: list[str]):
        return []

    monkeypatch.setattr(acts, "_make_spidercloud_scraper", lambda: FakeScraper())
    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation", fake_mutation
    )
    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_query", fake_query
    )
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)

    batch = {
        "urls": [
            {
                "url": source_url,
                "sourceUrl": source_url,
                "provider": "spidercloud",
                "siteId": "hubspot-site",
                "paginationLimit": 4,
            }
        ]
    }

    result = await acts.process_spidercloud_listing_batch(batch)
    assert result.get("queued")

    enqueue_calls = [call for call in calls if call["name"] == "router:enqueueScrapeUrls"]
    assert enqueue_calls, "expected listing batch to enqueue HubSpot URLs"

    urls = enqueue_calls[0]["args"]["urls"]
    for job_id in expected_job_ids:
        assert f"https://www.hubspot.com/careers/jobs/{job_id}" in urls
    assert source_url in urls
    for page_fragment in expected_pages:
        assert any(page_fragment in url for url in urls)
    assert not any("page=5" in url for url in urls)
