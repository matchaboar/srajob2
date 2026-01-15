from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest


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
    ("fixture_path", "expected_job_ids"),
    [
        (
            FIXTURE_PAGE_1,
            ("5986323", "7294272"),
        ),
        (
            FIXTURE_PAGE_2,
            ("5986324", "5986325"),
        ),
        (
            FIXTURE_PAGE_3,
            ("5986326", "5986327"),
        ),
    ],
)
async def test_hubspot_listing_batch_enqueues_job_urls_only(
    monkeypatch: pytest.MonkeyPatch,
    fixture_path: Path,
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
    queue_calls: list[Dict[str, Any]] = []
    complete_calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        return None

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        queue_calls.append(payload)
        return {"queued": len(payload.get("urls", []))}

    def fake_complete_scrape_urls(payload: Dict[str, Any]) -> Dict[str, Any]:
        complete_calls.append(payload)
        return {"updated": len(payload.get("items", []))}

    async def fake_query(*_args, **_kwargs):
        return None

    async def fake_seen(*_args, **_kwargs):
        return []

    async def fake_filter_new_job_urls(_urls: list[str]):
        return _urls

    monkeypatch.setattr(acts, "_make_spidercloud_scraper", lambda: FakeScraper())
    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation", fake_mutation
    )
    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_query", fake_query
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.dbos_queue.complete_scrape_urls",
        fake_complete_scrape_urls,
    )
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_seen)
    monkeypatch.setattr(acts, "filter_new_job_urls", fake_filter_new_job_urls)

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

    assert queue_calls, "expected listing batch to enqueue HubSpot URLs"

    urls = queue_calls[0]["urls"]
    for job_id in expected_job_ids:
        assert f"https://www.hubspot.com/careers/jobs/{job_id}" in urls
    assert source_url not in urls
    assert not any("page=" in url for url in urls)
