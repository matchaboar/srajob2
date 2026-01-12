from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.dbos_runtime import queue as dbos_queue  # noqa: E402
from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite  # noqa: E402
from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows.helpers.link_extractors import normalize_url  # noqa: E402
from job_scrape_application.workflows.site_handlers import get_site_handler  # noqa: E402

SCHEDULE_PATH = Path("job_scrape_application/config/prod/site_schedules.yml")
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


def _load_schedule_entries() -> List[Dict[str, Any]]:
    payload = yaml.safe_load(SCHEDULE_PATH.read_text(encoding="utf-8")) or {}
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("enabled", True)]


def _schedule_id(entry: Dict[str, Any]) -> str:
    return _slugify(str(entry.get("name") or entry.get("url") or "site"))


def _fixture_paths(entry: Dict[str, Any]) -> tuple[Path, Path]:
    slug = _schedule_id(entry)
    return FIXTURE_DIR / f"{slug}_listing.json", FIXTURE_DIR / f"{slug}_detail.json"


def _load_fixture(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"Fixture {path} must contain a dict payload")
    if not isinstance(payload.get("request"), dict):
        raise AssertionError(f"Fixture {path} missing request metadata")
    return payload


def _matches_expected_url(candidate: str, expected: str, source_url: str) -> bool:
    if candidate == expected:
        return True
    handler = get_site_handler(candidate) or get_site_handler(source_url) or get_site_handler(expected)
    if handler and handler.name == "greenhouse":
        api_url = handler.get_api_uri(candidate)
        return api_url == expected
    normalized_candidate = normalize_url(candidate, base_url=source_url)
    normalized_expected = normalize_url(expected, base_url=source_url)
    if normalized_candidate and normalized_expected:
        return normalized_candidate == normalized_expected
    return False


def _scrape_has_description(scrape: Dict[str, Any]) -> bool:
    items = scrape.get("items") if isinstance(scrape, dict) else None
    if not isinstance(items, dict):
        return False
    normalized = items.get("normalized")
    if not isinstance(normalized, list) or not normalized:
        return False
    row = normalized[0] if isinstance(normalized[0], dict) else {}
    description = row.get("description") or row.get("job_description")
    return isinstance(description, str) and bool(description.strip())


def _row_matches_expected(scrape: Dict[str, Any], expected: str, source_url: str) -> bool:
    items = scrape.get("items") if isinstance(scrape, dict) else None
    if not isinstance(items, dict):
        return False
    normalized = items.get("normalized")
    if not isinstance(normalized, list) or not normalized:
        return False
    row = normalized[0] if isinstance(normalized[0], dict) else {}
    for key in ("url", "job_url", "absolute_url", "apply_url"):
        value = row.get(key)
        if isinstance(value, str) and _matches_expected_url(value, expected, source_url):
            return True
    return False


SCHEDULE_ENTRIES = _load_schedule_entries()


@pytest.mark.parametrize(
    "entry",
    SCHEDULE_ENTRIES,
    ids=lambda entry: _schedule_id(entry),
)
def test_dbos_schedule_fixtures_exist(entry: Dict[str, Any]) -> None:
    listing_path, detail_path = _fixture_paths(entry)
    assert listing_path.exists() and detail_path.exists(), (
        f"Missing fixtures for {_schedule_id(entry)}"
    )


class _FixtureAsyncSpider:
    def __init__(self, api_key: str, fixtures: Dict[str, Dict[str, Any]], calls: List[Dict[str, Any]]):
        self.api_key = api_key
        self._fixtures = fixtures
        self._calls = calls

    async def __aenter__(self) -> "_FixtureAsyncSpider":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        fixture = self._fixtures.get(url)
        if not fixture:
            raise AssertionError(f"Unexpected SpiderCloud URL: {url}")
        request = fixture.get("request", {})
        expected_params = request.get("params")
        if expected_params is not None:
            assert params == expected_params
        expected_stream = request.get("stream")
        if expected_stream is not None:
            assert stream == expected_stream
        expected_content_type = request.get("contentType")
        if expected_content_type is not None:
            assert content_type == expected_content_type
        self._calls.append({"url": url, "params": params})
        response = fixture.get("response", [])
        if response is None:
            response = []

        async def _iterator():
            if isinstance(response, list):
                for item in response:
                    yield item
            else:
                yield response

        return _iterator()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    SCHEDULE_ENTRIES,
    ids=lambda entry: _schedule_id(entry),
)
async def test_dbos_schedule_workflow_steps(
    entry: Dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listing_path, detail_path = _fixture_paths(entry)
    assert listing_path.exists() and detail_path.exists(), (
        f"Missing fixtures for {_schedule_id(entry)}"
    )
    listing_fixture = _load_fixture(listing_path)
    detail_fixture = _load_fixture(detail_path)

    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test")
    dbos_sqlite._CONNECTIONS.connection = None

    site_id = _schedule_id(entry)
    expected_detail_url = detail_fixture["request"]["url"]
    fixtures = {
        listing_fixture["request"]["url"]: listing_fixture,
        detail_fixture["request"]["url"]: detail_fixture,
    }
    calls: List[Dict[str, Any]] = []

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: _FixtureAsyncSpider(api_key, fixtures, calls),
    )

    async def fake_convex_query(name: str, payload: Dict[str, Any]) -> Dict[str, Any] | None:
        if name == "router:getSiteById" and payload.get("id") == site_id:
            limit = entry.get("paginationLimit")
            if isinstance(limit, (int, float)) and limit > 0:
                return {"paginationLimit": int(limit)}
            return {"paginationLimit": 3}
        return None

    async def fake_store_scrape(scrape: Dict[str, Any]) -> str:
        stored_scrapes.append(scrape)
        return f"scrape-{len(stored_scrapes)}"

    async def fake_fetch_seen_urls(*_args: Any, **_kwargs: Any) -> List[str]:
        return []

    async def fake_filter_existing_job_urls(urls: List[str]) -> List[str]:
        return []

    stored_scrapes: List[Dict[str, Any]] = []
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_query", fake_convex_query)
    monkeypatch.setattr(acts, "store_scrape", fake_store_scrape)
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen_urls)
    monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing_job_urls)

    listing_url = entry.get("url")
    assert isinstance(listing_url, str) and listing_url
    assert expected_detail_url != listing_url, (
        f"Detail fixture uses listing URL for {_schedule_id(entry)}"
    )

    enqueue_payload = {
        "urls": [listing_url],
        "sourceUrl": listing_url,
        "provider": "spidercloud",
        "siteId": site_id,
        "pattern": entry.get("pattern"),
        "urlTypes": ["listing"],
    }
    dbos_queue.enqueue_scrape_urls(enqueue_payload)

    listing_batch = dbos_queue.lease_scrape_url_batch(url_type="listing", limit=1)
    await acts.process_spidercloud_listing_batch({"urls": listing_batch.urls})

    queued_detail_urls = [row["url"] for row in dbos_queue.list_scrape_urls(site_id=site_id)]
    if not queued_detail_urls:
        queued_detail_urls = [row["url"] for row in dbos_queue.list_scrape_urls()]
    assert any(
        _matches_expected_url(url, expected_detail_url, listing_url) for url in queued_detail_urls
    ), "Expected listing workflow to enqueue job detail URL"

    detail_batch = dbos_queue.lease_scrape_url_batch(url_type="detail", limit=1)
    await acts.process_spidercloud_job_batch({"urls": detail_batch.urls}, persist_scrapes=True)

    used_urls = {call["url"] for call in calls}
    assert listing_fixture["request"]["url"] in used_urls
    assert detail_fixture["request"]["url"] in used_urls

    assert stored_scrapes, "Expected store_scrape to persist at least one job"
    assert any(_scrape_has_description(scrape) for scrape in stored_scrapes)
    assert any(
        _row_matches_expected(scrape, expected_detail_url, listing_url) for scrape in stored_scrapes
    )
