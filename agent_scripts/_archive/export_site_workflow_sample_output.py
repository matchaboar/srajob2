from __future__ import annotations

import asyncio
import orjson
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import yaml


from job_scrape_application.dbos_runtime import queue as dbos_queue  # noqa: E402
from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite  # noqa: E402
from job_scrape_application.workflows import activities as acts  # noqa: E402

SCHEDULE_PATH = Path("job_scrape_application/config/prod/site_schedules.yml")
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")
OUTPUT_PATH = Path("site-wf-sample-output.md")
VOLTAGE_PARK_SLUG = "voltage_park"

logger = logging.getLogger(__name__)
def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"
def _load_schedule_entries() -> list[dict[str, Any]]:
    payload = yaml.safe_load(SCHEDULE_PATH.read_text(encoding="utf-8")) or {}
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("enabled", True)]
def _schedule_id(entry: dict[str, Any]) -> str:
    return _slugify(str(entry.get("name") or entry.get("url") or "site"))
def _fixture_paths(entry: dict[str, Any]) -> tuple[Path, Path]:
    slug = _schedule_id(entry)
    return FIXTURE_DIR / f"{slug}_listing.json", FIXTURE_DIR / f"{slug}_detail.json"
def _pagination_limit(entry: dict[str, Any]) -> int:
    limit = entry.get("paginationLimit")
    if isinstance(limit, (int, float)) and limit > 0:
        return int(limit)
    return 3
def _load_fixture(path: Path) -> dict[str, Any]:
    payload = orjson.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"Fixture {path} must contain a dict payload")
    if not isinstance(payload.get("request"), dict):
        raise AssertionError(f"Fixture {path} missing request metadata")
    return payload
class _FixtureAsyncSpider:
    def __init__(self, api_key: str, fixtures: dict[str, dict[str, Any]], calls: list[dict[str, Any]]):
        self.api_key = api_key
        self._fixtures = fixtures
        self._calls = calls

    async def __aenter__(self) -> "_FixtureAsyncSpider":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    def scrape_url(self, url: str, *, params: dict[str, Any], stream: bool, content_type: str):
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
def _response_summary(response: Any) -> str:
    if response is None:
        return "none"
    if isinstance(response, list):
        return f"list({len(response)})"
    return type(response).__name__
@dataclass(frozen=True)
class _ListingDiagnostics:
    site_id: str
    listing_url: str
    pattern: str | None
    pagination_limit: int
    listing_fixture_path: Path
    detail_fixture_path: Path
    listing_fixture_request_url: str | None
    detail_fixture_request_url: str | None
    listing_response_summary: str
    spider_calls: list[dict[str, Any]]
@dataclass(frozen=True)
class _ListingResult:
    detail_urls: list[str]
    diagnostics: _ListingDiagnostics
async def _collect_listing_output(entry: dict[str, Any]) -> _ListingResult:
    listing_path, detail_path = _fixture_paths(entry)
    listing_fixture = _load_fixture(listing_path)
    detail_fixture = _load_fixture(detail_path)

    listing_url = entry.get("url")
    if not isinstance(listing_url, str) or not listing_url:
        raise AssertionError(f"Schedule entry missing listing URL: {entry}")

    site_id = _schedule_id(entry)
    fixtures = {
        listing_fixture["request"]["url"]: listing_fixture,
        detail_fixture["request"]["url"]: detail_fixture,
    }
    calls: list[dict[str, Any]] = []

    def fake_convex_query(name: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        if name == "router:getSiteById" and payload.get("id") == site_id:
            return {"paginationLimit": _pagination_limit(entry)}
        return None

    def fake_store_scrape(scrape: dict[str, Any]) -> str:
        return "scrape-1"

    def fake_fetch_seen_urls(*_args: Any, **_kwargs: Any) -> list[str]:
        return []

    def fake_filter_existing_job_urls(urls: list[str]) -> list[str]:
        return []

    import job_scrape_application.services.convex_client as convex_client
    import job_scrape_application.workflows.scrapers.spidercloud_scraper as spidercloud_scraper

    original_spider = spidercloud_scraper.AsyncSpider
    original_convex_query = convex_client.convex_query
    original_store_scrape = acts.store_scrape
    original_fetch_seen = acts.fetch_seen_urls_for_site
    original_filter_existing = acts.filter_existing_job_urls

    spidercloud_scraper.AsyncSpider = lambda api_key: _FixtureAsyncSpider(  # type: ignore[assignment]
        api_key,
        fixtures,
        calls,
    )
    convex_client.convex_query = fake_convex_query  # type: ignore[assignment]
    acts.store_scrape = fake_store_scrape  # type: ignore[assignment]
    acts.fetch_seen_urls_for_site = fake_fetch_seen_urls  # type: ignore[assignment]
    acts.filter_existing_job_urls = fake_filter_existing_job_urls  # type: ignore[assignment]

    detail_urls: list[str] = []
    try:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "dbos.sqlite"
            os.environ["DBOS_SQLITE_PATH"] = str(db_path)
            os.environ["SPIDER_API_KEY"] = "test"
            dbos_sqlite._CONNECTIONS.connection = None

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

            queued_detail_urls = [
                row["url"] for row in dbos_queue.list_scrape_urls(site_id=site_id)
            ]
            if not queued_detail_urls:
                queued_detail_urls = [row["url"] for row in dbos_queue.list_scrape_urls()]
            detail_urls = queued_detail_urls
    finally:
        spidercloud_scraper.AsyncSpider = original_spider  # type: ignore[assignment]
        convex_client.convex_query = original_convex_query  # type: ignore[assignment]
        acts.store_scrape = original_store_scrape  # type: ignore[assignment]
        acts.fetch_seen_urls_for_site = original_fetch_seen  # type: ignore[assignment]
        acts.filter_existing_job_urls = original_filter_existing  # type: ignore[assignment]

    diagnostics = _ListingDiagnostics(
        site_id=site_id,
        listing_url=listing_url,
        pattern=entry.get("pattern") if isinstance(entry.get("pattern"), str) else None,
        pagination_limit=_pagination_limit(entry),
        listing_fixture_path=listing_path,
        detail_fixture_path=detail_path,
        listing_fixture_request_url=listing_fixture.get("request", {}).get("url"),
        detail_fixture_request_url=detail_fixture.get("request", {}).get("url"),
        listing_response_summary=_response_summary(listing_fixture.get("response")),
        spider_calls=list(calls),
    )
    return _ListingResult(detail_urls=detail_urls, diagnostics=diagnostics)
async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    entries = _load_schedule_entries()
    if not entries:
        raise SystemExit(f"No schedule entries found in {SCHEDULE_PATH}")

    lines: list[str] = [
        "# Site workflow sample output",
        "",
        "Generated from fixtures in `tests/job_scrape_application/workflows/fixtures/dbos_schedule`.",
        "",
    ]
    site_sections: list[str] = []
    warnings: list[_ListingDiagnostics] = []
    skipped: list[str] = []

    for entry in entries:
        site_id = _schedule_id(entry)
        if site_id == VOLTAGE_PARK_SLUG:
            skipped.append(site_id)
            continue
        result = await _collect_listing_output(entry)
        detail_urls = result.detail_urls
        listing_url = entry.get("url")
        site_sections.append(f"## {site_id}")
        site_sections.append("")
        site_sections.append(f"Listing URL: `{listing_url}`")
        site_sections.append("")
        if detail_urls:
            site_sections.append("Detail URLs:")
            site_sections.extend([f"- `{url}`" for url in detail_urls])
        else:
            site_sections.append("Detail URLs: _none_")
            warnings.append(result.diagnostics)
        site_sections.append("")

    if warnings:
        lines.append("## WARNINGS")
        lines.append("")
        for warning in warnings:
            lines.append(f"### {warning.site_id}")
            lines.append("")
            lines.append(f"- Listing URL: `{warning.listing_url}`")
            if warning.pattern:
                lines.append(f"- Pattern: `{warning.pattern}`")
            else:
                lines.append("- Pattern: _none_")
            lines.append(f"- Pagination limit: `{warning.pagination_limit}`")
            lines.append(f"- Listing fixture: `{warning.listing_fixture_path}`")
            lines.append(f"- Detail fixture: `{warning.detail_fixture_path}`")
            listing_request_url = warning.listing_fixture_request_url or "unknown"
            detail_request_url = warning.detail_fixture_request_url or "unknown"
            lines.append(f"- Listing fixture request URL: `{listing_request_url}`")
            lines.append(f"- Detail fixture request URL: `{detail_request_url}`")
            lines.append(f"- Listing response: `{warning.listing_response_summary}`")
            lines.append("- Spider calls:")
            if warning.spider_calls:
                for call in warning.spider_calls:
                    url = call.get("url")
                    params = call.get("params", {})
                    if isinstance(params, dict):
                        params_text = orjson.dumps(
                            params,
                            option=orjson.OPT_SORT_KEYS,
                        ).decode("utf-8")
                    else:
                        params_text = orjson.dumps(params).decode("utf-8")
                    lines.append(f"  - `{url}` params={params_text}")
            else:
                lines.append("  - _none_")
            lines.append("")

    lines.extend(site_sections)

    if skipped:
        lines.append("## Skipped")
        lines.append("")
        for site_id in skipped:
            lines.append(f"- `{site_id}` (listing/detail fixture uses identical URL)")
        lines.append("")

    OUTPUT_PATH.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    logger.info("Wrote %s", OUTPUT_PATH)
if __name__ == "__main__":
    asyncio.run(main())
