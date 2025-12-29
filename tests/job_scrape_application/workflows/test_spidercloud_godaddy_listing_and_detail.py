from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.activities import store_scrape  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import parse_markdown_hints  # noqa: E402
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
LISTING_FIXTURE = FIXTURE_DIR / "spidercloud_godaddy_search_page_1.json"
DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_godaddy_job_detail_commonmark.json"
MONGODB_LISTING_FIXTURE = FIXTURE_DIR / "spidercloud_mongodb_greenhouse_listing.json"
MONGODB_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_mongodb_greenhouse_job_detail.json"
AXON_LISTING_FIXTURE = FIXTURE_DIR / "spidercloud_axon_greenhouse_listing.json"
AXON_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_axon_greenhouse_job_detail.json"
PURESTORAGE_LISTING_FIXTURE = (
    FIXTURE_DIR / "spidercloud_purestorage_greenhouse_listing.json"
)
SAMSARA_LISTING_FIXTURE = (
    FIXTURE_DIR / "spidercloud_samsara_greenhouse_listing.json"
)
NEXHEALTH_LISTING_FIXTURE = (
    FIXTURE_DIR / "spidercloud_nexhealth_greenhouse_listing.json"
)
RUBRIK_LISTING_FIXTURE = (
    FIXTURE_DIR / "spidercloud_rubrik_greenhouse_listing.json"
)
ZSCALER_LISTING_FIXTURE = (
    FIXTURE_DIR / "spidercloud_zscaler_greenhouse_listing.json"
)
ASHBY_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_ashby_lambda_job_commonmark.json"
ASHBY_RAMP_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_ashby_ramp_job_commonmark.json"
NETFLIX_LISTING_FIXTURE = FIXTURE_DIR / "spidercloud_netflix_listing_page.json"
NETFLIX_LISTING_COMMONMARK_FIXTURE = (
    FIXTURE_DIR / "spidercloud_netflix_listing_page_commonmark.json"
)
NETFLIX_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_netflix_job_detail_commonmark.json"
NETFLIX_RAW_HTML_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_netflix_job_detail_790313323421_raw_html.json"
BLOOMBERG_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_bloomberg_avature_job_detail_commonmark.json"
OKTA_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_okta_greenhouse_job_detail_commonmark.json"
NEXHEALTH_DETAIL_FIXTURE = (
    FIXTURE_DIR / "spidercloud_nexhealth_greenhouse_job_detail.json"
)
DATAMINR_WORKDAY_DETAIL_FIXTURE = (
    FIXTURE_DIR / "spidercloud_dataminr_workday_job_detail_api.json"
)
GITHUB_DETAIL_FIXTURE = FIXTURE_DIR / "spidercloud_github_careers_job_4648_raw.json"
MITHRIL_DETAIL_FIXTURE = (
    FIXTURE_DIR / "spidercloud_greenhouse_mithril_job_4604609007_raw.json"
)
TOGETHERAI_DETAIL_FIXTURE = (
    FIXTURE_DIR / "spidercloud_greenhouse_togetherai_job_4967737007_raw.json"
)
WORKDAY_DETAIL_FIXTURES = (
    FIXTURE_DIR / "spidercloud_broadcom_workday_job_detail_api.json",
    FIXTURE_DIR / "spidercloud_broadcom_workday_job_detail_kubernetes_api.json",
)
NETFLIX_COMMONMARK_LISTING_FIXTURES = (
    FIXTURE_DIR / "spidercloud_netflix_api_page_1_commonmark.json",
    FIXTURE_DIR / "spidercloud_netflix_api_page_2_commonmark.json",
    FIXTURE_DIR / "spidercloud_netflix_api_page_3_commonmark.json",
)
NETFLIX_COMMONMARK_DETAIL_FIXTURES = (
    FIXTURE_DIR / "spidercloud_netflix_job_detail_790313345439_commonmark.json",
    FIXTURE_DIR / "spidercloud_netflix_job_detail_790313323421_commonmark.json",
    FIXTURE_DIR / "spidercloud_netflix_job_detail_790313310792_commonmark.json",
)
NETFLIX_EMPTY_COMMONMARK_DETAIL_FIXTURE = (
    FIXTURE_DIR / "spidercloud_netflix_job_detail_790313241540_commonmark.json"
)


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


def _extract_event_markdown(scraper: SpiderCloudScraper, payload: Any) -> str:
    event = _extract_first_event(payload)
    if not isinstance(event, dict):
        return ""
    markdown = scraper._extract_markdown(event)  # noqa: SLF001
    return markdown or ""


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


def _extract_normalized_from_commonmark(payload: Any) -> Dict[str, Any]:
    url = _extract_source_url(payload)
    commonmark = _extract_commonmark(payload)
    scraper = _make_scraper()
    normalized = scraper._normalize_job(url, commonmark, [], 0)  # noqa: SLF001
    assert normalized is not None, "expected normalized job from commonmark payload"
    return normalized


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


@pytest.mark.asyncio
async def test_spidercloud_mongodb_listing_extracts_job_links(monkeypatch: pytest.MonkeyPatch):
    raw_payload = _load_fixture(MONGODB_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected MongoDB listing URLs to be extracted"
    assert any("mongodb.com/careers/job" in url and "gh_jid=" in url for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_axon_listing_extracts_job_links(monkeypatch: pytest.MonkeyPatch):
    raw_payload = _load_fixture(AXON_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Axon listing URLs to be extracted"
    assert any("job-boards.greenhouse.io/axon/jobs/" in url for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_purestorage_listing_extracts_job_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(PURESTORAGE_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Pure Storage listing URLs to be extracted"
    assert any("boards.greenhouse.io/purestorage/jobs/" in url for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_samsara_listing_extracts_job_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(SAMSARA_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Samsara listing URLs to be extracted"
    assert any(
        "samsara.com/company/careers/roles/" in url and "gh_jid=" in url for url in urls
    )


@pytest.mark.asyncio
async def test_spidercloud_nexhealth_listing_extracts_job_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(NEXHEALTH_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected NexHealth listing URLs to be extracted"
    assert any("nexhealth.com/careers/open-positions" in url for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_rubrik_listing_extracts_job_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(RUBRIK_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Rubrik listing URLs to be extracted"
    assert any("rubrik.com/company/careers" in url and "gh_jid=" in url for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_zscaler_listing_extracts_job_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(ZSCALER_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Zscaler listing URLs to be extracted"
    assert any("job-boards.greenhouse.io/zscaler/jobs/" in url for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_netflix_listing_extracts_job_links(monkeypatch: pytest.MonkeyPatch):
    raw_payload = _load_fixture(NETFLIX_LISTING_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Netflix listing URLs to be extracted"
    assert any("explore.jobs.netflix.net/careers/job/" in url for url in urls)


@pytest.mark.asyncio
async def test_spidercloud_netflix_listing_commonmark_extracts_job_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(NETFLIX_LISTING_COMMONMARK_FIXTURE)
    source_url = _extract_source_url(raw_payload)

    assert _extract_commonmark(raw_payload), "expected commonmark content in fixture"

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Netflix listing URLs to be extracted from commonmark"
    assert any("explore.jobs.netflix.net/careers/job/" in url for url in urls)


@pytest.mark.asyncio
@pytest.mark.parametrize("fixture_path", NETFLIX_COMMONMARK_LISTING_FIXTURES)
async def test_spidercloud_netflix_commonmark_listing_enqueues_jobs(
    monkeypatch: pytest.MonkeyPatch,
    fixture_path: Path,
):
    raw_payload = _load_fixture(fixture_path)
    source_url = _extract_source_url(raw_payload)

    assert _extract_commonmark(raw_payload), "expected commonmark content in fixture"

    urls, _ = await _run_store_scrape(raw_payload, source_url, monkeypatch)

    assert urls, "expected Netflix listing URLs to be extracted from commonmark payload"
    assert any("explore.jobs.netflix.net/careers/job/" in url for url in urls)


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


def test_spidercloud_mongodb_job_detail_normalizes_description():
    payload = _load_fixture(MONGODB_DETAIL_FIXTURE)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw HTML event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Analytics Engineering Intern" in normalized["title"]
    assert "base salary range for this role" in normalized["description"].lower()
    assert "$56,576" in normalized["description"]
    assert "$82,368" in normalized["description"]
    assert "content-pay-transparency" not in normalized["description"]
    assert "class=" not in normalized["description"]

    hints = parse_markdown_hints(normalized["description"])
    assert hints.get("compensation_range") == {"low": 56576, "high": 82368}
    assert hints.get("compensation") == 69472


def test_spidercloud_axon_job_detail_normalizes_description():
    payload = _load_fixture(AXON_DETAIL_FIXTURE)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw HTML event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Account Executive" in normalized["title"]
    assert "The Pay:" in normalized["description"]
    assert "$73,100" in normalized["description"]
    assert "$117,000" in normalized["description"]
    assert "$197,750" in normalized["description"]
    assert "data-stringify" not in normalized["description"]
    assert "content-conclusion" not in normalized["description"]

    hints = parse_markdown_hints(normalized["description"])
    assert hints.get("compensation_range") == {"low": 73100, "high": 117000}
    assert hints.get("compensation") == 197750


def test_spidercloud_nexhealth_job_detail_normalizes_fields():
    payload = _load_fixture(NEXHEALTH_DETAIL_FIXTURE)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw HTML event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Demand Generation Lead" in normalized["title"]
    assert normalized["location"] == "San Francisco, CA"
    assert "About NexHealth" in normalized["description"]
    assert len(normalized["description"]) > 200


def test_spidercloud_greenhouse_mithril_api_job_detail_normalizes_description():
    payload = _load_fixture(MITHRIL_DETAIL_FIXTURE)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw payload event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Senior Product Engineer" in normalized["title"]
    assert "Mithril" in normalized["description"]
    assert len(normalized["description"]) > 200


def test_spidercloud_greenhouse_togetherai_api_job_detail_normalizes_description():
    payload = _load_fixture(TOGETHERAI_DETAIL_FIXTURE)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw payload event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Sales Development Engineer" in normalized["title"]
    assert "Together AI" in normalized["description"]
    assert len(normalized["description"]) > 200


def test_spidercloud_github_careers_job_detail_not_ignored():
    payload = _load_fixture(GITHUB_DETAIL_FIXTURE)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw HTML event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Senior Service Delivery Engineer" in normalized["title"]
    assert "GitHub" in normalized["description"]
    assert len(normalized["description"]) > 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_path, expected_title, expected_description_snippet",
    [
        (GITHUB_DETAIL_FIXTURE, "Senior Service Delivery Engineer", "GitHub"),
        (MITHRIL_DETAIL_FIXTURE, "Senior Product Engineer", "Mithril"),
        (TOGETHERAI_DETAIL_FIXTURE, "Sales Development Engineer", "Together AI"),
    ],
)
async def test_spidercloud_detail_raw_payload_ingests_job(
    monkeypatch: pytest.MonkeyPatch,
    fixture_path: Path,
    expected_title: str,
    expected_description_snippet: str,
):
    payload = _load_fixture(fixture_path)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001
    assert normalized is not None

    calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": len(args.get("jobs", []))}
        return None

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    await store_scrape(
        {
            "sourceUrl": url,
            "provider": "spidercloud",
            "startedAt": 0,
            "completedAt": 1,
            "items": {"provider": "spidercloud", "normalized": [normalized]},
        }
    )

    ingest_calls = [c for c in calls if c["name"] == "router:ingestJobsFromScrape"]
    assert ingest_calls, "expected normalized job to be ingested into Convex"
    jobs = ingest_calls[0]["args"].get("jobs", [])
    assert len(jobs) == 1
    assert jobs[0].get("url") == url
    assert expected_title in (jobs[0].get("title") or "")
    description = jobs[0].get("description") or ""
    assert expected_description_snippet in description
    assert len(description) > 200
    assert not any(c["name"] == "router:insertIgnoredJob" for c in calls)


def test_spidercloud_okta_job_detail_commonmark_normalizes_description():
    payload = _load_fixture(OKTA_DETAIL_FIXTURE)
    url = _extract_source_url(payload)
    commonmark = _extract_commonmark(payload)

    scraper = _make_scraper()
    normalized = scraper._normalize_job(url, commonmark, [], 0)  # noqa: SLF001

    assert normalized is not None
    assert "ABX Marketing Manager" in normalized["title"]
    assert len(normalized["description"]) > 200
    assert "Okta" in normalized["description"]
    for line in normalized["description"].splitlines():
        stripped = line.strip()
        assert stripped not in {"[", "](#)"}
        assert not re.fullmatch(r"\[\s*\]\(\s*#?\s*\)", stripped)


def test_spidercloud_netflix_job_detail_commonmark_normalizes_description():
    payload = _load_fixture(NETFLIX_DETAIL_FIXTURE)
    url = _extract_source_url(payload)
    commonmark = _extract_commonmark(payload)

    scraper = _make_scraper()
    normalized = scraper._normalize_job(url, commonmark, [], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Data Engineer" in normalized["title"]
    assert len(normalized["description"]) > 200
    assert "Netflix" in normalized["description"]
    assert "themeOptions" not in normalized["description"]
    assert '"domain": "netflix.com"' not in normalized["description"]
    assert "display_banner" not in normalized["description"]


def test_spidercloud_netflix_job_detail_raw_html_normalizes_description():
    payload = _load_fixture(NETFLIX_RAW_HTML_DETAIL_FIXTURE)
    url = _extract_source_url(payload)

    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)
    event = _extract_first_event(payload)
    assert event is not None, "expected raw HTML event in fixture"

    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Data Visualization Engineer" in normalized["title"]
    assert len(normalized["description"]) > 200
    assert "Netflix" in normalized["description"]


def test_spidercloud_bloomberg_avature_job_detail_commonmark_normalizes_description():
    payload = _load_fixture(BLOOMBERG_DETAIL_FIXTURE)
    url = _extract_source_url(payload)
    commonmark = _extract_commonmark(payload)

    scraper = _make_scraper()
    normalized = scraper._normalize_job(url, commonmark, [], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Infrastructure Automation Engineer" in normalized["title"]
    assert len(normalized["description"]) > 200
    assert "Bloomberg" in normalized["description"]


@pytest.mark.parametrize("fixture_path", WORKDAY_DETAIL_FIXTURES)
def test_spidercloud_workday_job_detail_api_extracts_markdown(fixture_path: Path):
    payload = _load_fixture(fixture_path)
    scraper = _make_scraper()
    markdown = _extract_event_markdown(scraper, payload)

    assert markdown, "expected markdown extracted from Workday API payload"
    assert "Job Description" in markdown
    assert "Please Note" in markdown
    assert "Requirements" in markdown


def test_spidercloud_workday_job_detail_api_normalizes_title_and_description():
    payload = _load_fixture(DATAMINR_WORKDAY_DETAIL_FIXTURE)
    url = _extract_source_url(payload)
    scraper = _make_scraper()
    event = _extract_first_event(payload)
    assert event is not None, "expected raw Workday API event in fixture"

    markdown = _extract_event_markdown(scraper, payload)
    normalized = scraper._normalize_job(url, markdown, [event], 0)  # noqa: SLF001

    assert normalized is not None
    assert "Software Engineer, Backend" in (normalized.get("title") or "")
    description = normalized.get("description") or ""
    assert len(description) > 200
    assert "Dataminr" in description


@pytest.mark.asyncio
@pytest.mark.parametrize("fixture_path", NETFLIX_COMMONMARK_DETAIL_FIXTURES)
async def test_spidercloud_netflix_commonmark_job_detail_ingests_job(
    monkeypatch: pytest.MonkeyPatch,
    fixture_path: Path,
):
    payload = _load_fixture(fixture_path)
    normalized = _extract_normalized_from_commonmark(payload)
    source_url = normalized["url"]

    calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": len(args.get("jobs", []))}
        return None

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    await store_scrape(
        {
            "sourceUrl": source_url,
            "provider": "spidercloud",
            "startedAt": 0,
            "completedAt": 1,
            "items": {"provider": "spidercloud", "normalized": [normalized]},
        }
    )

    ingest_calls = [c for c in calls if c["name"] == "router:ingestJobsFromScrape"]
    assert ingest_calls, "expected normalized job to be ingested into Convex"
    jobs = ingest_calls[0]["args"].get("jobs", [])
    assert len(jobs) == 1
    assert jobs[0].get("url") == source_url
    assert "Netflix" in (jobs[0].get("company") or "")
    description = jobs[0].get("description") or ""
    assert "themeOptions" not in description
    assert '"domain": "netflix.com"' not in description
    assert "display_banner" not in description


@pytest.mark.asyncio
async def test_spidercloud_ashby_commonmark_job_detail_ingests_job(
    monkeypatch: pytest.MonkeyPatch,
):
    payload = _load_fixture(ASHBY_DETAIL_FIXTURE)
    normalized = _extract_normalized_from_commonmark(payload)
    source_url = normalized["url"]

    calls: list[Dict[str, Any]] = []

    async def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": len(args.get("jobs", []))}
        return None

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    await store_scrape(
        {
            "sourceUrl": source_url,
            "provider": "spidercloud",
            "startedAt": 0,
            "completedAt": 1,
            "items": {"provider": "spidercloud", "normalized": [normalized]},
        }
    )

    ingest_calls = [c for c in calls if c["name"] == "router:ingestJobsFromScrape"]
    assert ingest_calls, "expected normalized job to be ingested into Convex"
    jobs = ingest_calls[0]["args"].get("jobs", [])
    assert len(jobs) == 1
    assert jobs[0].get("url") == source_url
    assert "Senior Software Engineer" in (jobs[0].get("title") or "")


def test_spidercloud_ashby_job_detail_prefers_metadata_description():
    payload = _load_fixture(ASHBY_DETAIL_FIXTURE)
    event = _extract_first_event(payload)

    assert event is not None, "expected a spidercloud event for the Ashby job detail fixture"

    scraper = _make_scraper()
    markdown = scraper._extract_markdown(event)  # noqa: SLF001

    assert markdown is not None
    assert "Senior Software Engineer" in markdown
    assert "Lambda" in markdown
    assert len(markdown) > 500


def test_spidercloud_ashby_ramp_job_detail_not_ignored():
    payload = _load_fixture(ASHBY_RAMP_DETAIL_FIXTURE)
    url = _extract_source_url(payload)
    commonmark = _extract_commonmark(payload)
    scraper = _make_scraper()

    normalized = scraper._normalize_job(url, commonmark, [], 0)  # noqa: SLF001

    assert normalized is not None
    assert scraper._last_ignored_job is None  # noqa: SLF001


def test_spidercloud_netflix_detail_placeholder_title_does_not_drop():
    payload = _load_fixture(NETFLIX_EMPTY_COMMONMARK_DETAIL_FIXTURE)
    url = _extract_source_url(payload)
    commonmark = _extract_commonmark(payload)
    event = _extract_first_event(payload) or {}

    scraper = _make_scraper()
    normalized = scraper._normalize_job(  # noqa: SLF001
        url,
        commonmark,
        [dict(event, title=url)],
        0,
    )

    assert normalized is not None


def test_spidercloud_title_from_markdown_skips_list_item():
    scraper = _make_scraper()
    markdown = "\n".join(
        [
            "* Facilitate requirements definition with design and engineering partners",
            "",
            "Sr. Director, Data Product Management - Product/Growth in San Francisco, California | Docusign",
            "",
            "Company Overview",
        ]
    )

    title = scraper._title_from_markdown(markdown)  # noqa: SLF001

    assert title == "Sr. Director, Data Product Management - Product/Growth"


def test_spidercloud_title_from_markdown_skips_id_and_url_lines():
    scraper = _make_scraper()
    markdown = "\n".join(
        [
            "C49B5C9B 6646 4A13 Af57 Ed522D15Cdf7)\\N*",
            "https://careers.docusign.com/jobs/27794?lang=en-us",
            "Jobs",
            "Senior Software Engineer",
        ]
    )

    title = scraper._title_from_markdown(markdown)  # noqa: SLF001

    assert title == "Senior Software Engineer"


def test_spidercloud_title_from_url_skips_id_like_slugs():
    scraper = _make_scraper()

    assert (
        scraper._title_from_url("https://careers.docusign.com/jobs/27794?lang=en-us")  # noqa: SLF001
        == "Untitled"
    )
    assert (
        scraper._title_from_url("https://boards.greenhouse.io/stripe/jobs/7379530")  # noqa: SLF001
        == "Untitled"
    )
    assert (
        scraper._title_from_url(
            "https://jobs.ashbyhq.com/notion/c49b5c9b-6646-4a13-af57-ed522d15cdf7"
        )  # noqa: SLF001
        == "Untitled"
    )
