from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers import (  # noqa: E402
    HubspotCareersHandler,
    get_site_handler,
)
from tests.job_scrape_application.sites.helpers import load_spidercloud_fixture  # noqa: E402

LISTING_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_page1.html")
LISTING_PAGE_2_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_page2.html")
LISTING_PAGE_3_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_page3.html")
DETAIL_FIXTURE = Path("tests/fixtures/hubspot_job_detail_commonmark.md")
ENGINEERING_DETAIL_FIXTURE = Path("tests/fixtures/hubspot_job_detail_7294272_commonmark.md")
GRAPHQL_FIXTURE = Path("tests/fixtures/spidercloud_hubspot_graphql_jobs.json")


def _load_hubspot_graphql_payload() -> dict:
    payload = load_spidercloud_fixture(GRAPHQL_FIXTURE)
    raw = payload[0][0]["content"]["raw"]
    return json.loads(raw)


def test_hubspot_handler_matches_and_extracts_links():
    handler = HubspotCareersHandler()
    listing_url = (
        "https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1"
        "#office=toronto,san-francisco,remote,cambridge;"
    )
    detail_url = "https://www.hubspot.com/careers/jobs/5986323"

    assert handler.matches_url(listing_url)
    assert handler.is_listing_url(listing_url)
    assert handler.matches_url(detail_url)
    assert not handler.is_listing_url(detail_url)
    assert isinstance(get_site_handler(listing_url), HubspotCareersHandler)

    html = LISTING_FIXTURE.read_text(encoding="utf-8")
    links = handler.get_links_from_raw_html(html)

    assert links
    assert any(link.endswith("/careers/jobs/5986323") for link in links)
    assert any("page=2" in link for link in links)
    assert all(link.startswith("https://www.hubspot.com/careers/jobs") for link in links)
    assert not any("hubs_signup-cta=" in link for link in links)


def test_hubspot_handler_normalizes_markdown():
    handler = HubspotCareersHandler()
    markdown = DETAIL_FIXTURE.read_text(encoding="utf-8")
    cleaned, title = handler.normalize_markdown(markdown)

    assert title == "Account Executive, Corporate - Benelux"
    assert "Apply for This Job" not in cleaned
    assert "Submit Your Application" not in cleaned
    assert "What's the recruiting process like at HubSpot?" not in cleaned


def test_hubspot_handler_extracts_location_hint():
    handler = HubspotCareersHandler()
    markdown = DETAIL_FIXTURE.read_text(encoding="utf-8")
    location = handler.extract_location_hint(markdown)
    assert location == "Remote - Netherlands"


def test_hubspot_handler_pagination_first_three_pages():
    handler = HubspotCareersHandler()

    page_1_links = handler.get_links_from_raw_html(
        LISTING_FIXTURE.read_text(encoding="utf-8")
    )
    page_2_links = handler.get_links_from_raw_html(
        LISTING_PAGE_2_FIXTURE.read_text(encoding="utf-8")
    )
    page_3_links = handler.get_links_from_raw_html(
        LISTING_PAGE_3_FIXTURE.read_text(encoding="utf-8")
    )

    assert any("page=2" in link for link in page_1_links)
    assert any("page=3" in link for link in page_1_links)

    assert "https://www.hubspot.com/careers/jobs" in page_2_links
    assert any("page=3" in link for link in page_2_links)

    assert any("page=2" in link for link in page_3_links)
    assert any("page=4" in link for link in page_3_links)


def test_hubspot_handler_normalizes_engineering_markdown():
    handler = HubspotCareersHandler()
    markdown = ENGINEERING_DETAIL_FIXTURE.read_text(encoding="utf-8")
    cleaned, title = handler.normalize_markdown(markdown)

    assert title == "Engineering Lead"
    assert "Apply for This Job" not in cleaned
    assert "Submit Your Application" not in cleaned


def test_hubspot_handler_extracts_engineering_location_hint():
    handler = HubspotCareersHandler()
    markdown = ENGINEERING_DETAIL_FIXTURE.read_text(encoding="utf-8")
    location = handler.extract_location_hint(markdown)

    assert location == "Remote - USA"


def test_hubspot_handler_spidercloud_config():
    handler = HubspotCareersHandler()
    listing_url = "https://www.hubspot.com/careers/jobs?page=1"
    detail_url = "https://www.hubspot.com/careers/jobs/5986323"

    listing_config = handler.get_spidercloud_config(listing_url)
    assert listing_config.get("return_format") == ["raw_html"]
    assert listing_config.get("request") == "chrome"

    detail_config = handler.get_spidercloud_config(detail_url)
    assert detail_config.get("return_format") == ["commonmark"]


def test_hubspot_handler_trims_signup_cta_from_detail_urls():
    handler = HubspotCareersHandler()
    url = "https://www.hubspot.com/careers/jobs/5986323?hubs_signup-cta=careers-apply"
    trimmed = handler.filter_job_urls([url])
    assert trimmed == ["https://www.hubspot.com/careers/jobs/5986323"]


def test_hubspot_handler_extracts_links_from_graphql_payload():
    handler = HubspotCareersHandler()
    payload = _load_hubspot_graphql_payload()
    links = handler.get_links_from_json(payload)
    jobs = payload.get("data", {}).get("jobs", [])
    job_ids = {str(job.get("id")) for job in jobs if isinstance(job, dict) and job.get("id")}

    assert links
    assert all(link.startswith("https://www.hubspot.com/careers/jobs/") for link in links)
    assert all(link.rsplit("/", 1)[-1].isdigit() for link in links)
    assert {link.rsplit("/", 1)[-1] for link in links} == job_ids


def test_hubspot_handler_builds_graphql_listing_url():
    handler = HubspotCareersHandler()
    listing_url = "https://www.hubspot.com/careers/jobs?page=1&q=engineering"
    api_url = handler.get_listing_api_uri(listing_url)

    assert api_url is not None
    parsed = urlparse(api_url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "wtcfns.hubspot.com"
    assert parsed.path == "/careers/graphql"

    params = parse_qs(parsed.query)
    assert "query" in params
    assert "variables" in params
    assert "jobs" in params["query"][0]

    variables = json.loads(params["variables"][0])
    assert variables["searchQuery"] == "engineering"
