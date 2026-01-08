from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers import (  # noqa: E402
    HubspotCareersHandler,
    get_site_handler,
)

LISTING_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_page1.html")
LISTING_PAGE_2_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_page2.html")
LISTING_PAGE_3_FIXTURE = Path("tests/fixtures/hubspot_careers_jobs_page3.html")
DETAIL_FIXTURE = Path("tests/fixtures/hubspot_job_detail_commonmark.md")
ENGINEERING_DETAIL_FIXTURE = Path("tests/fixtures/hubspot_job_detail_7294272_commonmark.md")


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
    assert any("hubs_signup-cta=careers-apply" in link for link in links)
    assert not any("hubs_signup-cta=careers-nav-cta" in link for link in links)


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
