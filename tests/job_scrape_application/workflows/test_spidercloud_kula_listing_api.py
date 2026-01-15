from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse


from job_scrape_application.workflows.site_handlers.kula_careers import (  # noqa: E402
    KulaCareersHandler,
)

FIXTURE = Path(
    "tests/job_scrape_application/workflows/fixtures/kula_voltagepark_listing.json"
)


def test_kula_handler_builds_listing_api_url():
    handler = KulaCareersHandler()
    api_url = handler.get_listing_api_uri("https://careers.kula.ai/voltagepark?jobs=true")
    assert api_url

    parsed = urlparse(api_url)
    params = parse_qs(parsed.query)
    assert parsed.path == "/api/internal/ats_job_posts"
    assert params.get("accountName") == ["voltagepark"]
    assert params.get("scope") == ["public"]
    assert params.get("type") == ["ats_job_post.index"]
    assert params.get("items") == ["99"]


def test_kula_handler_extracts_job_urls():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    handler = KulaCareersHandler()
    job_ids = handler.get_links_from_json(payload)
    assert "19780" in job_ids

    api_url = handler.get_listing_api_uri("https://careers.kula.ai/voltagepark")
    filtered = handler.filter_job_urls_for_site(job_ids, api_url)
    assert "https://careers.kula.ai/voltagepark/19780" in filtered


def test_kula_handler_builds_pagination_urls():
    handler = KulaCareersHandler()
    source_url = handler.get_listing_api_uri("https://careers.kula.ai/voltagepark")
    payload = {"meta": {"pages": 3, "page": 1}}

    pagination_urls = handler.get_pagination_urls_from_json(payload, source_url)
    assert any("page=2" in url for url in pagination_urls)
    assert any("page=3" in url for url in pagination_urls)
