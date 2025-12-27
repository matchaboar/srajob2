from __future__ import annotations

import html as html_lib
import json
import re
import os
import sys
from pathlib import Path

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers import (  # noqa: E402
    AshbyHqHandler,
    AvatureHandler,
    BaseSiteHandler,
    CiscoCareersHandler,
    GithubCareersHandler,
    GreenhouseHandler,
    NetflixHandler,
    UberCareersHandler,
    get_site_handler,
)


def test_get_site_handler_prefers_site_type():
    handler = get_site_handler("https://example.com", "greenhouse")
    assert isinstance(handler, GreenhouseHandler)


def test_ashby_handler_builds_api_and_links():
    handler = AshbyHqHandler()
    url = "https://jobs.ashbyhq.com/lambda"
    assert handler.matches_url(url)
    assert handler.get_listing_api_uri(url) == "https://api.ashbyhq.com/posting-api/job-board/lambda"
    assert handler.get_company_uri(url) == "https://jobs.ashbyhq.com/lambda"
    payload = {
        "jobs": [
            {"jobUrl": "https://jobs.ashbyhq.com/lambda/senior-software-engineer"},
            {"applyUrl": "https://jobs.ashbyhq.com/lambda/security-engineer"},
        ]
    }
    assert handler.get_links_from_json(payload) == [
        "https://jobs.ashbyhq.com/lambda/senior-software-engineer",
        "https://jobs.ashbyhq.com/lambda/security-engineer",
    ]


def test_greenhouse_handler_rewrites_and_formats():
    handler = GreenhouseHandler()
    detail = "https://coreweave.com/careers/job?4607747006&board=coreweave&gh_jid=4607747006"
    api_url = handler.get_api_uri(detail)
    assert api_url == "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4607747006"
    assert handler.get_company_uri(api_url) == "https://boards.greenhouse.io/coreweave/jobs/4607747006"
    assert handler.get_listing_api_uri("https://api.greenhouse.io/v1/boards/robinhood/jobs") == (
        "https://api.greenhouse.io/v1/boards/robinhood/jobs"
    )
    config = handler.get_spidercloud_config(api_url)
    assert config.get("return_format") == ["raw_html"]
    assert config.get("preserve_host") is False
    config = handler.get_spidercloud_config(detail)
    assert config.get("return_format") == ["raw_html"]
    assert config.get("preserve_host") is True


def test_github_careers_handler_builds_api_and_links():
    handler = GithubCareersHandler()
    url = "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100"
    assert handler.matches_url(url)
    api_url = handler.get_listing_api_uri(url)
    assert api_url is not None
    assert api_url.startswith("https://www.github.careers/api/jobs?")
    assert "keywords=engineer" in api_url
    assert "page=" not in api_url
    payload = {
        "jobs": [
            {"data": {"slug": "4822", "language": "en-us"}},
            {"data": {"slug": "4867", "languages": ["en-us", "fr"]}},
        ]
    }
    assert handler.get_links_from_json(payload) == [
        "https://www.github.careers/careers-home/jobs/4822?lang=en-us",
        "https://www.github.careers/careers-home/jobs/4867?lang=en-us",
    ]


def test_avature_handler_matches_and_extracts_links():
    handler = AvatureHandler()
    url = "https://bloomberg.avature.net/careers/SearchJobs/engineer?jobRecordsPerPage=12"
    assert handler.matches_url(url)
    assert handler.is_listing_url(url)
    assert handler.is_listing_url(
        "https://bloomberg.avature.net/careers/SearchJobsData/engineer?jobOffset=12"
    )
    assert handler.is_listing_url(
        "https://bloomberg.avature.net/careers/searchjobs/engineer"
    )
    assert not handler.is_listing_url(
        "https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548"
    )
    html = """
    <a href="https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548">Apply</a>
    <a href="https://bloomberg.avature.net/careers/SearchJobs/engineer/?jobRecordsPerPage=12&jobOffset=12">2</a>
    <a href="https://bloomberg.avature.net/careers/SaveJob?jobId=15548">Save</a>
    """
    assert handler.get_links_from_raw_html(html) == [
        "https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548",
        "https://bloomberg.avature.net/careers/SearchJobs/engineer/?jobRecordsPerPage=12&jobOffset=12",
    ]


def _extract_first_html(payload: object) -> str:
    if isinstance(payload, dict):
        content = payload.get("content")
        if isinstance(content, dict):
            raw = content.get("raw")
            if isinstance(raw, str) and ("<html" in raw.lower() or "smartapplydata" in raw.lower()):
                return raw
        for key in ("raw_html", "html", "body", "text"):
            val = payload.get(key)
            if isinstance(val, str) and ("<html" in val.lower() or "smartapplydata" in val.lower()):
                return val
        for value in payload.values():
            found = _extract_first_html(value)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _extract_first_html(value)
            if found:
                return found
    return ""


def _extract_json_from_pre(html_text: str) -> dict:
    match = re.search(r"<pre[^>]*>(?P<content>.*?)</pre>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise AssertionError("Unable to locate <pre> JSON block in fixture HTML")
    content = html_lib.unescape(match.group("content")).strip()
    if not content:
        raise AssertionError("Empty <pre> content in fixture HTML")
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise AssertionError("Expected JSON object payload from fixture")
    return parsed


def _load_netflix_api_payload(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    html = _extract_first_html(payload)
    if not html:
        raise AssertionError(f"Unable to extract HTML from {path}")
    return _extract_json_from_pre(html)


def test_netflix_handler_extracts_listing_and_pagination_links():
    handler = NetflixHandler()
    url = "https://explore.jobs.netflix.net/careers?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date"
    assert handler.matches_url(url)
    assert handler.is_listing_url(url)
    api_url = handler.get_listing_api_uri(url)
    assert api_url is not None
    assert "api/apply/v2/jobs" in api_url
    assert "query=engineer" in api_url
    assert "start=0" in api_url
    assert "num=10" in api_url

    fixture_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_listing_page.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    html = _extract_first_html(payload)
    assert html
    links = handler.get_links_from_raw_html(html)
    assert any(link.startswith("https://explore.jobs.netflix.net/careers/job/") for link in links)
    assert any("api/apply/v2/jobs" in link and "start=10" in link for link in links)


def test_netflix_handler_extracts_from_api_pre():
    handler = NetflixHandler()
    html = (
        "<html><pre>{"
        "\"domain\":\"netflix.com\","
        "\"positions\":[{\"canonicalPositionUrl\":\"https://explore.jobs.netflix.net/careers/job/123\"}],"
        "\"count\":15,"
        "\"query\":{\"query\":\"engineer\"}"
        "}</pre></html>"
    )
    links = handler.get_links_from_raw_html(html)
    assert "https://explore.jobs.netflix.net/careers/job/123" in links
    assert any("api/apply/v2/jobs" in link and "start=10" in link for link in links)


def test_netflix_handler_fixture_pages_have_unique_urls():
    handler = NetflixHandler()
    page_1 = _load_netflix_api_payload(
        Path("tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_api_page_1.json")
    )
    page_2 = _load_netflix_api_payload(
        Path("tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_api_page_2.json")
    )
    page_3 = _load_netflix_api_payload(
        Path("tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_api_page_3.json")
    )

    urls_1 = handler.get_links_from_json(page_1)
    urls_2 = handler.get_links_from_json(page_2)
    urls_3 = handler.get_links_from_json(page_3)

    assert urls_1 and urls_2 and urls_3
    assert all(url.startswith("https://explore.jobs.netflix.net/careers/job/") for url in urls_1)
    assert all(url.startswith("https://explore.jobs.netflix.net/careers/job/") for url in urls_2)
    assert all(url.startswith("https://explore.jobs.netflix.net/careers/job/") for url in urls_3)

    assert set(urls_1).isdisjoint(urls_2)
    assert set(urls_1).isdisjoint(urls_3)
    assert set(urls_2).isdisjoint(urls_3)


def test_uber_careers_handler_extracts_listing_and_pagination_links():
    handler = UberCareersHandler()
    url = (
        "https://www.uber.com/us/en/careers/list/"
        "?query=engineer&location=USA-California-San%20Francisco"
        "&location=USA-California-Los%20Angeles"
        "&location=USA-California-Sunnyvale"
        "&location=USA-California-Culver%20City"
        "&location=USA-New%20York-New%20York"
        "&location=USA-Washington-Seattle"
        "&location=USA-Illinois-Chicago"
        "&location=USA-Texas-Dallas"
        "&location=USA-Florida-Miami"
        "&location=USA-Arizona-Phoenix"
        "&location=USA-Georgia-Atlanta"
        "&location=USA-District%20of%20Columbia-Washington"
    )
    assert handler.matches_url(url)
    assert handler.is_listing_url(url)

    fixture_paths = [
        Path(
            "tests/job_scrape_application/workflows/fixtures/spidercloud_uber_careers_listing_page_1.json"
        ),
        Path(
            "tests/job_scrape_application/workflows/fixtures/spidercloud_uber_careers_listing_page_2.json"
        ),
        Path(
            "tests/job_scrape_application/workflows/fixtures/spidercloud_uber_careers_listing_page_3.json"
        ),
    ]
    for fixture_path in fixture_paths:
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        html = _extract_first_html(payload)
        assert html

        links = handler.get_links_from_raw_html(html)
        job_links = [link for link in links if re.search(r"/careers/list/\d+$", link)]
        assert job_links


def test_cisco_careers_handler_extracts_listing_and_pagination_links():
    handler = CiscoCareersHandler()
    url = "https://careers.cisco.com/global/en/search-results?keywords=%22software%20engineer%22&s=1"
    assert handler.matches_url(url)
    assert handler.is_listing_url(url)

    fixture_sets = [
        (
            Path(
                "tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_search_page_1.json"
            ),
            "from=10",
        ),
        (
            Path(
                "tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_search_page_2.json"
            ),
            "from=20",
        ),
        (
            Path(
                "tests/job_scrape_application/workflows/fixtures/spidercloud_cisco_search_page_3.json"
            ),
            "from=30",
        ),
    ]

    for fixture_path, expected_page in fixture_sets:
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        html = _extract_first_html(payload)
        assert html

        links = handler.get_links_from_raw_html(html)
        job_links = [link for link in links if "/global/en/job/" in link]
        assert job_links
        assert any("search-results" in link and expected_page in link for link in links)


class _BaseHandlerForTest(BaseSiteHandler):
    @classmethod
    def matches_url(cls, url: str) -> bool:
        return False


def test_base_handler_extracts_positions_when_jobs_missing():
    handler = _BaseHandlerForTest()
    payload = {
        "positions": [
            {"canonicalPositionUrl": "https://example.com/job/1"},
            {"canonicalPositionUrl": "https://example.com/job/2"},
        ]
    }
    assert handler.get_links_from_json(payload) == [
        "https://example.com/job/1",
        "https://example.com/job/2",
    ]


def test_base_handler_parses_pre_json_positions():
    handler = _BaseHandlerForTest()
    html = (
        "<html><pre>{"
        "\"positions\":[{\"canonicalPositionUrl\":\"https://example.com/job/1\"}]"
        "}</pre></html>"
    )
    assert handler.get_links_from_raw_html(html) == ["https://example.com/job/1"]


def test_base_handler_parses_pre_json_list_payload():
    handler = _BaseHandlerForTest()
    html = (
        "<html><pre>["
        "{\"jobs\":[{\"jobUrl\":\"https://example.com/job/2\"}]}"
        "]</pre></html>"
    )
    assert handler.get_links_from_raw_html(html) == ["https://example.com/job/2"]
