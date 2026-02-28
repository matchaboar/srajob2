from __future__ import annotations

import html as html_lib
import orjson
import re
from pathlib import Path


from job_scrape_application.workflows.site_handlers import (  # noqa: E402
    AshbyHqHandler,
    AvatureHandler,
    BaseSiteHandler,
    CiscoCareersHandler,
    GithubCareersHandler,
    GreenhouseHandler,
    MetaCareersHandler,
    NetflixHandler,
    UberCareersHandler,
    WorkdayHandler,
    get_site_handler,
)
from tests.job_scrape_application.sites.helpers import load_spidercloud_fixture  # noqa: E402

_PRE_TAG_RE = re.compile(r"<pre[^>]*>(?P<content>.*?)</pre>", flags=re.IGNORECASE | re.DOTALL)
_UBER_JOB_LINK_RE = re.compile(r"/careers/list/\d+$")


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
    # API detail URLs (boards-api.greenhouse.io) should use raw format for JSON
    config = handler.get_spidercloud_config(api_url)
    assert config.get("return_format") == ["raw"]
    assert config.get("request") == "basic"
    assert config.get("preserve_host") is False
    # Marketing site detail URLs should still use commonmark/raw_html for HTML
    config = handler.get_spidercloud_config(detail)
    assert config.get("return_format") == ["commonmark", "raw_html"]
    assert config.get("preserve_host") is True


def test_greenhouse_handler_builds_api_from_board_job_url():
    handler = GreenhouseHandler()
    url = "https://boards.greenhouse.io/samsara/jobs/1234567"
    api_url = handler.get_api_uri(url)
    assert api_url == "https://boards-api.greenhouse.io/v1/boards/samsara/jobs/1234567"


def test_greenhouse_handler_builds_api_from_job_boards_url():
    handler = GreenhouseHandler()
    url = "https://job-boards.greenhouse.io/xai/jobs/5012607007"
    api_url = handler.get_api_uri(url)
    assert api_url == "https://boards-api.greenhouse.io/v1/boards/xai/jobs/5012607007"


def test_greenhouse_handler_builds_api_from_custom_domain_locale_path():
    handler = GreenhouseHandler()
    url = (
        "https://www.coupang.jobs/en/jobs/7471081/"
        "seniorstaff-android-engineer-streaming-player-coupang-play/"
    )
    api_url = handler.get_api_uri(url)
    assert api_url == "https://boards-api.greenhouse.io/v1/boards/coupang/jobs/7471081"


def test_greenhouse_handler_builds_api_from_job_board_path():
    handler = GreenhouseHandler()
    url = "https://app.careerpuck.com/job-board/lyft/job/8304477002?gh_jid=8304477002"
    api_url = handler.get_api_uri(url)
    assert api_url == "https://boards-api.greenhouse.io/v1/boards/lyft/jobs/8304477002"


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
    detail_url = "https://www.github.careers/careers-home/jobs/4852?lang=en-us"
    config = handler.get_spidercloud_config(detail_url)
    assert config.get("return_format") == ["raw_html"]


def test_github_careers_handler_filters_invalid_urls():
    """Test that GitHub careers handler filters out navigation/filter URLs."""
    handler = GithubCareersHandler()

    # Valid job detail URLs (should pass)
    valid_urls = [
        "https://www.github.careers/careers-home/jobs/4632?lang=en-us",
        "https://www.github.careers/careers-home/jobs/4904?lang=en-us",
        "https://www.github.careers/careers-home/jobs/senior-engineer?lang=en-us",
    ]

    # Invalid navigation/filter URLs (should be filtered out)
    invalid_urls = [
        "https://www.github.careers/jobs",
        "https://www.github.careers/jobs?country=Spain",
        "https://www.github.careers/jobs?page=1&categories=Marketing",
        "https://www.github.careers/experienced-professionals/locations",
        "https://www.github.careers/life-at-github/categories",
        "https://www.github.careers/careers-home/categories",
        "https://www.github.careers/careers-home/locations",
        "https://www.github.careers/careers-home/jobs/categories",
        "https://www.github.careers/benefits/locations",
        "https://www.github.careers/benefits/categories",
        "https://www.github.careers/early-in-profession",
        "https://www.github.careers/life-at-github",
        "https://www.github.careers/benefits",
        "https://www.github.careers/careers-home",
    ]

    # Test filter_job_urls with all URLs mixed
    all_urls = valid_urls + invalid_urls
    filtered = handler.filter_job_urls(all_urls)

    # Only valid job URLs should remain
    assert set(filtered) == set(valid_urls), (
        f"Expected only valid URLs, got: {filtered}"
    )

    # Ensure no invalid URLs are in the filtered result
    for url in invalid_urls:
        assert url not in filtered, f"Invalid URL should have been filtered: {url}"


def test_github_careers_handler_is_valid_job_detail_url():
    """Test the _is_valid_job_detail_url helper method."""
    handler = GithubCareersHandler()

    # Valid job detail URLs
    assert handler._is_valid_job_detail_url(
        "https://www.github.careers/careers-home/jobs/4632?lang=en-us"
    )
    assert handler._is_valid_job_detail_url(
        "https://www.github.careers/careers-home/jobs/senior-engineer"
    )

    # Invalid: not github.careers
    assert not handler._is_valid_job_detail_url(
        "https://example.com/careers-home/jobs/4632"
    )

    # Invalid: wrong path structure
    assert not handler._is_valid_job_detail_url(
        "https://www.github.careers/jobs/4632"
    )
    assert not handler._is_valid_job_detail_url(
        "https://www.github.careers/careers-home/jobs"
    )

    # Invalid: navigation paths
    assert not handler._is_valid_job_detail_url(
        "https://www.github.careers/careers-home/jobs/categories"
    )
    assert not handler._is_valid_job_detail_url(
        "https://www.github.careers/careers-home/jobs/locations"
    )


def test_github_careers_handler_looks_like_navigation_url():
    """Test the _looks_like_navigation_url helper method."""
    handler = GithubCareersHandler()

    # Navigation URLs
    assert handler._looks_like_navigation_url("https://www.github.careers/jobs")
    assert handler._looks_like_navigation_url(
        "https://www.github.careers/jobs?country=Spain"
    )
    assert handler._looks_like_navigation_url(
        "https://www.github.careers/life-at-github/categories"
    )
    assert handler._looks_like_navigation_url(
        "https://www.github.careers/benefits/locations"
    )
    assert handler._looks_like_navigation_url(
        "https://www.github.careers/careers-home"
    )

    # Not navigation URLs (job details)
    assert not handler._looks_like_navigation_url(
        "https://www.github.careers/careers-home/jobs/4632?lang=en-us"
    )

    # Not github.careers
    assert not handler._looks_like_navigation_url(
        "https://example.com/jobs"
    )


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


def test_avature_handler_filters_listing_urls_against_source_url():
    handler = AvatureHandler()
    source_url = (
        "https://bloomberg.avature.net/careers/SearchJobs/engineer?"
        "1845=%5B162619%2C162522%2C162483%2C162484%2C162552%2C162508%2C162520%2C162535%5D"
        "&1845_format=3996"
        "&1686=%5B57029%5D"
        "&1686_format=2312"
        "&listFilterMode=1"
        "&jobRecordsPerPage=12"
        "&jobOffset=0"
    )
    valid_listing = (
        "https://bloomberg.avature.net/careers/SearchJobs/engineer?"
        "1845=%5B162619%2C162522%2C162483%2C162484%2C162552%2C162508%2C162520%2C162535%5D"
        "&1845_format=3996"
        "&1686=%5B57029%5D"
        "&1686_format=2312"
        "&listFilterMode=1"
        "&jobRecordsPerPage=12"
        "&jobOffset=12"
    )
    urls = [
        "https://bloomberg.avature.net/careers/SearchJobs?jobOffset=12",
        valid_listing,
        "https://bloomberg.avature.net/careers/SearchJobs/engineer?%3B1845_format=3996&jobOffset=24",
        "https://bloomberg.avature.net/careers/SearchJobs/engineer/feed?jobOffset=12",
        "https://bloomberg.avature.net/careers/SearchJobs/engineer?jobOffset=-2",
        "https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548",
        "https://other.avature.net/careers/SearchJobs/engineer?jobOffset=12",
    ]
    assert set(handler.filter_job_urls_for_site(urls, source_url)) == {
        "https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548",
        valid_listing,
    }


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
    match = _PRE_TAG_RE.search(html_text)
    if not match:
        raise AssertionError("Unable to locate <pre> JSON block in fixture HTML")
    content = html_lib.unescape(match.group("content")).strip()
    if not content:
        raise AssertionError("Empty <pre> content in fixture HTML")
    parsed = orjson.loads(content)
    if not isinstance(parsed, dict):
        raise AssertionError("Expected JSON object payload from fixture")
    return parsed


def _load_netflix_api_payload(path: Path) -> dict:
    payload = load_spidercloud_fixture(path)
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
    payload = load_spidercloud_fixture(fixture_path)
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


def test_meta_careers_handler_matches_and_extracts_links():
    handler = MetaCareersHandler()
    listing_url = (
        "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering&offices[0]=Seattle%2C%20WA"
    )
    detail_url = "https://www.metacareers.com/jobs/100000000000000/"
    detail_profile_url = "https://www.metacareers.com/profile/job_details/677160418622314"
    assert handler.matches_url(listing_url)
    assert handler.is_listing_url(listing_url)
    assert handler.matches_url(detail_url)
    assert not handler.is_listing_url(detail_url)
    assert handler.matches_url(detail_profile_url)
    assert not handler.is_listing_url(detail_profile_url)

    config = handler.get_spidercloud_config(listing_url)
    assert config.get("request") == "chrome"
    assert config.get("return_format") == ["commonmark", "raw_html"]
    assert config.get("return_page_links") is True
    assert isinstance(config.get("execution_scripts"), dict)
    assert "*" in config.get("execution_scripts")
    wait_for = config.get("wait_for")
    assert isinstance(wait_for, dict)
    assert wait_for.get("selector", {}).get("selector") == "#meta-jobs"

    html = (
        '<a href="/profile/job_details/677160418622314">Job</a>'
        '<a href="/jobs/100000000000000/">Job 2</a>'
        '<a href="/profile/job_details/727671609895617)[###">Job 3</a>'
        '<a href="/jobsearch/?teams[0]=Software%20Engineering&page=2">Next</a>'
        '<a href="/culture">Culture</a>'
    )
    links = handler.get_links_from_raw_html(html)
    assert "https://www.metacareers.com/profile/job_details/677160418622314" in links
    assert "https://www.metacareers.com/profile/job_details/100000000000000" in links
    assert "https://www.metacareers.com/profile/job_details/727671609895617" in links
    assert any("jobsearch" in link and "page=2" in link for link in links)
    assert "https://www.metacareers.com/culture" not in links


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
        payload = load_spidercloud_fixture(fixture_path)
        html = _extract_first_html(payload)
        assert html

        links = handler.get_links_from_raw_html(html)
        job_links = [link for link in links if _UBER_JOB_LINK_RE.search(link)]
        assert job_links
        assert any("page=" in link for link in links if "careers/list" in link)


def test_uber_careers_handler_pagination_pages_have_jobs():
    handler = UberCareersHandler()
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

    first_payload = load_spidercloud_fixture(fixture_paths[0])
    first_html = _extract_first_html(first_payload)
    assert first_html
    first_links = handler.get_links_from_raw_html(first_html)
    pagination_links = [
        link
        for link in first_links
        if "careers/list" in link and "page=" in link
    ]
    assert pagination_links
    assert any("page=1" in link for link in pagination_links)

    for fixture_path in fixture_paths:
        payload = load_spidercloud_fixture(fixture_path)
        html = _extract_first_html(payload)
        assert html
        links = handler.get_links_from_raw_html(html)
        job_links = [link for link in links if _UBER_JOB_LINK_RE.search(link)]
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
        payload = load_spidercloud_fixture(fixture_path)
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


def test_base_handler_filters_non_job_detail_urls():
    handler = _BaseHandlerForTest()
    urls = [
        "https://www.linkedin.com/company/adobe",
        "https://careers.adobe.com/us/en/c/information-technology-jobs",
        "https://www.adobe.com/creativecloud/buy/students.html",
        "https://careers.adobe.com/us/en/c/engineering-and-product-jobs",
        "https://careers.adobe.com/us/en/teams",
        "https://careers.adobe.com/us/en/apply?jobSeqNo=ADOBUSR162038EXTERNALENUS",
        "https://bloomberg.avature.net/careers/SaveJob?jobId=14551",
        "https://affable-kiwi-46.convex.site/share/job?id=example&app=https%3A%2F%2Fsrajob.netlify.app",
        "https://www.linkedin.com/company/bloomberg-lp",
        "https://www.linkedin.com/jobs/view/1234567890/",
        "https://careers.adobe.com/us/en/job/123456/Senior-Engineer",
        "https://boards.greenhouse.io/coreweave/jobs/4607747006",
    ]
    filtered = handler.filter_job_urls(urls)
    for blocked in urls[:8]:
        assert blocked not in filtered
    assert "https://www.linkedin.com/jobs/view/1234567890/" in filtered
    assert "https://careers.adobe.com/us/en/job/123456/Senior-Engineer" in filtered
    assert "https://boards.greenhouse.io/coreweave/jobs/4607747006" in filtered


def test_base_handler_keeps_adobe_listing_urls():
    handler = _BaseHandlerForTest()
    urls = [
        "https://careers.adobe.com/us/en/search-results?keywords=engineer",
        "https://careers.adobe.com/us/en/search-results?from=10&s=1",
        "https://careers.adobe.com/us/en/job/R162737/Research-Engineer",
    ]
    filtered = handler.filter_job_urls(urls)
    for url in urls:
        assert url in filtered


def test_base_handler_filters_meta_non_job_detail_urls():
    handler = _BaseHandlerForTest()
    urls = [
        "https://www.meta.com/media-gallery",
        "https://www.metacareers.com/accessibility-and-engagement",
        "https://www.linkedin.com/company/meta",
        "https://www.metacareers.com/culture",
        "https://www.metacareers.com/nyc-disclosure-notice",
        "https://www.metacareers.com/teams/business",
        "https://www.metacareers.com/rotational-programs",
        "https://www.investor.atmeta.com/home/default.aspx",
        "https://www.meta.com/about/company-info",
        "https://www.metacareers.com/blog",
        "https://www.meta.com/brand/resources",
        "https://www.facebook.com/LifeAtMeta",
        "https://www.twitter.com/MetaforBusiness",
        "https://www.metacareers.com/profile/info",
        "https://www.instagram.com/lifeatmeta",
        "https://www.metacareers.com/accommodations_request",
        "https://transparency.meta.com/policies/community-standards",
        "https://www.metacareers.com/profile/job_details/677160418622314",
        "https://www.metacareers.com/jobs/100000000000000/",
        "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering",
    ]
    filtered = handler.filter_job_urls(urls)
    for blocked in urls[:17]:
        assert blocked not in filtered
    assert "https://www.metacareers.com/profile/job_details/677160418622314" in filtered
    assert "https://www.metacareers.com/jobs/100000000000000/" in filtered
    assert "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering" in filtered


def test_base_handler_filters_locale_careers_list_urls():
    handler = _BaseHandlerForTest()
    urls = [
        "https://www.uber.com/br/pt-br/careers/list",
        "https://www.uber.com/gt/es/careers/list",
        "https://careers.example.com/ae/ar/careers/list/",
        "https://jobs.sample.org/us/en/careers/list",
        "https://www.uber.com/us/en/careers/list?query=engineer",
        "https://careers.example.com/us/en/careers/list?page=2",
        "https://jobs.sample.org/us/en/careers/listing",
        "https://jobs.sample.org/us/eng/careers/list",
        "https://jobs.sample.org/us/en/careers/list/123",
    ]
    filtered = handler.filter_job_urls(urls)
    for blocked in urls[:4]:
        assert blocked not in filtered
    for kept in urls[4:]:
        assert kept in filtered


def test_base_handler_drop_source_listing_url():
    handler = _BaseHandlerForTest()
    source_url = "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering&page=4"
    urls = [
        "https://www.metacareers.com/jobsearch?teams[0]=Software%20Engineering&page=4",
        "/jobsearch?teams[0]=Software%20Engineering&page=4",
        "https://www.metacareers.com/profile/job_details/1092822929374881",
    ]
    cleaned = handler.drop_source_listing_url(urls, source_url)
    assert "https://www.metacareers.com/profile/job_details/1092822929374881" in cleaned
    assert len(cleaned) == 1


def test_base_handler_filters_navigation_urls():
    """Test that base handler filters common navigation URL patterns."""
    handler = _BaseHandlerForTest()
    # Navigation URLs that should be filtered
    navigation_urls = [
        "https://careers.example.com/categories",
        "https://careers.example.com/locations",
        "https://careers.example.com/teams",
        "https://careers.example.com/departments",
        "https://careers.example.com/about",
        "https://careers.example.com/benefits",
        "https://careers.example.com/culture",
        "https://careers.example.com/life-at-company/locations",
        "https://careers.example.com/search",
        "https://careers.example.com/careers/jobs",  # ends in /jobs without ID
        "https://example.com/jobs",  # just /jobs
        "https://example.com/company/jobs/",  # /jobs/ at end
    ]
    # Valid job URLs that should pass through
    valid_urls = [
        "https://careers.example.com/jobs/12345",  # has job ID
        "https://careers.example.com/job/senior-engineer",
        "https://careers.example.com/positions/123456",
        "https://boards.greenhouse.io/company/jobs/4607747006",
    ]
    all_urls = navigation_urls + valid_urls
    filtered = handler.filter_job_urls(all_urls)

    # All navigation URLs should be filtered out
    for url in navigation_urls:
        assert url not in filtered, f"Navigation URL should be filtered: {url}"

    # All valid job URLs should remain
    for url in valid_urls:
        assert url in filtered, f"Valid job URL should remain: {url}"


def test_workday_handler_get_company_uri_broadcom():
    handler = WorkdayHandler()
    api_url = (
        "https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/"
        "External_Career/job/USA-CA-Irvine/IC-Design-Engineer_R023525"
    )
    expected = (
        "https://broadcom.wd1.myworkdayjobs.com/"
        "External_Career/job/USA-CA-Irvine/IC-Design-Engineer_R023525"
    )
    assert handler.get_company_uri(api_url) == expected


def test_workday_handler_get_company_uri_dataminr():
    handler = WorkdayHandler()
    api_url = (
        "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/"
        "Dataminr/job/Melbourne-AU/Customer-Success-Associate_JR1945"
    )
    expected = (
        "https://dataminr.wd12.myworkdayjobs.com/"
        "Dataminr/job/Melbourne-AU/Customer-Success-Associate_JR1945"
    )
    assert handler.get_company_uri(api_url) == expected


def test_workday_handler_get_company_uri_non_api_url_returns_none():
    handler = WorkdayHandler()
    marketing_url = (
        "https://broadcom.wd1.myworkdayjobs.com/"
        "External_Career/job/USA-CA-Irvine/IC-Design-Engineer_R023525"
    )
    assert handler.get_company_uri(marketing_url) is None


def test_workday_handler_get_company_uri_non_workday_url_returns_none():
    handler = WorkdayHandler()
    assert handler.get_company_uri("https://boards.greenhouse.io/acme/jobs/123") is None
