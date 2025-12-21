from __future__ import annotations

from job_scrape_application.workflows.site_handlers import (
    AshbyHqHandler,
    AvatureHandler,
    GithubCareersHandler,
    GreenhouseHandler,
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
        "https://boards.greenhouse.io/v1/boards/robinhood/jobs"
    )
    config = handler.get_spidercloud_config(api_url)
    assert config.get("return_format") == ["raw_html"]
    assert config.get("preserve_host") is False


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
    html = """
    <a href="https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548">Apply</a>
    <a href="https://bloomberg.avature.net/careers/SearchJobs/engineer/?jobRecordsPerPage=12&jobOffset=12">2</a>
    <a href="https://bloomberg.avature.net/careers/SaveJob?jobId=15548">Save</a>
    """
    assert handler.get_links_from_raw_html(html) == [
        "https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548",
        "https://bloomberg.avature.net/careers/SearchJobs/engineer/?jobRecordsPerPage=12&jobOffset=12",
    ]
