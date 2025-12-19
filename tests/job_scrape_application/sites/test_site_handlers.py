from __future__ import annotations

from job_scrape_application.workflows.site_handlers import (
    AshbyHqHandler,
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

