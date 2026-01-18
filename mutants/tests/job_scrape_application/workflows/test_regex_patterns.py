from __future__ import annotations

import re

import pytest

from job_scrape_application.workflows.helpers.regex_patterns import ASHBY_JOB_URL_PATTERN


@pytest.mark.parametrize(
    "suffix",
    [
        ")",
        ")\n*",
        ")\n###",
        ")\n###\n",
        "]*",
        "]",
        "*",
        "###",
    ],
)
def test_ashby_job_url_pattern_strips_trailing_junk(suffix: str) -> None:
    base = "https://jobs.ashbyhq.com/notion/a003d9b2-bc51-4f5b-8bca-068f10114308"
    text = f"See {base}{suffix} for details."
    matches = re.findall(ASHBY_JOB_URL_PATTERN, text)
    assert matches == [base]


def test_ashby_job_url_pattern_allows_valid_paths() -> None:
    urls = [
        "https://jobs.ashbyhq.com/lambda/senior-software-engineer",
        "https://jobs.ashbyhq.com/notion/87b03f55-c420-44ed-a9db-61519ea03fa5",
        "https://jobs.ashbyhq.com/ramp/67fadb77-43d8-4449-954b-d4cf2c6d3b8b",
    ]
    text = " | ".join(urls)
    matches = re.findall(ASHBY_JOB_URL_PATTERN, text)
    assert matches == urls
