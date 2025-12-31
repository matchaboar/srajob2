import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.helpers.scrape_utils import derive_company_from_url


def test_derive_company_from_greenhouse_url_uses_slug():
    url = "https://boards.greenhouse.io/robinhood/jobs/123456"
    assert derive_company_from_url(url) == "Robinhood"


def test_derive_company_from_generic_careers_url():
    url = "https://careers.databricks.com/open-roles"
    assert derive_company_from_url(url) == "Databricks"


def test_derive_company_from_workday_url_uses_tenant():
    url = (
        "https://dataminr.wd12.myworkdayjobs.com/en-US/Dataminr/job/"
        "London-UK/Account-Executive--Public-Sector--UK--Ireland---EU-_JR1875?q=engi"
    )
    assert derive_company_from_url(url) == "Dataminr"


def test_derive_company_from_workday_api_url_uses_tenant():
    url = (
        "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/"
        "London-UK/Account-Executive--Public-Sector--UK--Ireland---EU-_JR1875"
    )
    assert derive_company_from_url(url) == "Dataminr"
