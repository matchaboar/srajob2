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
