from __future__ import annotations

import importlib
import os
import sys

sys.path.insert(0, os.path.abspath("."))


def test_scrape_workflow_imports_fail_listing_batch_urls():
    activities = importlib.import_module("job_scrape_application.workflows.activities")
    scrape_workflow = importlib.import_module("job_scrape_application.workflows.scrape_workflow")

    assert scrape_workflow.fail_listing_batch_urls is activities.fail_listing_batch_urls
