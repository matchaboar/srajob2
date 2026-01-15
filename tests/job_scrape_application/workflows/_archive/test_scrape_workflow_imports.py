from __future__ import annotations

import importlib



def test_scrape_workflow_imports_fail_listing_batch_urls():
    activities = importlib.import_module("job_scrape_application.workflows.activities")
    scrape_workflow = importlib.import_module("job_scrape_application.workflows.scrape_workflow")

    assert scrape_workflow.fail_listing_batch_urls is activities.fail_listing_batch_urls
