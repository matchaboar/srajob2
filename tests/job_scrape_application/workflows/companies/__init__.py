"""Company-specific workflow tests.

This package contains parametrized tests for company job extraction workflows.
Tests call production DBOS workflow code directly, mocking only the data layer.

Pattern:
    Each test module corresponds to a company (e.g., test_adobe.py, test_hubspot.py)
    and uses parametrization to test multiple job fixtures from that company.

Usage:
    # Run all company tests
    uv run pytest tests/job_scrape_application/workflows/companies/ -v

    # Run specific company tests
    uv run pytest tests/job_scrape_application/workflows/companies/test_adobe.py -v

    # Run with specific fixture
    uv run pytest "tests/job_scrape_application/workflows/companies/test_adobe.py[adobe_r158340]" -v
"""
