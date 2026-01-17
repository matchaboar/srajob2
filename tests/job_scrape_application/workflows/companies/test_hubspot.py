"""Parametrized HubSpot job extraction tests.

These tests call production DBOS workflows to validate job extraction
for HubSpot careers pages. Tests are automatically discovered from
fixture/ground-truth file pairs.

Replaces:
- test_hubspot_convex_job_k1706w2saf5mw428zpz1krzb157yyqsc.py
- test_hubspot_convex_job_k170ypncb9s0rxc75xs4jgs2dh7yvhhd.py
- test_hubspot_convex_job_k178arygp08ynza3gh6dx5k03x7yv210.py
- test_hubspot_convex_job_k17cn3f07694z5rekhg083h2x17ytc1g.py
- test_hubspot_engineering_listing.py

Usage:
    # Run all HubSpot tests
    uv run pytest tests/job_scrape_application/workflows/companies/test_hubspot.py -v
"""

from __future__ import annotations

import pytest

from job_scrape_application.workflows.workflow.company_test_support import (
    CompanyTestCase,
    assert_job_matches_expected,
    discover_company_test_cases,
    get_test_ids,
    run_company_workflow_test,
)

# Discover all HubSpot test cases at module load time
HUBSPOT_TEST_CASES = discover_company_test_cases("hubspot")


@pytest.mark.skipif(
    len(HUBSPOT_TEST_CASES) == 0,
    reason="No HubSpot test cases found (fixture/assertion pairs missing)",
)
@pytest.mark.parametrize("test_case", HUBSPOT_TEST_CASES, ids=get_test_ids(HUBSPOT_TEST_CASES))
@pytest.mark.asyncio
async def test_hubspot_job_extraction(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test HubSpot job extraction calls production workflow.

    Validates that the production scrape_job_detail_batch workflow correctly
    extracts job data from HubSpot careers fixtures.

    Args:
        test_case: CompanyTestCase with fixture and expected values
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None, (
        f"{test_case.identifier}: No jobs extracted from fixture"
    )

    assert_job_matches_expected(result.first_job, test_case.expected, test_case)


@pytest.mark.skipif(
    len(HUBSPOT_TEST_CASES) == 0,
    reason="No HubSpot test cases found",
)
@pytest.mark.parametrize("test_case", HUBSPOT_TEST_CASES, ids=get_test_ids(HUBSPOT_TEST_CASES))
@pytest.mark.asyncio
async def test_hubspot_job_has_description(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that HubSpot jobs have non-empty descriptions.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    description = result.first_job.get("description", "")
    assert description, f"{test_case.identifier}: Description should not be empty"


@pytest.mark.skipif(
    len(HUBSPOT_TEST_CASES) == 0,
    reason="No HubSpot test cases found",
)
@pytest.mark.parametrize("test_case", HUBSPOT_TEST_CASES, ids=get_test_ids(HUBSPOT_TEST_CASES))
@pytest.mark.asyncio
async def test_hubspot_job_has_company_hubspot(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that extracted jobs have company set to 'HubSpot'.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    company = result.first_job.get("company", "")
    assert company.lower() == "hubspot", (
        f"{test_case.identifier}: Company should be 'HubSpot', got {company!r}"
    )
