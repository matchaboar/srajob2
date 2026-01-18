"""Parametrized Microsoft job extraction tests.

These tests call production DBOS workflows to validate job extraction
for Microsoft careers pages. Tests are automatically discovered from
fixture/ground-truth file pairs.

Replaces:
- edge_cases/test_spidercloud_microsoft_listing_api.py
- edge_cases/test_microsoft_convex_job_k17b53z6txnbwffc5ztyxf2cks7ytade.py
- edge_cases/test_microsoft_convex_job_k172wr4a5r7d6nzvz3hd14pa1x7yvc5t.py

Usage:
    # Run all Microsoft tests
    uv run pytest tests/job_scrape_application/workflows/companies/test_microsoft.py -v
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

# Discover all Microsoft test cases at module load time
MICROSOFT_TEST_CASES = discover_company_test_cases("microsoft")


@pytest.mark.skipif(
    len(MICROSOFT_TEST_CASES) == 0,
    reason="No Microsoft test cases found (fixture/assertion pairs missing)",
)
@pytest.mark.parametrize("test_case", MICROSOFT_TEST_CASES, ids=get_test_ids(MICROSOFT_TEST_CASES))
@pytest.mark.asyncio
async def test_microsoft_job_extraction(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test Microsoft job extraction calls production workflow.

    Validates that the production scrape_job_detail_batch workflow correctly
    extracts job data from Microsoft careers fixtures.

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
    len(MICROSOFT_TEST_CASES) == 0,
    reason="No Microsoft test cases found",
)
@pytest.mark.parametrize("test_case", MICROSOFT_TEST_CASES, ids=get_test_ids(MICROSOFT_TEST_CASES))
@pytest.mark.asyncio
async def test_microsoft_job_has_description(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that Microsoft jobs have non-empty descriptions.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    description = result.first_job.get("description", "")
    assert description, f"{test_case.identifier}: Description should not be empty"


@pytest.mark.skipif(
    len(MICROSOFT_TEST_CASES) == 0,
    reason="No Microsoft test cases found",
)
@pytest.mark.parametrize("test_case", MICROSOFT_TEST_CASES, ids=get_test_ids(MICROSOFT_TEST_CASES))
@pytest.mark.asyncio
async def test_microsoft_job_has_company_microsoft(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that extracted jobs have company set to 'Microsoft'.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    company = result.first_job.get("company", "")
    assert company.lower() == "microsoft", (
        f"{test_case.identifier}: Company should be 'Microsoft', got {company!r}"
    )
