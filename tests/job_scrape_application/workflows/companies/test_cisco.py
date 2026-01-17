"""Parametrized Cisco job extraction tests.

These tests call production DBOS workflows to validate job extraction
for Cisco careers pages. Tests are automatically discovered from
fixture/ground-truth file pairs.

Note: Cisco careers pages have complex HTML structure that may not extract
cleanly. These tests document current extraction behavior.

Replaces:
- edge_cases/test_spidercloud_cisco_detail.py
- edge_cases/test_cisco_convex_job_k17bmfbapf9g9rhsz775f2kk717ytyzy.py

Usage:
    # Run all Cisco tests
    uv run pytest tests/job_scrape_application/workflows/companies/test_cisco.py -v
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

# Discover all Cisco test cases at module load time
CISCO_TEST_CASES = discover_company_test_cases("cisco")


@pytest.mark.skipif(
    len(CISCO_TEST_CASES) == 0,
    reason="No Cisco test cases found (fixture/assertion pairs missing)",
)
@pytest.mark.parametrize("test_case", CISCO_TEST_CASES, ids=get_test_ids(CISCO_TEST_CASES))
@pytest.mark.asyncio
async def test_cisco_job_extraction(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test Cisco job extraction calls production workflow.

    Validates that the production scrape_job_detail_batch workflow correctly
    extracts job data from Cisco careers fixtures.

    Note: Cisco's complex HTML structure may result in partial extraction.
    These tests document current behavior.

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
    len(CISCO_TEST_CASES) == 0,
    reason="No Cisco test cases found",
)
@pytest.mark.parametrize("test_case", CISCO_TEST_CASES, ids=get_test_ids(CISCO_TEST_CASES))
@pytest.mark.asyncio
async def test_cisco_job_has_description(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that Cisco jobs have non-empty descriptions.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    description = result.first_job.get("description", "")
    assert description, f"{test_case.identifier}: Description should not be empty"


@pytest.mark.skipif(
    len(CISCO_TEST_CASES) == 0,
    reason="No Cisco test cases found",
)
@pytest.mark.parametrize("test_case", CISCO_TEST_CASES, ids=get_test_ids(CISCO_TEST_CASES))
@pytest.mark.asyncio
async def test_cisco_job_has_company_cisco(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that extracted jobs have company set to 'Cisco'.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    company = result.first_job.get("company", "")
    assert company.lower() == "cisco", (
        f"{test_case.identifier}: Company should be 'Cisco', got {company!r}"
    )
