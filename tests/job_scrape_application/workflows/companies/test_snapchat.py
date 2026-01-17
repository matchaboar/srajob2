"""Parametrized Snapchat (Snap) job extraction tests.

These tests call production DBOS workflows to validate job extraction
for Snap careers pages. Tests are automatically discovered from
fixture/ground-truth file pairs.

Replaces:
- edge_cases/test_spidercloud_snapchat_job_detail.py
- edge_cases/test_snapchat_job_detail_convex.py
- edge_cases/test_snapchat_convex_job_k17akjbw.py

Usage:
    # Run all Snapchat tests
    uv run pytest tests/job_scrape_application/workflows/companies/test_snapchat.py -v
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

# Discover all Snapchat test cases at module load time
SNAPCHAT_TEST_CASES = discover_company_test_cases("snapchat")


@pytest.mark.skipif(
    len(SNAPCHAT_TEST_CASES) == 0,
    reason="No Snapchat test cases found (fixture/assertion pairs missing)",
)
@pytest.mark.parametrize("test_case", SNAPCHAT_TEST_CASES, ids=get_test_ids(SNAPCHAT_TEST_CASES))
@pytest.mark.asyncio
async def test_snapchat_job_extraction(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test Snapchat job extraction calls production workflow.

    Validates that the production scrape_job_detail_batch workflow correctly
    extracts job data from Snap careers fixtures.

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
    len(SNAPCHAT_TEST_CASES) == 0,
    reason="No Snapchat test cases found",
)
@pytest.mark.parametrize("test_case", SNAPCHAT_TEST_CASES, ids=get_test_ids(SNAPCHAT_TEST_CASES))
@pytest.mark.asyncio
async def test_snapchat_job_has_description(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that Snapchat jobs have non-empty descriptions.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    description = result.first_job.get("description", "")
    assert description, f"{test_case.identifier}: Description should not be empty"


@pytest.mark.skipif(
    len(SNAPCHAT_TEST_CASES) == 0,
    reason="No Snapchat test cases found",
)
@pytest.mark.parametrize("test_case", SNAPCHAT_TEST_CASES, ids=get_test_ids(SNAPCHAT_TEST_CASES))
@pytest.mark.asyncio
async def test_snapchat_job_has_company_snap(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that extracted jobs have company set to 'Snap'.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    company = result.first_job.get("company", "")
    assert company.lower() == "snap", (
        f"{test_case.identifier}: Company should be 'Snap', got {company!r}"
    )
