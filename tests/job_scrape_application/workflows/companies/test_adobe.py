"""Parametrized Adobe job extraction tests.

These tests call production DBOS workflows to validate job extraction
for Adobe careers pages. Tests are automatically discovered from
fixture/ground-truth file pairs.

Replaces:
- test_spidercloud_adobe_job_detail_r158340.py
- test_spidercloud_adobe_job_detail_r160885.py
- test_spidercloud_adobe_job_detail_r161751.py
- test_spidercloud_adobe_job_detail_r161977.py
- test_spidercloud_adobe_job_detail_r163844.py
- test_spidercloud_adobe_apply_detail.py

Usage:
    # Run all Adobe tests
    uv run pytest tests/job_scrape_application/workflows/companies/test_adobe.py -v

    # Run specific fixture
    uv run pytest "tests/.../test_adobe.py[adobe_careers_20260114T134509]" -v
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

# Discover all Adobe test cases at module load time
ADOBE_TEST_CASES = discover_company_test_cases("adobe")


@pytest.mark.skipif(
    len(ADOBE_TEST_CASES) == 0,
    reason="No Adobe test cases found (fixture/assertion pairs missing)",
)
@pytest.mark.parametrize("test_case", ADOBE_TEST_CASES, ids=get_test_ids(ADOBE_TEST_CASES))
@pytest.mark.asyncio
async def test_adobe_job_extraction(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test Adobe job extraction calls production workflow.

    Validates that the production scrape_job_detail_batch workflow correctly
    extracts job data from Adobe careers fixtures.

    Args:
        test_case: CompanyTestCase with fixture and expected values
        workflow_test: WorkflowTest fixture from conftest
    """
    # Run production workflow
    result = await run_company_workflow_test(test_case, workflow_test)

    # Verify we got at least one job
    assert result.first_job is not None, (
        f"{test_case.identifier}: No jobs extracted from fixture"
    )

    # Verify expected fields
    assert_job_matches_expected(result.first_job, test_case.expected, test_case)


@pytest.mark.skipif(
    len(ADOBE_TEST_CASES) == 0,
    reason="No Adobe test cases found",
)
@pytest.mark.parametrize("test_case", ADOBE_TEST_CASES, ids=get_test_ids(ADOBE_TEST_CASES))
@pytest.mark.asyncio
async def test_adobe_job_has_description(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that Adobe jobs have non-empty descriptions.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    description = result.first_job.get("description", "")
    assert description, f"{test_case.identifier}: Description should not be empty"
    assert len(description) > 100, (
        f"{test_case.identifier}: Description seems too short ({len(description)} chars)"
    )


@pytest.mark.skipif(
    len(ADOBE_TEST_CASES) == 0,
    reason="No Adobe test cases found",
)
@pytest.mark.parametrize("test_case", ADOBE_TEST_CASES, ids=get_test_ids(ADOBE_TEST_CASES))
@pytest.mark.asyncio
async def test_adobe_job_has_company_adobe(
    test_case: CompanyTestCase,
    workflow_test,
) -> None:
    """Test that extracted jobs have company set to 'Adobe'.

    Args:
        test_case: CompanyTestCase with fixture data
        workflow_test: WorkflowTest fixture from conftest
    """
    result = await run_company_workflow_test(test_case, workflow_test)

    assert result.first_job is not None
    company = result.first_job.get("company", "")
    assert company.lower() == "adobe", (
        f"{test_case.identifier}: Company should be 'Adobe', got {company!r}"
    )
