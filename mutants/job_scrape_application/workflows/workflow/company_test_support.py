"""
Shared fixtures and utilities for company-specific workflow tests.

These tests validate that PRODUCTION workflows correctly extract job data.
Tests call actual DBOS workflow code, mocking only external dependencies
(SpiderCloud responses, Convex operations).

Key Patterns:
1. Tests call production workflow code directly (not internal methods)
2. Mock only the data layer (SpiderCloud, Convex)
3. Parametrize tests by fixture files for comprehensive coverage
4. Ground truth YAML files define expected extraction results

Directory Structure:
    fixtures/dbos_schedule/{site}_detail.json - Primary fixtures
    fixtures/debug/{company}/*_detail.json - Debug/additional fixtures
    ground_truth/{site}.yml - Ground truth for dbos_schedule
    ground_truth/debug/{company}/*.yml - Ground truth for debug fixtures
"""

from __future__ import annotations

import orjson
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Directories (relative to repo root)
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth")


@dataclass
class CompanyTestCase:
    """Test case for company-specific workflow tests.

    This class encapsulates all data needed to test a single job extraction
    workflow, including the fixture path, expected values, and metadata.

    Attributes:
        identifier: Unique test identifier (used for pytest parametrization)
        company: Company name (e.g., "Adobe", "HubSpot")
        fixture_path: Path to SpiderCloud fixture JSON file
        assertion_path: Path to ground truth YAML file
        url: Job detail URL from the fixture
        source_url: Source/listing URL that led to this job
        expected: Dict of expected field values from ground truth
        site_id: Site identifier (e.g., "adobe_careers", "greenhouse")
    """

    identifier: str
    company: str
    fixture_path: Path
    assertion_path: Path
    url: str = ""
    source_url: str = ""
    expected: dict[str, Any] = field(default_factory=dict)
    site_id: str = ""

    @classmethod
    def from_paths(
        cls,
        fixture_path: Path,
        assertion_path: Path,
        company: str,
    ) -> "CompanyTestCase":
        """Load test case from fixture and assertion files.

        Args:
            fixture_path: Path to fixture JSON file
            assertion_path: Path to assertion YAML file
            company: Company name

        Returns:
            CompanyTestCase instance
        """
        identifier = assertion_path.stem

        # Load fixture to extract URL
        fixture_data = orjson.loads(fixture_path.read_text(encoding="utf-8"))
        url = ""
        source_url = ""

        if isinstance(fixture_data, dict):
            request = fixture_data.get("request", {})
            url = request.get("url", "")
            source_url = fixture_data.get("source_url", url)
        elif isinstance(fixture_data, list):
            # Legacy list format
            if fixture_data and isinstance(fixture_data[0], list) and fixture_data[0]:
                first_item = fixture_data[0][0]
                if isinstance(first_item, dict):
                    url = first_item.get("url", "")

        # Load assertion
        assertion_data = yaml.safe_load(assertion_path.read_text(encoding="utf-8"))
        expected = assertion_data.get("expected", {})
        site_id = assertion_data.get("site_id", "")

        # Override URL from assertion if provided
        if assertion_data.get("detail_url"):
            url = assertion_data["detail_url"]

        return cls(
            identifier=identifier,
            company=company,
            fixture_path=fixture_path,
            assertion_path=assertion_path,
            url=url,
            source_url=source_url or url,
            expected=expected,
            site_id=site_id,
        )


def discover_company_test_cases(company: str) -> list[CompanyTestCase]:
    """Discover all test cases for a specific company.

    Searches for fixture/assertion pairs in:
    1. fixtures/dbos_schedule/ with matching ground_truth/*.yml
    2. fixtures/debug/{company}/ with matching ground_truth/debug/{company}/*.yml

    Args:
        company: Company name (lowercase, e.g., "adobe", "hubspot")

    Returns:
        List of CompanyTestCase instances
    """
    test_cases: list[CompanyTestCase] = []
    company_lower = company.lower()

    # Search dbos_schedule fixtures
    dbos_schedule_dir = FIXTURE_DIR / "dbos_schedule"
    if dbos_schedule_dir.exists():
        for fixture_path in dbos_schedule_dir.glob("*_detail.json"):
            # Check if fixture name matches company
            fixture_name = fixture_path.stem.replace("_detail", "")
            if company_lower in fixture_name.lower():
                assertion_path = GROUND_TRUTH_DIR / f"{fixture_name}.yml"
                if assertion_path.exists():
                    try:
                        test_cases.append(
                            CompanyTestCase.from_paths(
                                fixture_path, assertion_path, company
                            )
                        )
                    except Exception as e:
                        logger.warning(f"Failed to load test case {fixture_name}: {e}")

    # Search debug fixtures for company
    debug_fixture_dir = FIXTURE_DIR / "debug" / company_lower
    if debug_fixture_dir.exists():
        for fixture_path in debug_fixture_dir.glob("*_detail.json"):
            identifier = fixture_path.stem.replace("_detail", "")
            assertion_path = GROUND_TRUTH_DIR / "debug" / company_lower / f"{identifier}.yml"
            if assertion_path.exists():
                try:
                    test_cases.append(
                        CompanyTestCase.from_paths(
                            fixture_path, assertion_path, company
                        )
                    )
                except Exception as e:
                    logger.warning(f"Failed to load debug test case {identifier}: {e}")

    return test_cases


def discover_all_test_cases_for_companies(
    companies: list[str],
) -> dict[str, list[CompanyTestCase]]:
    """Discover test cases for multiple companies.

    Args:
        companies: List of company names

    Returns:
        Dict mapping company name to list of test cases
    """
    return {company: discover_company_test_cases(company) for company in companies}


def get_test_ids(test_cases: list[CompanyTestCase]) -> list[str]:
    """Get test IDs for pytest parametrization.

    Args:
        test_cases: List of CompanyTestCase instances

    Returns:
        List of identifier strings
    """
    return [tc.identifier for tc in test_cases]


class CompanyWorkflowTestResult:
    """Result from running a company workflow test."""

    def __init__(
        self,
        jobs: list[dict[str, Any]],
        scrapes: list[dict[str, Any]],
        step_calls: dict[str, list[dict[str, Any]]],
    ):
        self.jobs = jobs
        self.scrapes = scrapes
        self.step_calls = step_calls

    @property
    def first_job(self) -> dict[str, Any] | None:
        """Get the first extracted job, if any."""
        return self.jobs[0] if self.jobs else None

    def get_jobs_for_url(self, url: str) -> list[dict[str, Any]]:
        """Get all jobs matching a URL."""
        return [j for j in self.jobs if j.get("url") == url]


def _process_fixture_to_jobs(fixture_data: dict[str, Any], url: str) -> list[dict[str, Any]]:
    """Process fixture data using production extractors."""
    from job_scrape_application.workflows.extractors.integration import extract_job_from_scrape
    from job_scrape_application.workflows.site_handlers import get_site_handler
    from job_scrape_application.workflows.site_handlers.base import BaseSiteHandler

    def _extract_structured_data(raw_html: str) -> dict[str, Any] | None:
        if not raw_html:
            return None
        parsed = BaseSiteHandler._extract_json_payload_from_html(raw_html)  # noqa: SLF001
        if isinstance(parsed, dict):
            return parsed
        if raw_html.strip().startswith("{"):
            try:
                parsed = orjson.loads(raw_html)
            except orjson.JSONDecodeError:
                return None
            if isinstance(parsed, dict):
                data_obj = parsed.get("data")
                if isinstance(data_obj, dict):
                    return data_obj
                return parsed
            return None
        return None

    def _process_item(item: dict[str, Any]) -> dict[str, Any] | None:
        content = item.get("content", item)
        if not isinstance(content, dict):
            return None
        raw_html = content.get("raw") or ""
        commonmark = content.get("commonmark") or ""

        def _is_trivial_commonmark(value: str) -> bool:
            stripped = value.strip()
            if not stripped:
                return True
            if len(stripped) < 200 and len(stripped.splitlines()) <= 3:
                return True
            return False

        if isinstance(commonmark, str) and not _is_trivial_commonmark(commonmark):
            markdown = commonmark
        else:
            markdown = raw_html or commonmark or ""

        if not markdown:
            return None
        handler = get_site_handler(url)
        structured_data = _extract_structured_data(raw_html or markdown)
        return extract_job_from_scrape(
            url=url,
            markdown=markdown,
            raw_row=item,
            handler=handler,
            structured_data=structured_data,
            debug=False,
        )

    jobs: list[dict[str, Any]] = []
    response = fixture_data.get("response", [])

    if isinstance(response, list):
        for item in response:
            if isinstance(item, str):
                try:
                    parsed_item = orjson.loads(item.strip())
                except (orjson.JSONDecodeError, TypeError):
                    continue
                if isinstance(parsed_item, dict):
                    job = _process_item(parsed_item)
                    if job:
                        jobs.append(job)
            elif isinstance(item, list):
                for sub_item in item:
                    if isinstance(sub_item, dict):
                        job = _process_item(sub_item)
                        if job:
                            jobs.append(job)
            elif isinstance(item, dict):
                job = _process_item(item)
                if job:
                    jobs.append(job)
    elif isinstance(response, dict):
        job = _process_item(response)
        if job:
            jobs.append(job)

    return jobs


async def run_company_workflow_test(
    test_case: CompanyTestCase,
    workflow_test: Any,
) -> CompanyWorkflowTestResult:
    """Run production workflow and return extracted job data.

    This function:
    1. Loads the fixture and pre-processes it to extract job data
    2. Mocks scrape_job_details to return the fixture data
    3. Mocks ingest_jobs_from_scrape_step to capture jobs
    4. Calls the PRODUCTION scrape_job_detail_batch workflow
    5. Returns the captured job data for assertion

    Args:
        test_case: CompanyTestCase with fixture and URL info
        workflow_test: WorkflowTest instance from conftest fixture

    Returns:
        CompanyWorkflowTestResult with extracted jobs and metadata
    """
    from job_scrape_application.workflows.workflow.test_utils import SpiderFixture

    # Load fixture
    fixture = SpiderFixture.from_file(test_case.fixture_path)
    workflow_test.with_spider_fixture(fixture)

    # Pre-process fixture to extract jobs
    processed_jobs = _process_fixture_to_jobs(fixture.raw, test_case.url)

    # Container for captured jobs
    captured_jobs: list[dict[str, Any]] = []

    # Mock scrape_job_details to return fixture data
    async def mock_scrape_job_details(
        urls: list[str],
        source_url: str,
        pattern: str | None = None,
        posted_at_by_url: dict[str, int] | None = None,
        site_id: str | None = None,
    ) -> dict[str, Any]:
        workflow_test.captured.calls["scrape_job_details"].append({
            "urls": urls,
            "source_url": source_url,
        })
        return {
            "scrape": {
                "items": {
                    "normalized": processed_jobs,
                    "raw": [],
                },
                "siteId": site_id,
            }
        }

    # Mock ingest_jobs_from_scrape_step to capture jobs
    def mock_ingest_jobs(jobs: list[dict[str, Any]], site_id: str | None = None) -> None:
        workflow_test.captured.calls["ingest_jobs_from_scrape_step"].append({
            "jobs": jobs,
            "site_id": site_id,
        })
        # Store jobs in captured data
        workflow_test.captured.stored_scrapes.append({
            "items": {"normalized": jobs},
            "siteId": site_id,
        })
        captured_jobs.extend(jobs)

    # Apply mocks to workflow module.
    # Import via the workflow function's module to avoid package attribute shadowing.
    import importlib
    from job_scrape_application.workflows.workflow import (
        scrape_job_detail_batch as scrape_job_detail_batch_workflow,
    )

    wf_module = importlib.import_module(scrape_job_detail_batch_workflow.__module__)
    workflow_test.monkeypatch.setattr(
        wf_module, "scrape_job_details", mock_scrape_job_details
    )
    workflow_test.monkeypatch.setattr(
        wf_module, "ingest_jobs_from_scrape_step", mock_ingest_jobs
    )

    # Import production workflow (uses the patched module)
    scrape_job_detail_batch = wf_module.scrape_job_detail_batch

    # Build batch input
    batch = {
        "urls": [
            {
                "url": test_case.url,
                "sourceUrl": test_case.source_url,
            }
        ],
        "siteId": test_case.site_id,
    }

    # Run production workflow
    await workflow_test.run(scrape_job_detail_batch, batch=batch)

    return CompanyWorkflowTestResult(
        jobs=captured_jobs,
        scrapes=workflow_test.captured.stored_scrapes,
        step_calls=workflow_test.captured.calls,
    )


def assert_job_matches_expected(
    job: dict[str, Any],
    expected: dict[str, Any],
    test_case: CompanyTestCase,
) -> None:
    """Assert that extracted job matches expected values.

    Handles different assertion types:
    - Exact match: field == expected
    - Contains match: field_contains in actual
    - Not null: field_not_null means field is not None
    - Min value: field_min_* means >= threshold

    Args:
        job: Extracted job dict
        expected: Expected values from ground truth
        test_case: Test case for error context

    Raises:
        AssertionError: If any assertion fails
    """
    for key, value in expected.items():
        # Handle different assertion types
        if key.endswith("_not_contains"):
            field_name = key.replace("_not_contains", "")
            actual = job.get(field_name, "")
            assert value.lower() not in str(actual).lower(), (
                f"{test_case.identifier}: {field_name} should not contain {value!r}, "
                f"got {actual!r}"
            )

        elif key.endswith("_contains"):
            field_name = key.replace("_contains", "")
            actual = job.get(field_name, "")
            assert value.lower() in str(actual).lower(), (
                f"{test_case.identifier}: {field_name} should contain {value!r}, "
                f"got {actual!r}"
            )

        elif key.endswith("_not_null"):
            field_name = key.replace("_not_null", "")
            actual = job.get(field_name)
            assert actual is not None, (
                f"{test_case.identifier}: {field_name} should not be null"
            )

        elif key.startswith("description_min_words"):
            description = job.get("description", "")
            word_count = len(description.split()) if description else 0
            assert word_count >= value, (
                f"{test_case.identifier}: description should have >= {value} words, "
                f"got {word_count}"
            )

        elif key == "full_description_word_count_min":
            if value <= 0:
                continue
            description = job.get("description", "")
            word_count = len(description.split()) if description else 0
            assert word_count >= value, (
                f"{test_case.identifier}: full description should have >= {value} words, "
                f"got {word_count}"
            )

        elif key == "truncated_description_word_count_max":
            if value <= 0:
                continue
            description = job.get("description", "")
            word_count = len(description.split()) if description else 0
            assert word_count <= value, (
                f"{test_case.identifier}: description should have <= {value} words, "
                f"got {word_count}"
            )

        elif key.startswith("cost_milli_cents_min"):
            actual = job.get("cost_milli_cents", 0) or 0
            assert actual >= value, (
                f"{test_case.identifier}: cost_milli_cents should be >= {value}, "
                f"got {actual}"
            )

        elif key == "is_remote":
            actual = job.get("remote", False)
            assert actual == value, (
                f"{test_case.identifier}: remote should be {value}, got {actual}"
            )

        elif key == "level":
            actual = job.get("level", "mid")
            assert actual == value, (
                f"{test_case.identifier}: level should be {value!r}, got {actual!r}"
            )

        else:
            # Exact match
            actual = job.get(key)
            assert actual == value, (
                f"{test_case.identifier}: {key} should be {value!r}, got {actual!r}"
            )
