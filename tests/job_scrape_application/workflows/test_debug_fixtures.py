"""
Tests for debugging specific user-submitted job extraction issues.

This test file is separate from the main test suite to allow testing individual
jobs without affecting the automated test runs. Fixtures and assertions for debug
jobs are stored in the debug/ subfolder.
"""

from __future__ import annotations

import orjson
import logging
from pathlib import Path

import pytest

# Import helper functions from the main test file
import tests.job_scrape_application.workflows.test_job_detail_extraction_e2e as main_test

WorkflowTestModule = main_test.WorkflowTestModule
_load_fixture = main_test._load_fixture
_load_assertions = main_test._load_assertions
_validate_job_against_assertions = main_test._validate_job_against_assertions
_format_assertion_failures = main_test._format_assertion_failures
_write_extraction_result = main_test._write_extraction_result

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/debug")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth/debug")

logger = logging.getLogger(__name__)


def _get_debug_fixtures() -> list[tuple[str, Path, Path]]:
    """Get all debug fixture files and their ground truth files.

    Supports both flat and per-company folder organization:
    - Flat: fixtures/debug/{id}_detail.json + ground_truth/debug/{id}.yml
    - Nested: fixtures/debug/{company}/{id}_detail.json + ground_truth/debug/{company}/{id}.yml
    """
    if not FIXTURE_DIR.exists():
        return []

    fixtures = []

    # Search recursively for all *_detail.json files
    for fixture_file in FIXTURE_DIR.rglob("*_detail.json"):
        # Extract identifier from filename (e.g., "greenhouse_abc12345_20250113" from "greenhouse_abc12345_20250113_detail.json")
        identifier = fixture_file.stem.replace("_detail", "")

        # Find matching ground truth file in the same relative folder structure
        # e.g., fixtures/debug/airbnb/foo.json -> ground_truth/debug/airbnb/foo.yml
        relative_path = fixture_file.relative_to(FIXTURE_DIR)
        assertion_file = GROUND_TRUTH_DIR / relative_path.parent / f"{identifier}.yml"

        # Also check flat structure (for backwards compatibility)
        if not assertion_file.exists():
            assertion_file = GROUND_TRUTH_DIR / f"{identifier}.yml"

        if assertion_file.exists():
            fixtures.append((identifier, fixture_file, assertion_file))

    return fixtures


DEBUG_FIXTURES = _get_debug_fixtures()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identifier,fixture_path,assertion_path",
    DEBUG_FIXTURES,
    ids=lambda x: x[0] if isinstance(x, tuple) else str(x),
)
async def test_debug_job_extraction(
    identifier: str,
    fixture_path: Path,
    assertion_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reset_dbos: None,
) -> None:
    """
    Test extraction for a specific debug job.

    This test validates that the job extraction works correctly for
    user-submitted jobs that had issues in production.
    """
    # Load fixture - it should be in the same format as other detail fixtures
    fixture_data = orjson.loads(fixture_path.read_text(encoding="utf-8"))

    # For debug fixtures, we need to construct a mock schedule entry
    # Extract site name from identifier (e.g., "netflix" from "netflix_790313551266")
    site_name = identifier.split("_")[0]

    # Get the detail URL from the fixture
    if isinstance(fixture_data, list) and len(fixture_data) > 0:
        if isinstance(fixture_data[0], list) and len(fixture_data[0]) > 0:
            detail_url = fixture_data[0][0].get("url", "")
        else:
            detail_url = fixture_data[0].get("url", "")
    else:
        detail_url = fixture_data.get("request", {}).get("url", "")

    # Create a mock schedule entry
    mock_entry = {
        "name": site_name,
        "url": detail_url,
        "type": site_name,
        "enabled": True,
    }

    # Convert debug fixture to standard format if needed
    if isinstance(fixture_data, list):
        # Already in the format [[{...}]] - need to wrap it properly
        # Debug fixtures from dump_spidercloud_response.py come as [[response]]
        # We need to convert to {request: {...}, response: {...}}
        # For sync mode (stream: False), response is a dict, not a list of JSON strings
        detail_fixture = {
            "request": {
                "url": detail_url,
                "params": {},
                "stream": False,  # Use sync mode (streaming is deprecated)
            },
            "response": fixture_data[0][0],  # Direct dict for sync mode
        }
    else:
        # Already in standard format - uses streaming mode if stream is not explicitly False
        # NOTE: Streaming mode is DEPRECATED. Fixtures without stream: False will trigger
        # a DeprecationWarning. New fixtures should always set stream: False in the request.
        detail_fixture = fixture_data

    # Run the extraction
    module = WorkflowTestModule(mock_entry, detail_fixture, tmp_path, monkeypatch)
    await module.setup()
    result = await module.run_detail_extraction()

    # Write extraction result for inspection
    _write_extraction_result(result)

    # Validate against assertions
    if not result.extracted_jobs:
        pytest.fail(f"No jobs extracted for {identifier}")

    # Load assertions from the debug assertion file
    try:
        import yaml
        assertions = yaml.safe_load(assertion_path.read_text(encoding="utf-8"))
    except Exception as exc:
        pytest.fail(f"Failed to load assertions from {assertion_path}: {exc}")

    job = result.extracted_jobs[0]

    validation_results = _validate_job_against_assertions(job, assertions)
    failures = [r for r in validation_results if not r.passed]

    if failures:
        failure_msg = _format_assertion_failures(validation_results)
        pytest.fail(f"Assertion validation failed for {identifier}:\n{failure_msg}")
