"""
End-to-end tests for listing URL extraction workflow.

Tests that given a listing page fixture, the workflow will:
1. Extract job URLs from the page content
2. Filter URLs using the appropriate site handler
3. Normalize URLs for enqueuing

This module uses the unified ListingWorkflowModule which calls production code,
eliminating the need for reimplemented extraction logic in tests.

Results can be output to ./site-detail-e2e-examples for inspection.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.core.listing_workflow import (
    ListingExtractionTrace,
    ListingWorkflowModule,
)
from job_scrape_application.workflows.core import SpiderFixture

SCHEDULE_PATH = Path("job_scrape_application/config/prod/site_schedules.yml")
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")
SINGLE_REQUEST_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/single_request")
DEBUG_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/debug")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth")
OUTPUT_DIR = Path("./site-detail-e2e-examples")


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


def _load_schedule_entries() -> List[Dict[str, Any]]:
    payload = yaml.safe_load(SCHEDULE_PATH.read_text(encoding="utf-8")) or {}
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("enabled", True)]


def _schedule_id(entry: Dict[str, Any]) -> str:
    return _slugify(str(entry.get("name") or entry.get("url") or "site"))


def _listing_fixture_path(entry: Dict[str, Any]) -> Path:
    """Get listing fixture path for a schedule entry."""
    slug = _schedule_id(entry)
    # Prefer single request fixtures if they exist
    single_request = SINGLE_REQUEST_FIXTURE_DIR / f"{slug}_listing.json"
    if single_request.exists():
        return single_request
    # Fallback to JSONL streaming fixtures
    return FIXTURE_DIR / f"{slug}_listing.json"


def _load_fixture(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"Fixture {path} must contain a dict payload")
    return payload


def _build_scrape_payload(fixture: Dict[str, Any]) -> Dict[str, Any]:
    """Build a scrape payload from fixture data.

    Converts fixture format to the format expected by ListingWorkflowModule.
    """
    response = fixture.get("response", [])
    request = fixture.get("request", {})
    url = request.get("url", "")

    # Parse response if it's a list of JSONL strings
    if isinstance(response, list) and response:
        parsed_items = []
        for item in response:
            if isinstance(item, str):
                try:
                    parsed = json.loads(item)
                    parsed_items.append(parsed)
                except json.JSONDecodeError:
                    continue
            elif isinstance(item, dict):
                parsed_items.append(item)
        response = parsed_items

    return {
        "sourceUrl": url,
        "provider": "spidercloud",
        "items": {
            "provider": "spidercloud",
            "raw": [response] if response else [],
        },
    }


def _get_test_entries() -> List[tuple[str, Path, Dict[str, Any]]]:
    """Get all test entries with available listing fixtures."""
    entries = _load_schedule_entries()
    test_entries = []
    for entry in entries:
        fixture_path = _listing_fixture_path(entry)
        if fixture_path.exists():
            test_entries.append((_schedule_id(entry), fixture_path, entry))
    return test_entries


# Parametrize tests with all available fixtures
_TEST_ENTRIES = _get_test_entries()
_TEST_IDS = [entry[0] for entry in _TEST_ENTRIES]


@pytest.mark.parametrize(
    "site_id,fixture_path,schedule_entry",
    _TEST_ENTRIES,
    ids=_TEST_IDS,
)
def test_listing_extraction(
    site_id: str,
    fixture_path: Path,
    schedule_entry: Dict[str, Any],
) -> None:
    """Test that listing extraction produces URLs.

    Uses the unified ListingWorkflowModule which calls production code.
    This ensures tests and production use the same extraction logic.
    """
    fixture_data = _load_fixture(fixture_path)
    scrape_payload = _build_scrape_payload(fixture_data)
    source_url = fixture_data.get("request", {}).get("url", "")

    # Use the unified workflow module with debug tracing
    workflow = ListingWorkflowModule(debug=True, write_output=False)
    url_entries = workflow.extract_listing_urls(
        scrape_payload,
        source_url,
        site_id,
    )

    # Get the trace for debugging
    trace = workflow.get_trace()
    assert trace is not None, "Expected trace to be captured"

    # Write trace to output dir if verbose mode
    if os.environ.get("DEBUG_EXTRACTION_VERBOSE"):
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        json_path = OUTPUT_DIR / f"{site_id}_listing_extraction.json"
        md_path = OUTPUT_DIR / f"{site_id}_listing_extraction.md"
        json_path.write_text(json.dumps(trace.to_json_detailed(), indent=2))
        md_path.write_text(trace.to_markdown_concise())

    # Assertions - at minimum, extraction should not error
    # Sites with jobs should produce URLs
    extracted_count = len(trace.extracted_urls)
    normalized_count = len(trace.normalized_urls)

    # Log info for debugging
    print(f"\n{site_id}: extracted={extracted_count}, normalized={normalized_count}")

    # Basic sanity check - if we extracted URLs, we should have normalized some
    if extracted_count > 0:
        assert normalized_count > 0, (
            f"Site {site_id} extracted {extracted_count} URLs but normalized 0. "
            f"Check handler filtering. Rejected: {trace.rejected_urls[:5]}"
        )


@pytest.mark.parametrize(
    "site_id,fixture_path,schedule_entry",
    _TEST_ENTRIES[:5],  # Just test first 5 for smoke test
    ids=_TEST_IDS[:5],
)
def test_listing_extraction_smoke(
    site_id: str,
    fixture_path: Path,
    schedule_entry: Dict[str, Any],
) -> None:
    """Smoke test that listing extraction runs without errors."""
    fixture_data = _load_fixture(fixture_path)
    scrape_payload = _build_scrape_payload(fixture_data)
    source_url = fixture_data.get("request", {}).get("url", "")

    # Use the unified workflow module
    workflow = ListingWorkflowModule(debug=False, write_output=False)
    url_entries = workflow.extract_listing_urls(
        scrape_payload,
        source_url,
        site_id,
    )

    # Should not error
    assert isinstance(url_entries, list)


def test_listing_workflow_trace_output() -> None:
    """Test that the workflow module produces correct trace format."""
    # Create a simple mock scrape payload
    scrape_payload = {
        "sourceUrl": "https://example.com/jobs",
        "provider": "spidercloud",
        "items": {
            "provider": "spidercloud",
            "raw": [[{
                "url": "https://example.com/jobs",
                "content": {
                    "commonmark": "[Job 1](https://example.com/job/1)\n[Job 2](https://example.com/job/2)",
                },
            }]],
        },
    }

    workflow = ListingWorkflowModule(debug=True, write_output=False)
    url_entries = workflow.extract_listing_urls(
        scrape_payload,
        "https://example.com/jobs",
        "test_site",
    )

    trace = workflow.get_trace()
    assert trace is not None

    # Check trace structure
    assert trace.site_id == "test_site"
    assert trace.source_url == "https://example.com/jobs"
    assert isinstance(trace.steps, list)
    assert len(trace.steps) > 0

    # Check JSON output
    json_output = trace.to_json_detailed()
    assert "site_id" in json_output
    assert "extracted_urls" in json_output
    assert "steps" in json_output

    # Check markdown output
    md_output = trace.to_markdown_concise()
    assert "# Listing Extraction:" in md_output
    assert "test_site" in md_output
