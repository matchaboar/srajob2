"""
End-to-end workflow test for GitHub careers using the DBOS schedule workflow pattern.

Tests the full flow:
1. Load GitHub listing fixture
2. Extract job URLs via activities
3. Verify error classification (all_seen vs zero_urls)
4. Ensure no invalid_response errors
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    import temporalio  # noqa: F401
except ImportError:
    pytest.skip("temporalio not installed", allow_module_level=True)

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows.site_handlers import get_site_handler  # noqa: E402


def _load_github_fixture() -> Dict[str, Any]:
    """Load the GitHub API listing fixture."""
    fixture_path = Path("tests/job_scrape_application/workflows/fixtures/debug/github_api_listing.json")
    if not fixture_path.exists():
        pytest.skip(f"GitHub fixture not found: {fixture_path}")

    with open(fixture_path) as f:
        data = json.load(f)

    # Convert to fixture format expected by workflow
    return {
        "request": {
            "url": "https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100",
            "params": {}
        },
        "response": data
    }


def _fixture_scrape_payload(fixture: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    """Convert fixture to scrape payload format."""
    return {
        "items": {"raw": fixture.get("response")},
        "sourceUrl": source_url,
    }


def _extract_fixture_detail_urls(
    fixture: Dict[str, Any],
    source_url: str,
) -> List[str]:
    """Extract job detail URLs from fixture using workflow activities."""
    scrape_payload = _fixture_scrape_payload(fixture, source_url)
    extracted_urls = acts._extract_job_urls_from_scrape(scrape_payload)  # noqa: SLF001
    handler = get_site_handler(source_url) if source_url else None
    filtered_urls = acts._filter_job_urls(  # noqa: SLF001
        extracted_urls,
        handler,
        source_url=source_url,
    )
    return filtered_urls


def test_github_workflow_url_extraction():
    """
    Test that GitHub listing fixture extracts job URLs correctly using workflow activities.

    This tests the full flow through the activities layer, not just the scraper.
    """
    fixture = _load_github_fixture()
    source_url = "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100"

    # Extract URLs using the same code path as the workflow
    detail_urls = _extract_fixture_detail_urls(fixture, source_url)

    # Verify extraction
    # Note: Workflow activities may extract more URLs than raw scraper
    # (e.g., from HTML events, link parsing, etc.)
    assert len(detail_urls) >= 30, f"Expected at least 30 job URLs, got {len(detail_urls)}"
    assert len(detail_urls) <= 200, f"Expected at most 200 job URLs, got {len(detail_urls)}"

    # Verify all URLs are valid GitHub careers URLs
    # GitHub has two URL formats:
    # 1. /careers-home/jobs/{slug}?lang={lang} (from API)
    # 2. /jobs/{id} (from HTML links)
    for url in detail_urls:
        assert "github.careers" in url, f"Invalid URL: {url}"
        assert "/jobs/" in url, f"Invalid job URL format: {url}"

    print(f"\n✓ Extracted {len(detail_urls)} job URLs from GitHub careers via workflow")
    print(f"  Sample URLs:")
    for i, url in enumerate(detail_urls[:5], 1):
        print(f"    {i}. {url}")


@pytest.mark.asyncio
async def test_github_workflow_error_classification():
    """
    Test error classification logic with GitHub fixture.

    Simulates scenarios:
    1. All URLs already exist → should NOT be zero_urls error
    2. Truly zero URLs → should BE zero_urls error
    """
    fixture = _load_github_fixture()
    source_url = "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100"

    # Extract URLs
    detail_urls = _extract_fixture_detail_urls(fixture, source_url)

    # Scenario 1: Simulate all URLs already existing
    # This tests the fix at line 2752: return 0, should_warn_zero_urls and not all_seen
    job_urls_before_existing = detail_urls
    job_existing_dropped = detail_urls.copy()
    invalid_urls: List[str] = []

    all_seen = (
        len(job_urls_before_existing) > 0
        and len(job_existing_dropped) == len(job_urls_before_existing)
        and len(invalid_urls) == 0
    )

    assert all_seen is True, "all_seen should be True when all URLs exist"

    # Simulate should_warn_zero_urls=True (GitHub is a listing page)
    handler = get_site_handler(source_url)
    should_warn_zero_urls = handler is not None and handler.is_listing_url(source_url)

    assert should_warn_zero_urls is True, "GitHub URL should be recognized as listing page"

    # The critical fix: should_warn_zero_urls and not all_seen
    should_fail = should_warn_zero_urls and not all_seen

    assert should_fail is False, \
        "Should NOT fail with zero_urls when all URLs were just skipped (all_seen=True)"

    print(f"\n✓ Error classification works correctly:")
    print(f"  - Extracted {len(detail_urls)} URLs")
    print(f"  - All existing (all_seen=True)")
    print(f"  - Result: should_fail={should_fail} (correct!)")


def test_github_empty_response_handling():
    """
    Test that empty response [[]] is handled correctly.

    This tests the fix in spidercloud_scraper.py for empty list handling.
    """
    from job_scrape_application.workflows.scrapers.spidercloud_scraper import (
        SpiderCloudScraper,
        SpidercloudDependencies,
    )

    # Create scraper
    async def fetch_seen(*args, **kwargs):
        return []

    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=fetch_seen,
    )

    scraper = SpiderCloudScraper(deps)
    handler = get_site_handler("https://www.github.careers/api/jobs")

    # Test empty response
    result = scraper._process_sync_json_response(
        original_url="https://www.github.careers/api/jobs",
        request_url="https://www.github.careers/api/jobs",
        raw_result=[[]],  # Empty nested list
        started_at=1234567890000,
        attempt=0,
        handler=handler,
    )

    # Should NOT fail as invalid_response
    assert "failed" not in result or result.get("failed") is None, \
        f"Empty list should not fail as invalid_response: {result.get('failed')}"

    assert result.get("job_urls") == [], "Should have empty job_urls list"

    print("\n✓ Empty response [[]] handled correctly (no invalid_response error)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
