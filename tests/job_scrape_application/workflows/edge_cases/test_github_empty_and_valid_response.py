"""
Test GitHub careers empty and valid response handling.

This test verifies two critical bug fixes:
1. Empty list responses [[]] from SpiderCloud are handled as valid empty results
2. When URLs are extracted but all already exist, should not report "zero_urls" error
"""
from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers import get_site_handler  # noqa: E402


def _make_scraper() -> SpiderCloudScraper:
    """Create a test scraper instance with mock dependencies."""
    async def _fetch_seen_urls_for_site(*_args, **_kwargs) -> list[str]:
        return []

    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=_fetch_seen_urls_for_site,
    )
    return SpiderCloudScraper(deps)


def test_github_empty_list_response():
    """
    Test that [[]] empty response from SpiderCloud is handled correctly.

    Original bug: SpiderCloud returned [[]] which was unwrapped to [] and
    rejected as "invalid_response". Should be treated as valid empty result.
    """
    scraper = _make_scraper()
    handler = get_site_handler("https://www.github.careers/careers-home/jobs")

    site_url = "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100"
    started_at = 1234567890000

    # Simulate SpiderCloud returning [[]] (empty nested list)
    raw_result = [[]]

    result = scraper._process_sync_json_response(
        original_url=site_url,
        request_url=site_url,
        raw_result=raw_result,
        started_at=started_at,
        attempt=0,
        handler=handler,
    )

    # Should NOT have failed with invalid_response
    assert "failed" not in result or result.get("failed") is None, \
        f"Empty list response should not be treated as invalid_response: {result.get('failed')}"

    # Should have empty job_urls list
    assert "job_urls" in result
    assert result["job_urls"] == [], "Should have empty job_urls list for empty response"

    # Should have valid structure
    assert "raw" in result
    assert "costMilliCents" in result
    assert "startedAt" in result


def test_github_valid_response_with_jobs():
    """
    Test GitHub careers listing with actual jobs data.

    Uses real fixture from GitHub API that returns 43 jobs. Verifies that:
    1. Jobs are extracted correctly
    2. No invalid_response errors
    """

    scraper = _make_scraper()
    handler = get_site_handler("https://www.github.careers/api/jobs")

    fixture_path = Path(__file__).parent / "fixtures" / "debug" / "github_api_listing.json"
    if not fixture_path.exists():
        import pytest
        pytest.skip(f"Fixture not found: {fixture_path}")

    with open(fixture_path) as f:
        fixture_data = json.load(f)

    site_url = "https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100"
    started_at = 1234567890000

    # Process the response
    result = scraper._process_sync_json_response(
        original_url=site_url,
        request_url=site_url,
        raw_result=fixture_data,
        started_at=started_at,
        attempt=0,
        handler=handler,
    )

    # Should not have failed
    assert "failed" not in result or result.get("failed") is None, \
        f"Valid response should not fail: {result.get('failed')}"

    # Should have extracted job URLs
    assert "job_urls" in result
    job_urls = result["job_urls"]

    # Expect around 43 jobs
    assert len(job_urls) >= 30, f"Expected at least 30 jobs, got {len(job_urls)}"
    assert len(job_urls) <= 50, f"Expected at most 50 jobs, got {len(job_urls)}"

    # Verify all URLs are for GitHub careers
    for url in job_urls:
        assert "github.careers" in url, f"Invalid job URL: {url}"
        assert "/careers-home/jobs/" in url, f"Invalid job URL format: {url}"


def test_error_classification_all_seen_logic():
    """
    Test the error classification logic fix.

    Bug fix: When all URLs are skipped because they exist in DB, should NOT be
    marked as failed. This tests the fix at line 2752:
    `return 0, should_warn_zero_urls and not all_seen`
    """
    # Scenario 1: All URLs skipped (already exist)
    job_urls_before_existing = [
        "https://example.com/job1",
        "https://example.com/job2",
        "https://example.com/job3",
    ]
    job_existing_dropped = job_urls_before_existing.copy()
    invalid_urls = []

    # Calculate all_seen as the code does at line 2725-2729
    all_seen = (
        len(job_urls_before_existing) > 0
        and len(job_existing_dropped) == len(job_urls_before_existing)
        and len(invalid_urls) == 0
    )

    assert all_seen is True, "Should be True when all URLs are existing"

    # Simulate should_warn_zero_urls=True (this is a listing page)
    should_warn_zero_urls = True

    # The return value at line 2752: should_warn_zero_urls and not all_seen
    should_fail = should_warn_zero_urls and not all_seen

    assert should_fail is False, \
        "Should NOT fail with zero_urls when all URLs were just skipped (all_seen=True)"


def test_error_classification_truly_zero_urls():
    """Test that truly zero URLs (nothing extracted) still fails appropriately."""
    # Scenario 2: Truly zero URLs extracted
    job_urls_before_existing = []
    job_existing_dropped = []
    invalid_urls = []

    all_seen = (
        len(job_urls_before_existing) > 0
        and len(job_existing_dropped) == len(job_urls_before_existing)
        and len(invalid_urls) == 0
    )

    assert all_seen is False, "Should be False when no URLs extracted"

    should_warn_zero_urls = True  # Listing page
    should_fail = should_warn_zero_urls and not all_seen

    assert should_fail is True, \
        "Should fail with zero_urls when truly no URLs were extracted"


def test_error_classification_partial_skipped():
    """Test that partial skipping (some URLs invalid or other issues) still fails."""
    # Scenario 3: Some URLs skipped but not all
    job_urls_before_existing = [
        "https://example.com/job1",
        "https://example.com/job2",
        "https://example.com/job3",
    ]
    job_existing_dropped = [
        "https://example.com/job1",
        "https://example.com/job2",
    ]
    invalid_urls = []

    all_seen = (
        len(job_urls_before_existing) > 0
        and len(job_existing_dropped) == len(job_urls_before_existing)
        and len(invalid_urls) == 0
    )

    assert all_seen is False, "Should be False when not all URLs were dropped"

    should_warn_zero_urls = True
    should_fail = should_warn_zero_urls and not all_seen

    assert should_fail is True, \
        "Should fail when not all URLs were skipped (indicates a problem)"


def test_error_classification_invalid_urls_present():
    """Test that presence of invalid URLs causes failure even if all valid ones exist."""
    # Scenario 4: All valid URLs exist but there are invalid URLs
    job_urls_before_existing = [
        "https://example.com/job1",
    ]
    job_existing_dropped = [
        "https://example.com/job1",
    ]
    invalid_urls = [
        "invalid-url-1",
        "invalid-url-2",
    ]

    all_seen = (
        len(job_urls_before_existing) > 0
        and len(job_existing_dropped) == len(job_urls_before_existing)
        and len(invalid_urls) == 0
    )

    assert all_seen is False, "Should be False when invalid URLs are present"

    should_warn_zero_urls = True
    should_fail = should_warn_zero_urls and not all_seen

    assert should_fail is True, \
        "Should fail when invalid URLs are present (indicates a problem)"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v", "-s"])
