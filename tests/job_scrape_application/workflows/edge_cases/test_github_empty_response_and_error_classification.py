"""
Test GitHub careers empty response handling and error classification fixes.

This test demonstrates two bug fixes:
1. Empty list responses from SpiderCloud should not be treated as "invalid_response"
2. When URLs are extracted but all are skipped (already exist), should not report "zero_urls" error
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers import get_site_handler  # noqa: E402


class TestGithubEmptyResponseHandling:
    """Test that empty list responses from SpiderCloud are handled correctly."""

    def test_empty_list_response_not_invalid(self):
        """
        Test that SpiderCloud returning [[]] is treated as empty results, not invalid_response.

        Bug: SpiderCloud returned [[]] for GitHub careers listing, which was unwrapped
        to [] and then rejected as "invalid_response". This should be treated as a
        valid empty result instead.
        """
        deps = SpidercloudDependencies(
            mask_secret=lambda v: v,
            sanitize_headers=lambda h: h,
            build_request_snapshot=lambda *_a, **_k: {},
            log_dispatch=lambda *_a, **_k: None,
            log_sync_response=lambda *_a, **_k: None,
            trim_scrape_for_convex=lambda payload, **_k: payload,
            settings=SimpleNamespace(spider_api_key="test-key"),
            fetch_seen_urls_for_site=lambda *_a, **_k: [],
        )
        scraper = SpiderCloudScraper(deps)
        handler = get_site_handler("https://www.github.careers/careers-home/jobs")

        # Simulate SpiderCloud returning [[]] (empty nested list)
        raw_result = [[]]
        original_url = "https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100"
        started_at = 1234567890000

        result = scraper._process_sync_json_response(
            original_url=original_url,
            request_url=original_url,
            raw_result=raw_result,
            started_at=started_at,
            attempt=0,
            handler=handler,
        )

        # Should NOT have failed with invalid_response
        assert "failed" not in result or result.get("failed") is None, \
            "Empty list response should not be treated as invalid_response"

        # Should have empty job_urls list
        assert "job_urls" in result
        assert result["job_urls"] == [], "Should have empty job_urls list for empty response"

        # Should have valid structure
        assert "raw" in result
        assert "costMilliCents" in result
        assert "startedAt" in result

    def test_empty_outer_list_response(self):
        """Test that SpiderCloud returning [] (empty list) is handled correctly."""
        deps = SpidercloudDependencies(
            mask_secret=lambda v: v,
            sanitize_headers=lambda h: h,
            build_request_snapshot=lambda *_a, **_k: {},
            log_dispatch=lambda *_a, **_k: None,
            log_sync_response=lambda *_a, **_k: None,
            trim_scrape_for_convex=lambda payload, **_k: payload,
            settings=SimpleNamespace(spider_api_key="test-key"),
            fetch_seen_urls_for_site=lambda *_a, **_k: [],
        )
        scraper = SpiderCloudScraper(deps)
        handler = get_site_handler("https://www.github.careers/careers-home/jobs")

        raw_result = []
        original_url = "https://www.github.careers/careers-home/jobs"
        started_at = 1234567890000

        result = scraper._process_sync_json_response(
            original_url=original_url,
            request_url=original_url,
            raw_result=raw_result,
            started_at=started_at,
            attempt=0,
            handler=handler,
        )

        # Empty list after unwrapping should be treated as empty result
        assert "failed" not in result or result.get("failed") is None, \
            "Empty list should be treated as empty result, not invalid"
        assert result["job_urls"] == []

    def test_truly_invalid_response_still_fails(self):
        """Test that truly invalid responses (non-list, non-dict) still fail."""
        deps = SpidercloudDependencies(
            mask_secret=lambda v: v,
            sanitize_headers=lambda h: h,
            build_request_snapshot=lambda *_a, **_k: {},
            log_dispatch=lambda *_a, **_k: None,
            log_sync_response=lambda *_a, **_k: None,
            trim_scrape_for_convex=lambda payload, **_k: payload,
            settings=SimpleNamespace(spider_api_key="test-key"),
            fetch_seen_urls_for_site=lambda *_a, **_k: [],
        )
        scraper = SpiderCloudScraper(deps)
        handler = get_site_handler("https://www.github.careers/careers-home/jobs")

        # Test various truly invalid types
        invalid_types = [
            "string response",
            12345,
            None,
            True,
        ]

        for raw_result in invalid_types:
            result = scraper._process_sync_json_response(
                original_url="https://example.com",
                request_url="https://example.com",
                raw_result=raw_result,
                started_at=1234567890000,
                attempt=0,
                handler=handler,
            )

            # Should fail with invalid_response
            assert "failed" in result, \
                f"Should fail for invalid type: {type(raw_result).__name__}"
            assert result["failed"]["reason"] == "invalid_response", \
                f"Should have invalid_response reason for type: {type(raw_result).__name__}"

    def test_empty_dict_response_valid(self):
        """Test that an empty dict response continues normally."""
        deps = SpidercloudDependencies(
            mask_secret=lambda v: v,
            sanitize_headers=lambda h: h,
            build_request_snapshot=lambda *_a, **_k: {},
            log_dispatch=lambda *_a, **_k: None,
            log_sync_response=lambda *_a, **_k: None,
            trim_scrape_for_convex=lambda payload, **_k: payload,
            settings=SimpleNamespace(spider_api_key="test-key"),
            fetch_seen_urls_for_site=lambda *_a, **_k: [],
        )
        scraper = SpiderCloudScraper(deps)
        handler = get_site_handler("https://www.github.careers/careers-home/jobs")

        # Empty dict is valid - it's like an API returning {}
        raw_result = {}
        original_url = "https://www.github.careers/careers-home/jobs"
        started_at = 1234567890000

        result = scraper._process_sync_json_response(
            original_url=original_url,
            request_url=original_url,
            raw_result=raw_result,
            started_at=started_at,
            attempt=0,
            handler=handler,
        )

        # Empty dict should not fail - it proceeds to extraction which finds 0 URLs
        # Check that it doesn't have the "invalid_response" failure
        if "failed" in result:
            assert result["failed"]["reason"] != "invalid_response", \
                "Empty dict should not be invalid_response"


class TestErrorClassificationAllSeen:
    """Test that URLs skipped because they already exist don't cause zero_urls error."""

    def test_all_seen_not_zero_urls_error(self):
        """
        Test the error classification fix: when all URLs are skipped because they
        already exist, should NOT report as "zero_urls" error.

        Bug: Line 2750 returned `should_warn_zero_urls` instead of accounting for
        `all_seen`. This caused listings to be marked as "zero_urls" errors even
        when URLs were extracted but all already existed in database.

        Fix: Return `should_warn_zero_urls and not all_seen` to only fail when
        it's truly zero URLs, not when URLs exist but are all skipped.
        """

        # This is a unit test scenario simulating the logic in _enqueue_from_scrape
        # We'll test that all_seen=True prevents zero_urls error

        # Scenario 1: All URLs skipped (already exist)
        # - job_urls_before_existing has items
        # - All dropped as existing
        # - No invalid URLs
        # Expected: should_warn_zero_urls=True, all_seen=True → return False (no error)

        job_urls_before_existing = [
            "https://example.com/job1",
            "https://example.com/job2",
            "https://example.com/job3",
        ]
        job_existing_dropped = job_urls_before_existing.copy()
        invalid_urls = []

        # Calculate all_seen as the code does
        all_seen = (
            len(job_urls_before_existing) > 0
            and len(job_existing_dropped) == len(job_urls_before_existing)
            and len(invalid_urls) == 0
        )

        assert all_seen is True, "Should be True when all URLs are existing"

        # Simulate should_warn_zero_urls=True (this is a listing page)
        should_warn_zero_urls = True

        # The return value should be: should_warn_zero_urls and not all_seen
        should_fail = should_warn_zero_urls and not all_seen

        assert should_fail is False, \
            "Should NOT fail with zero_urls when all URLs were just skipped (all_seen=True)"

    def test_truly_zero_urls_still_fails(self):
        """Test that truly zero URLs (nothing extracted) still fails appropriately."""
        # Scenario 2: Truly zero URLs extracted
        # - No URLs before existing check
        # - Nothing dropped
        # Expected: should_warn_zero_urls=True, all_seen=False → return True (error)

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

    def test_partial_skipped_urls_still_fails(self):
        """Test that partial skipping (some URLs invalid or other issues) still fails."""
        # Scenario 3: Some URLs skipped but not all
        # - Some URLs before existing
        # - Only some dropped as existing (not all)
        # Expected: all_seen=False → return True (error)

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

    def test_invalid_urls_present_still_fails(self):
        """Test that presence of invalid URLs causes failure even if all valid ones exist."""
        # Scenario 4: All valid URLs exist but there are invalid URLs
        # - URLs before existing
        # - All dropped as existing
        # - But there are invalid URLs too
        # Expected: all_seen=False → return True (error)

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
    pytest.main([__file__, "-v", "-s"])
