"""
Test to demonstrate the invalid_url marking issue for greenhouse URLs.

This test shows that valid job-boards.greenhouse.io URLs are incorrectly
marked as "invalid_url" in skip reasons when they are converted to
boards-api.greenhouse.io URLs that already exist in the database.
"""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers import get_site_handler  # noqa: E402
from job_scrape_application.workflows.activities import _filter_job_urls  # noqa: E402


def test_greenhouse_job_boards_url_conversion():
    """
    Test that job-boards.greenhouse.io URLs are properly converted to API URLs.

    This demonstrates the URL conversion that happens in production:
    - job-boards.greenhouse.io URLs are extracted from listing pages
    - They're converted to boards-api.greenhouse.io API URLs
    - The original URLs are marked as "invalid_url" because they don't match the converted URLs
    """
    # Test URLs from the log
    test_urls = [
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4828142007",
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4926299007",
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4969639007",
    ]

    source_url = "https://api.greenhouse.io/v1/boards/thetradedesk/jobs"
    handler = get_site_handler(source_url)

    assert handler is not None, "Should have a greenhouse handler"
    assert handler.name == "greenhouse", f"Expected greenhouse handler, got {handler.name}"

    # Test URL conversion
    for url in test_urls:
        # Check that the handler matches the URL
        assert handler.matches_url(url), f"Handler should match URL: {url}"

        # Check slug extraction
        slug = handler._extract_slug_from_url(url)
        assert slug == "thetradedesk", f"Should extract slug 'thetradedesk', got: {slug}"

        # Check job ID extraction
        job_id = handler._extract_job_id_from_url(url)
        assert job_id is not None, f"Should extract job ID from: {url}"

        # Check API URI conversion
        api_url = handler.get_api_uri(url, source_url=source_url)
        assert api_url is not None, f"Should convert to API URL: {url}"
        assert api_url.startswith("https://boards-api.greenhouse.io/v1/boards/"), \
            f"API URL should use boards-api domain: {api_url}"
        assert f"/thetradedesk/jobs/{job_id}" in api_url, \
            f"API URL should contain slug and job ID: {api_url}"

    # Test filtering behavior
    filtered = _filter_job_urls(
        test_urls,
        handler,
        source_url=source_url
    )

    print(f"\nOriginal URLs: {test_urls}")
    print(f"Filtered URLs: {filtered}")

    # The filtered URLs should be the converted API URLs, not the original URLs
    assert len(filtered) == len(test_urls), "All URLs should be filtered to API URLs"
    for url in filtered:
        assert url.startswith("https://boards-api.greenhouse.io/v1/boards/"), \
            f"Filtered URL should be API URL: {url}"

    # The problem: original URLs are not in the filtered list
    # This causes them to be marked as "invalid_url" in the skip reasons
    for original_url in test_urls:
        assert original_url not in filtered, \
            f"Original URL should not be in filtered list (it's converted): {original_url}"


def test_invalid_url_marking_in_skip_reasons():
    """
    Test that demonstrates how valid URLs are marked as "invalid_url" in skip reasons.

    The issue:
    1. job-boards.greenhouse.io URLs are extracted
    2. They're converted to boards-api.greenhouse.io URLs
    3. If the converted URLs already exist, they're filtered out
    4. The original job-boards URLs are marked as "invalid_url" even though they're valid

    This is misleading because the URLs are actually valid - they just convert to
    URLs that already exist in the database.
    """
    test_urls = [
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4828142007",
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4926299007",
    ]

    source_url = "https://api.greenhouse.io/v1/boards/thetradedesk/jobs"
    handler = get_site_handler(source_url)

    # Simulate the filtering that happens in activities
    filtered = _filter_job_urls(test_urls, handler, source_url=source_url)

    # These are the URLs that would be marked as "invalid_url"
    # because they're in extracted_urls but not in the filtered list
    invalid_urls = [url for url in test_urls if url not in filtered]

    print(f"\nExtracted URLs: {test_urls}")
    print(f"Filtered URLs: {filtered}")
    print(f"URLs marked as 'invalid_url': {invalid_urls}")

    # The problem: ALL original URLs are marked as "invalid"
    # even though they're valid greenhouse URLs
    assert len(invalid_urls) == len(test_urls), \
        "All original URLs are marked as invalid because they're converted"

    # But they're actually valid greenhouse URLs!
    for url in invalid_urls:
        assert handler.matches_url(url), f"URL is valid for greenhouse handler: {url}"
        assert handler._extract_job_id_from_url(url) is not None, \
            f"URL has valid job ID: {url}"
        assert handler.get_api_uri(url) is not None, \
            f"URL can be converted to API URL: {url}"


def test_classify_filtered_urls_greenhouse():
    """
    Test that _classify_filtered_urls correctly identifies converted URLs.
    """
    from job_scrape_application.workflows.activities import _classify_filtered_urls

    test_urls = [
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4828142007",
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4926299007",
    ]

    converted_api_urls = [
        "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/4828142007",
        "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/4926299007",
    ]

    source_url = "https://api.greenhouse.io/v1/boards/thetradedesk/jobs"
    handler = get_site_handler(source_url)

    # Classify the URLs
    converted_urls, invalid_urls = _classify_filtered_urls(
        test_urls, converted_api_urls, handler, source_url
    )

    print(f"\nExtracted URLs: {test_urls}")
    print(f"Filtered URLs: {converted_api_urls}")
    print(f"Converted URLs: {converted_urls}")
    print(f"Invalid URLs: {invalid_urls}")

    # All original URLs should be classified as converted, not invalid
    assert len(converted_urls) == len(test_urls), \
        "All URLs should be classified as converted"
    assert len(invalid_urls) == 0, \
        "No URLs should be classified as invalid"

    for url in test_urls:
        assert url in converted_urls, \
            f"URL should be classified as converted: {url}"


def test_classify_filtered_urls_truly_invalid():
    """
    Test that _classify_filtered_urls correctly identifies truly invalid URLs.
    """
    from job_scrape_application.workflows.activities import _classify_filtered_urls

    # Mix of valid and invalid URLs
    extracted_urls = [
        "https://job-boards.greenhouse.io/thetradedesk/jobs/4828142007",  # Will be converted
        "https://example.com/privacy-policy",  # Invalid
        "https://facebook.com/share",  # Invalid
    ]

    # Only the converted URL makes it through
    filtered_urls = [
        "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/4828142007",
    ]

    source_url = "https://api.greenhouse.io/v1/boards/thetradedesk/jobs"
    handler = get_site_handler(source_url)

    converted_urls, invalid_urls = _classify_filtered_urls(
        extracted_urls, filtered_urls, handler, source_url
    )

    print(f"\nExtracted URLs: {extracted_urls}")
    print(f"Filtered URLs: {filtered_urls}")
    print(f"Converted URLs: {converted_urls}")
    print(f"Invalid URLs: {invalid_urls}")

    # The greenhouse URL should be converted
    assert len(converted_urls) == 1
    assert "https://job-boards.greenhouse.io/thetradedesk/jobs/4828142007" in converted_urls

    # The other URLs should be invalid
    assert len(invalid_urls) == 2
    assert "https://example.com/privacy-policy" in invalid_urls
    assert "https://facebook.com/share" in invalid_urls


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
