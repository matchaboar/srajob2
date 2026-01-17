"""Tests for scrape_listing_batch workflow helper functions.

Note: Full workflow integration tests require DBOS initialization.
These tests focus on the deterministic helper functions.
"""

from __future__ import annotations


class TestParseListingBatch:
    """Tests for _parse_listing_batch helper function."""

    def test_parses_valid_entries(self) -> None:
        """Test parsing of valid URL entries."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _parse_listing_batch,
        )

        parsed = _parse_listing_batch({
            "urls": [
                {
                    "url": "https://boards.greenhouse.io/company/jobs",
                    "sourceUrl": "https://boards.greenhouse.io/company",
                    "pattern": "greenhouse",
                }
            ]
        })

        assert len(parsed.entries) == 1
        assert parsed.source_url == "https://boards.greenhouse.io/company"
        assert len(parsed.groups) == 1

    def test_ignores_non_dict_entries(self) -> None:
        """Test that non-dict entries are ignored."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _parse_listing_batch,
        )

        parsed = _parse_listing_batch({
            "urls": ["invalid", None, 123, {"url": "https://example.com"}]
        })

        assert len(parsed.entries) == 1

    def test_ignores_entries_without_url(self) -> None:
        """Test that entries without URL are ignored."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _parse_listing_batch,
        )

        parsed = _parse_listing_batch({
            "urls": [{"sourceUrl": "https://example.com"}]
        })

        assert len(parsed.entries) == 0

    def test_groups_by_source_url_and_pattern(self) -> None:
        """Test that entries are grouped by source URL and pattern."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _parse_listing_batch,
        )

        parsed = _parse_listing_batch({
            "urls": [
                {"url": "https://ex.com/1", "sourceUrl": "https://ex.com", "pattern": "a"},
                {"url": "https://ex.com/2", "sourceUrl": "https://ex.com", "pattern": "a"},
                {"url": "https://ex.com/3", "sourceUrl": "https://ex.com", "pattern": "b"},
            ]
        })

        assert len(parsed.groups) == 2
        # First group: (https://ex.com, "a") has 2 URLs
        # Second group: (https://ex.com, "b") has 1 URL

    def test_extracts_posted_at_by_url(self) -> None:
        """Test that postedAt values are extracted."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _parse_listing_batch,
        )

        parsed = _parse_listing_batch({
            "urls": [
                {
                    "url": "https://example.com/1",
                    "sourceUrl": "https://example.com",
                    "postedAt": 1704067200000,
                }
            ]
        })

        assert len(parsed.posted_at_groups) == 1


class TestExtractJobUrlsFromScrape:
    """Tests for _extract_job_urls_from_scrape helper function."""

    def test_extracts_from_job_urls_field(self) -> None:
        """Test extraction from items.job_urls field (SpiderCloud scraper format)."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "job_urls": [
                    "https://careers.example.com/positions/12345",
                    "https://careers.example.com/positions/67890",
                    "https://careers.example.com/positions/11111",
                ]
            }
        })

        assert len(urls) == 3
        assert "https://careers.example.com/positions/12345" in urls
        assert "https://careers.example.com/positions/67890" in urls
        assert "https://careers.example.com/positions/11111" in urls

    def test_extracts_from_job_urls_with_whitespace(self) -> None:
        """Test that job_urls with whitespace are stripped."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "job_urls": [
                    "  https://example.com/job/1  ",
                    "",
                    "   ",
                    "https://example.com/job/2",
                ]
            }
        })

        assert len(urls) == 2
        assert "https://example.com/job/1" in urls
        assert "https://example.com/job/2" in urls

    def test_job_urls_field_takes_priority(self) -> None:
        """Test that job_urls field is checked first and includes all URLs."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "job_urls": [
                    "https://example.com/job/1",
                    "https://example.com/job/2",
                ],
                "normalized": [
                    {"url": "https://example.com/job/3"},
                ],
            }
        })

        # Should have all 3 URLs since job_urls is checked first, then normalized
        assert len(urls) == 3
        assert "https://example.com/job/1" in urls
        assert "https://example.com/job/2" in urls
        assert "https://example.com/job/3" in urls

    def test_deduplicates_job_urls_and_normalized(self) -> None:
        """Test deduplication when same URL appears in job_urls and normalized."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "job_urls": [
                    "https://example.com/job/1",
                ],
                "normalized": [
                    {"url": "https://example.com/job/1"},  # duplicate
                    {"url": "https://example.com/job/2"},
                ],
            }
        })

        assert len(urls) == 2
        assert "https://example.com/job/1" in urls
        assert "https://example.com/job/2" in urls

    def test_ignores_non_string_job_urls(self) -> None:
        """Test that non-string values in job_urls are ignored."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "job_urls": [
                    "https://example.com/job/1",
                    None,
                    123,
                    {"url": "should be ignored"},
                    "https://example.com/job/2",
                ]
            }
        })

        assert len(urls) == 2
        assert "https://example.com/job/1" in urls
        assert "https://example.com/job/2" in urls

    def test_extracts_from_normalized(self) -> None:
        """Test extraction from normalized items."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "normalized": [
                    {"url": "https://example.com/job/1"},
                    {"url": "https://example.com/job/2"},
                ]
            }
        })

        assert len(urls) == 2
        assert "https://example.com/job/1" in urls
        assert "https://example.com/job/2" in urls

    def test_extracts_job_url_field(self) -> None:
        """Test extraction from job_url field."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "normalized": [
                    {"job_url": "https://example.com/job/1"},
                ]
            }
        })

        assert len(urls) == 1
        assert "https://example.com/job/1" in urls

    def test_extracts_from_raw_items(self) -> None:
        """Test extraction from raw items."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "raw": [
                    {"url": "https://example.com/job/1"},
                ]
            }
        })

        assert len(urls) == 1

    def test_deduplicates_urls(self) -> None:
        """Test that URLs are deduplicated between normalized and raw."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        urls = _extract_job_urls_from_scrape({
            "items": {
                "normalized": [{"url": "https://example.com/job/1"}],
                "raw": [{"url": "https://example.com/job/1"}],
            }
        })

        assert len(urls) == 1

    def test_returns_empty_for_invalid_payload(self) -> None:
        """Test that invalid payload returns empty list."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _extract_job_urls_from_scrape,
        )

        assert _extract_job_urls_from_scrape({}) == []
        assert _extract_job_urls_from_scrape({"items": None}) == []
        assert _extract_job_urls_from_scrape({"items": []}) == []


class TestFilterValidJobUrls:
    """Tests for _filter_valid_job_urls helper function."""

    def test_returns_empty_for_empty_input(self) -> None:
        """Test that empty input returns empty lists."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _filter_valid_job_urls,
        )

        valid, invalid = _filter_valid_job_urls([], "", None)

        assert valid == []
        assert invalid == []

    def test_deduplicates_urls(self) -> None:
        """Test that duplicate URLs are removed."""
        from job_scrape_application.workflows.workflow.scrape_listing_batch import (
            _filter_valid_job_urls,
        )

        valid, invalid = _filter_valid_job_urls(
            ["https://example.com/job/1", "https://example.com/job/1"],
            "",
            None,
        )

        assert len(valid) == 1
