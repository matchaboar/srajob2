"""Tests for enqueue_scheduled_listings workflow helper functions.

Note: Full workflow integration tests require DBOS initialization.
These tests focus on the deterministic helper functions.
"""

from __future__ import annotations


class TestGenerateListingUrls:
    """Tests for listing URL generation."""

    def test_generates_base_url(self) -> None:
        """Test that base URL is always included."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _generate_listing_urls_for_site,
        )

        urls = _generate_listing_urls_for_site({
            "url": "https://boards.greenhouse.io/company",
        })

        assert "https://boards.greenhouse.io/company" in urls

    def test_handles_missing_url(self) -> None:
        """Test that missing URL returns empty list."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _generate_listing_urls_for_site,
        )

        urls = _generate_listing_urls_for_site({})

        assert urls == []

    def test_handles_empty_url(self) -> None:
        """Test that empty URL returns empty list."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _generate_listing_urls_for_site,
        )

        urls = _generate_listing_urls_for_site({"url": ""})

        assert urls == []


class TestDedupeUrls:
    """Tests for URL deduplication."""

    def test_removes_duplicates(self) -> None:
        """Test that duplicate URLs are removed."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _dedupe_urls,
        )

        urls = _dedupe_urls([
            "https://example.com/1",
            "https://example.com/2",
            "https://example.com/1",
            "https://example.com/3",
            "https://example.com/2",
        ])

        assert len(urls) == 3
        assert urls == [
            "https://example.com/1",
            "https://example.com/2",
            "https://example.com/3",
        ]

    def test_preserves_order(self) -> None:
        """Test that first occurrence order is preserved."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _dedupe_urls,
        )

        urls = _dedupe_urls(["c", "a", "b", "a", "c"])

        assert urls == ["c", "a", "b"]


class TestLimitListingUrls:
    """Tests for listing URL pagination limiting."""

    def test_no_limit_returns_all(self) -> None:
        """Test that no limit returns all URLs."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _limit_listing_urls,
        )

        urls = [f"https://example.com?page={i}" for i in range(10)]
        result = _limit_listing_urls(urls, limit=None)

        assert len(result) == 10

    def test_zero_limit_returns_all(self) -> None:
        """Test that zero limit returns all URLs."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _limit_listing_urls,
        )

        urls = [f"https://example.com?page={i}" for i in range(10)]
        result = _limit_listing_urls(urls, limit=0)

        assert len(result) == 10

    def test_filters_urls_above_limit(self) -> None:
        """Test that URLs with page above limit are filtered."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _limit_listing_urls,
        )

        urls = [
            "https://example.com?page=1",
            "https://example.com?page=2",
            "https://example.com?page=5",
            "https://example.com?page=10",
        ]
        result = _limit_listing_urls(urls, limit=3)

        assert "https://example.com?page=1" in result
        assert "https://example.com?page=2" in result
        # page=5 and page=10 are above limit

    def test_includes_urls_without_page_param(self) -> None:
        """Test that URLs without page parameter are included."""
        from job_scrape_application.workflows.workflow.enqueue_scheduled_listings import (
            _limit_listing_urls,
        )

        urls = [
            "https://example.com",
            "https://example.com?page=1",
            "https://example.com?page=5",
        ]
        result = _limit_listing_urls(urls, limit=2)

        # Base URL has no page, so it's included
        assert "https://example.com" in result
        assert "https://example.com?page=1" in result
