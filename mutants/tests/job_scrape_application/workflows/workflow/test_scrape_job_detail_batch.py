"""Tests for scrape_job_detail_batch workflow helper functions.

Note: Full workflow integration tests require DBOS initialization.
These tests focus on the deterministic helper functions.
"""

from __future__ import annotations


class TestParseDetailBatch:
    """Tests for _parse_detail_batch helper function."""

    def test_parses_valid_entries(self) -> None:
        """Test parsing of valid URL entries."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _parse_detail_batch,
        )

        parsed = _parse_detail_batch({
            "urls": [
                {
                    "url": "https://example.com/job/123",
                    "sourceUrl": "https://example.com",
                    "siteId": "site-123",
                }
            ]
        })

        assert len(parsed.entries) == 1
        assert len(parsed.urls) == 1
        assert parsed.site_id == "site-123"

    def test_filters_listing_urls(self) -> None:
        """Test that listing URLs are filtered out."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _parse_detail_batch,
        )

        parsed = _parse_detail_batch({
            "urls": [
                {"url": "https://example.com/jobs", "urlType": "listing"},
                {"url": "https://example.com/job/123", "urlType": "detail"},
            ]
        })

        # Only detail URL should be included
        assert len(parsed.urls) == 1
        assert parsed.urls[0] == "https://example.com/job/123"

    def test_extracts_site_id_from_entries(self) -> None:
        """Test that site_id is extracted from entries."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _parse_detail_batch,
        )

        parsed = _parse_detail_batch({
            "urls": [
                {
                    "url": "https://example.com/job/123",
                    "siteId": "site-123",
                }
            ]
        })

        assert parsed.site_id == "site-123"

    def test_extracts_pattern_from_entries(self) -> None:
        """Test that pattern is extracted from entries."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _parse_detail_batch,
        )

        parsed = _parse_detail_batch({
            "urls": [
                {
                    "url": "https://example.com/job/123",
                    "pattern": "greenhouse",
                }
            ]
        })

        assert parsed.pattern == "greenhouse"

    def test_extracts_posted_at_by_url(self) -> None:
        """Test that postedAt is extracted by URL."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _parse_detail_batch,
        )

        parsed = _parse_detail_batch({
            "urls": [
                {
                    "url": "https://example.com/job/123",
                    "postedAt": 1704067200000,
                }
            ]
        })

        assert len(parsed.posted_at_by_url) == 1

    def test_ignores_non_dict_entries(self) -> None:
        """Test that non-dict entries are ignored."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _parse_detail_batch,
        )

        parsed = _parse_detail_batch({
            "urls": ["invalid", None, 123, {"url": "https://example.com/job/1"}]
        })

        assert len(parsed.entries) == 1

    def test_builds_url_to_entry_mapping(self) -> None:
        """Test that URL to entry mapping is built correctly."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _parse_detail_batch,
        )

        parsed = _parse_detail_batch({
            "urls": [
                {"url": "https://example.com/job/123", "_id": "entry-1"},
            ]
        })

        assert "https://example.com/job/123" in parsed.url_to_entry
        assert parsed.url_to_entry["https://example.com/job/123"]["_id"] == "entry-1"


class TestNormalizeJobFields:
    """Tests for job field normalization."""

    def test_extracts_normalized_items(self) -> None:
        """Test extraction from normalized items."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _normalize_job_fields,
        )

        result = _normalize_job_fields({
            "scrape": {
                "items": {
                    "normalized": [
                        {
                            "title": "Software Engineer",
                            "company": "Acme Corp",
                            "url": "https://example.com/job/123",
                        }
                    ]
                }
            }
        })

        assert len(result) == 1
        assert result[0]["title"] == "Software Engineer"
        assert result[0]["company"] == "Acme Corp"

    def test_extracts_from_normalized_sample(self) -> None:
        """Test extraction from normalizedSample (fallback)."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _normalize_job_fields,
        )

        result = _normalize_job_fields({
            "items": {
                "normalizedSample": [
                    {"title": "Data Scientist", "url": "https://example.com/job/456"}
                ]
            }
        })

        assert len(result) == 1
        assert result[0]["title"] == "Data Scientist"


class TestIdentify404Urls:
    """Tests for 404 URL identification."""

    def test_identifies_404_by_status(self) -> None:
        """Test identification by HTTP status code."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _identify_404_urls,
        )

        result = _identify_404_urls({
            "scrape": {
                "items": {
                    "failed": [
                        {"url": "https://example.com/job/gone", "status": 404}
                    ]
                }
            }
        })

        assert "https://example.com/job/gone" in result

    def test_identifies_404_by_reason(self) -> None:
        """Test identification by reason containing 404."""
        from job_scrape_application.workflows.workflow.scrape_job_detail_batch import (
            _identify_404_urls,
        )

        result = _identify_404_urls({
            "items": {
                "failed": [
                    {"url": "https://example.com/job/missing", "reason": "HTTP 404 Not Found"}
                ]
            }
        })

        assert "https://example.com/job/missing" in result
