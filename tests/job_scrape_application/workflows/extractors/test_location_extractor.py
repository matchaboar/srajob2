"""
Unit tests for the location extractor.

These tests verify that the LocationExtractor produces the CORRECT expected values
from assertion files. Tests are designed to FAIL when extraction logic is wrong.

The assertion file value is always the CORRECT value - if the test fails,
the extractor needs to be fixed.
"""

from __future__ import annotations

import pytest

from job_scrape_application.workflows.extractors import (
    ExtractionContext,
    extract_field,
)
from job_scrape_application.workflows.extractors.location_extractor import (
    ContentPatternLocationStrategy,
    ExplicitLabelLocationStrategy,
    HintedLocationStrategy,
    LocationExtractor,
    RawRowLocationStrategy,
    StructuredDataLocationStrategy,
    _is_valid_location,
)
from job_scrape_application.workflows.site_handlers import get_site_handler

from job_scrape_application.workflows.extractors.test_support import (
    ALL_TEST_CASES,
    ExtractorTestCase,
    get_test_ids,
    run_extractor_test,
)


# =============================================================================
# Parametrized tests using real fixtures and assertions
# =============================================================================


# Filter test cases that have location expectations
LOCATION_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if "location" in tc.expected or "location_contains" in tc.expected
]


@pytest.mark.parametrize(
    "test_case",
    LOCATION_TEST_CASES,
    ids=get_test_ids(LOCATION_TEST_CASES),
)
def test_location_extraction_accuracy(test_case: ExtractorTestCase) -> None:
    """
    Test that location extraction produces the CORRECT expected value.

    The assertion file specifies what the location SHOULD be.
    If this test fails, the location extractor logic needs to be fixed.
    """
    if "location" in test_case.expected:
        result = run_extractor_test(test_case, "location", match_type="exact")
    elif "location_contains" in test_case.expected:
        result = run_extractor_test(test_case, "location", "location_contains", match_type="contains")
    else:
        pytest.skip(f"No location expectation for {test_case.identifier}")
        return

    if not result.passed:
        pytest.fail(result.format_failure())


@pytest.mark.parametrize(
    "test_case",
    LOCATION_TEST_CASES[:5] if len(LOCATION_TEST_CASES) >= 5 else LOCATION_TEST_CASES,
    ids=get_test_ids(LOCATION_TEST_CASES[:5] if len(LOCATION_TEST_CASES) >= 5 else LOCATION_TEST_CASES),
)
def test_location_strategy_selection(test_case: ExtractorTestCase) -> None:
    """
    Test that the correct strategy wins for location extraction.
    """
    handler = get_site_handler(test_case.url)
    context = ExtractionContext.from_scrape_result(
        url=test_case.url,
        markdown=test_case.raw_markdown,
        handler=handler,
        raw_row=test_case.raw_row,
        structured_data=test_case.structured_data,
        debug=True,
    )

    result = extract_field(context, "location", run_all=True)

    # Verify a strategy won
    assert result.winning_strategy is not None, (
        f"No winning strategy for {test_case.identifier}. "
        f"All results: {[r.to_dict() for r in result.all_results]}"
    )


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestStructuredDataLocationStrategy:
    """Tests for StructuredDataLocationStrategy."""

    def test_extracts_from_location_key(self) -> None:
        """Should extract location from 'location' key."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"location": "San Francisco, CA"},
        )
        strategy = StructuredDataLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "San Francisco, CA"

    def test_extracts_from_locationName(self) -> None:
        """Should extract from Ashby-style locationName."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"locationName": "New York, NY"},
        )
        strategy = StructuredDataLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "New York, NY"

    def test_extracts_from_schema_org_format(self) -> None:
        """Should extract from Schema.org JobPosting format."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={
                "jobLocation": {
                    "address": {
                        "addressLocality": "Seattle",
                        "addressRegion": "WA",
                    }
                }
            },
        )
        strategy = StructuredDataLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "Seattle" in result.value

    def test_accepts_country_only_from_structured_data(self) -> None:
        """Should accept country-only from explicit structured data."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"location": "United States"},
        )
        strategy = StructuredDataLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "United States"

    def test_handles_list_of_locations(self) -> None:
        """Should handle array of locations."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"location": ["San Francisco, CA", "New York, NY"]},
        )
        strategy = StructuredDataLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "San Francisco" in result.value


class TestRawRowLocationStrategy:
    """Tests for RawRowLocationStrategy."""

    def test_extracts_from_raw_row(self) -> None:
        """Should extract location from raw_row."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"location": "Austin, TX"},
        )
        strategy = RawRowLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Austin, TX"

    def test_rejects_country_only_from_raw_row(self) -> None:
        """Should reject country-only locations from raw row."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"location": "United States"},
        )
        strategy = RawRowLocationStrategy()
        result = strategy.extract(context)

        # Raw row strategy should reject country-only
        assert not result.is_valid

    def test_handles_dict_location(self) -> None:
        """Should extract from nested dict with name field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"location": {"name": "Denver, CO"}},
        )
        strategy = RawRowLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Denver, CO"


class TestExplicitLabelLocationStrategy:
    """Tests for ExplicitLabelLocationStrategy."""

    def test_extracts_from_location_label(self) -> None:
        """Should extract from 'Location: City, ST' pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Location: Portland, OR\n\nWe are looking for...",
        )
        strategy = ExplicitLabelLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Portland, OR"

    def test_extracts_from_location_line(self) -> None:
        """Should extract from standalone 'Location:' line."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Job Title: Engineer\nLocation: Boston, MA\nDescription...",
        )
        strategy = ExplicitLabelLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Boston, MA"

    def test_skips_when_no_label(self) -> None:
        """Should skip when no location label found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="We're based in San Francisco and looking for engineers.",
        )
        strategy = ExplicitLabelLocationStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestContentPatternLocationStrategy:
    """Tests for ContentPatternLocationStrategy."""

    def test_extracts_from_based_in_pattern(self) -> None:
        """Should extract from 'Based in [Location]' pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="We are based in Chicago, IL and growing fast.",
        )
        strategy = ContentPatternLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "Chicago" in result.value

    def test_extracts_from_remote_with_location(self) -> None:
        """Should extract from 'Remote (City, ST)' pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="This is a remote position (San Diego, CA).",
        )
        strategy = ContentPatternLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "San Diego" in result.value

    def test_extracts_city_state_pattern(self) -> None:
        """Should extract standalone City, ST pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Our office in Miami, FL is hiring.",
        )
        strategy = ContentPatternLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid


class TestHintedLocationStrategy:
    """Tests for HintedLocationStrategy."""

    def test_extracts_from_hints(self) -> None:
        """Should extract from hints.location."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"location": "Los Angeles, CA"},
        )
        strategy = HintedLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Los Angeles, CA"

    def test_extracts_from_locations_list(self) -> None:
        """Should extract from hints.locations array."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"locations": ["Phoenix, AZ", "Tucson, AZ"]},
        )
        strategy = HintedLocationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "Phoenix" in result.value

    def test_filters_remote_only_values(self) -> None:
        """Should filter out 'Remote' when it's the only location."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"location": "Remote"},
        )
        strategy = HintedLocationStrategy()
        result = strategy.extract(context)

        # Should skip remote-only as it's not a real location
        assert not result.is_valid


class TestLocationValidation:
    """Tests for location validation logic."""

    def test_rejects_empty_location(self) -> None:
        """Should reject empty locations."""
        is_valid, reason = _is_valid_location("")
        assert not is_valid

    def test_rejects_job_title_words(self) -> None:
        """Should reject strings containing job title words."""
        invalid_locations = [
            "Software Engineer",
            "Senior Developer",
            "Python Developer",
            "Manager Position",
        ]
        for loc in invalid_locations:
            is_valid, reason = _is_valid_location(loc)
            assert not is_valid, f"Should reject job title: {loc}"

    def test_rejects_invalid_state_codes(self) -> None:
        """Should reject City, XX patterns with invalid state codes."""
        is_valid, reason = _is_valid_location("City, ZZ")
        assert not is_valid

    def test_rejects_degree_abbreviations(self) -> None:
        """Should reject degree abbreviations like 'BS, MS'."""
        is_valid, reason = _is_valid_location("BS, MS")
        assert not is_valid

    def test_accepts_valid_city_state(self) -> None:
        """Should accept valid City, ST format."""
        valid_locations = [
            "San Francisco, CA",
            "New York, NY",
            "Austin, TX",
            "Seattle, WA",
            "Toronto, ON",  # Canadian province
        ]
        for loc in valid_locations:
            is_valid, reason = _is_valid_location(loc)
            assert is_valid, f"Should accept valid location: {loc}"

    def test_rejects_country_only_without_flag(self) -> None:
        """Should reject country-only without allow_country_only flag."""
        is_valid, reason = _is_valid_location("United States")
        assert not is_valid

    def test_accepts_country_only_with_flag(self) -> None:
        """Should accept country-only with allow_country_only flag."""
        is_valid, reason = _is_valid_location("United States", allow_country_only=True)
        assert is_valid


# =============================================================================
# Integration tests
# =============================================================================


class TestLocationExtractorIntegration:
    """Integration tests for the full location extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = LocationExtractor()

        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_structured_data_wins_over_content(self) -> None:
        """Structured data should have higher priority than content patterns."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"location": "API Location, CA"},
            description_body="Based in Content Location, NY.",
        )

        result = extract_field(context, "location", run_all=True)

        assert result.final_value == "API Location, CA"
        assert result.winning_strategy == "structured_data_location"

    def test_remote_fallback_when_no_location(self) -> None:
        """Should fall back to 'Remote' when job is remote and no location found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"remote": True},
        )

        result = extract_field(context, "location", run_all=True)

        assert result.final_value == "Remote"
        assert result.winning_strategy == "remote_fallback_location"
