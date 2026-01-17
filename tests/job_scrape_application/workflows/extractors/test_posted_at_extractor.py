"""
Unit tests for the posted_at extractor.

These tests verify that the PostedAtExtractor produces the CORRECT expected values
from assertion files. Tests are designed to FAIL when extraction logic is wrong.

The assertion file value is always the CORRECT value - if the test fails,
the extractor needs to be fixed.
"""

from __future__ import annotations

from datetime import datetime, date

import pytest

from job_scrape_application.workflows.extractors import (
    ExtractionContext,
    extract_field,
)
from job_scrape_application.workflows.extractors.posted_at_extractor import (
    ExplicitPostedAtFieldStrategy,
    HintedPostedAtStrategy,
    NowFallbackPostedAtStrategy,
    PostedAtExtractor,
    StructuredDataPostedAtStrategy,
    _is_valid_posted_at,
    _normalize_posted_at,
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


# Filter test cases that have posted_at expectations
POSTED_AT_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if "posted_at_not_null" in tc.expected
]


@pytest.mark.parametrize(
    "test_case",
    POSTED_AT_TEST_CASES,
    ids=get_test_ids(POSTED_AT_TEST_CASES),
)
def test_posted_at_extraction_not_null(test_case: ExtractorTestCase) -> None:
    """
    Test that posted_at extraction produces a non-null value.

    The assertion file specifies that posted_at SHOULD NOT be null.
    If this test fails, the posted_at extractor logic needs to be fixed.
    """
    result = run_extractor_test(test_case, "posted_at", "posted_at_not_null", match_type="not_null")

    if not result.passed:
        pytest.fail(result.format_failure())


@pytest.mark.parametrize(
    "test_case",
    POSTED_AT_TEST_CASES[:5] if len(POSTED_AT_TEST_CASES) >= 5 else POSTED_AT_TEST_CASES,
    ids=get_test_ids(POSTED_AT_TEST_CASES[:5] if len(POSTED_AT_TEST_CASES) >= 5 else POSTED_AT_TEST_CASES),
)
def test_posted_at_strategy_selection(test_case: ExtractorTestCase) -> None:
    """
    Test that the correct strategy wins for posted_at extraction.
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

    result = extract_field(context, "posted_at", run_all=True)

    # Verify a strategy won
    assert result.winning_strategy is not None, (
        f"No winning strategy for {test_case.identifier}. "
        f"All results: {[r.to_dict() for r in result.all_results]}"
    )


# =============================================================================
# Unit tests for date normalization
# =============================================================================


class TestPostedAtNormalization:
    """Tests for _normalize_posted_at function."""

    def test_normalizes_datetime(self) -> None:
        """Should pass through datetime objects."""
        dt = datetime(2024, 1, 15, 10, 30)
        result = _normalize_posted_at(dt)
        assert result == dt

    def test_normalizes_date(self) -> None:
        """Should convert date to datetime."""
        d = date(2024, 1, 15)
        result = _normalize_posted_at(d)
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_normalizes_unix_timestamp_seconds(self) -> None:
        """Should normalize unix timestamp in seconds."""
        # Jan 15, 2024 00:00:00 UTC
        ts = 1705276800
        result = _normalize_posted_at(ts)
        assert isinstance(result, datetime)
        assert result.year == 2024

    def test_normalizes_unix_timestamp_milliseconds(self) -> None:
        """Should normalize unix timestamp in milliseconds."""
        ts = 1705276800000
        result = _normalize_posted_at(ts)
        assert isinstance(result, datetime)
        assert result.year == 2024

    def test_normalizes_iso_string(self) -> None:
        """Should parse ISO date strings."""
        result = _normalize_posted_at("2024-01-15")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_normalizes_iso_datetime_string(self) -> None:
        """Should parse ISO datetime strings."""
        result = _normalize_posted_at("2024-01-15T10:30:00Z")
        assert isinstance(result, datetime)
        assert result.year == 2024

    def test_returns_none_for_invalid(self) -> None:
        """Should return None for invalid values."""
        assert _normalize_posted_at(None) is None
        assert _normalize_posted_at("not a date") is None
        assert _normalize_posted_at([]) is None


class TestPostedAtValidation:
    """Tests for _is_valid_posted_at function."""

    def test_rejects_none(self) -> None:
        """Should reject None."""
        is_valid, reason = _is_valid_posted_at(None)
        assert not is_valid

    def test_rejects_date_too_old(self) -> None:
        """Should reject dates before 2010."""
        old_date = datetime(2005, 1, 1)
        is_valid, reason = _is_valid_posted_at(old_date)
        assert not is_valid
        assert "too old" in reason

    def test_rejects_future_date(self) -> None:
        """Should reject future dates."""
        future_date = datetime(2099, 1, 1)
        is_valid, reason = _is_valid_posted_at(future_date)
        assert not is_valid
        assert "future" in reason

    def test_accepts_valid_date(self) -> None:
        """Should accept valid recent dates."""
        valid_date = datetime(2024, 6, 15)
        is_valid, reason = _is_valid_posted_at(valid_date)
        assert is_valid


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestExplicitPostedAtFieldStrategy:
    """Tests for ExplicitPostedAtFieldStrategy."""

    def test_extracts_datetime_from_raw_row(self) -> None:
        """Should extract datetime from raw row."""
        dt = datetime(2024, 1, 15, 10, 30)
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"posted_at": dt},
        )
        strategy = ExplicitPostedAtFieldStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == dt

    def test_extracts_iso_string_from_raw_row(self) -> None:
        """Should parse ISO string from raw row."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"datePosted": "2024-01-15T10:30:00Z"},
        )
        strategy = ExplicitPostedAtFieldStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value.year == 2024

    def test_extracts_timestamp_from_raw_row(self) -> None:
        """Should parse unix timestamp from raw row."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"published_at": 1705276800000},  # Milliseconds
        )
        strategy = ExplicitPostedAtFieldStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value.year == 2024

    def test_skips_when_no_field(self) -> None:
        """Should skip when no posted_at field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={},
        )
        strategy = ExplicitPostedAtFieldStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestStructuredDataPostedAtStrategy:
    """Tests for StructuredDataPostedAtStrategy."""

    def test_extracts_from_datePosted(self) -> None:
        """Should extract from Schema.org datePosted."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"datePosted": "2024-02-20"},
        )
        strategy = StructuredDataPostedAtStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value.year == 2024
        assert result.value.month == 2

    def test_extracts_from_createdAt(self) -> None:
        """Should extract from createdAt field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"createdAt": "2024-03-10T08:00:00Z"},
        )
        strategy = StructuredDataPostedAtStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value.month == 3


class TestHintedPostedAtStrategy:
    """Tests for HintedPostedAtStrategy."""

    def test_extracts_from_hints(self) -> None:
        """Should extract from hints.posted_at."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"posted_at": "2024-04-15"},
        )
        strategy = HintedPostedAtStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value.month == 4

    def test_extracts_from_date_hint(self) -> None:
        """Should extract from hints.date."""
        dt = datetime(2024, 5, 20)
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"date": dt},
        )
        strategy = HintedPostedAtStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value.month == 5


class TestNowFallbackPostedAtStrategy:
    """Tests for NowFallbackPostedAtStrategy."""

    def test_returns_current_time(self) -> None:
        """Should return current datetime as fallback."""
        context = ExtractionContext(
            url="https://example.com/job/123",
        )
        strategy = NowFallbackPostedAtStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is not None
        # Should be close to now
        assert (datetime.now() - result.value).total_seconds() < 60
        assert result.confidence < 0.5


# =============================================================================
# Integration tests
# =============================================================================


class TestPostedAtExtractorIntegration:
    """Integration tests for the full posted_at extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = PostedAtExtractor()

        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_explicit_wins_over_structured(self) -> None:
        """Explicit field should have higher priority than structured data."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"posted_at": datetime(2024, 1, 10)},
            structured_data={"datePosted": "2024-02-20"},
        )

        result = extract_field(context, "posted_at", run_all=True)

        assert result.final_value.month == 1  # From explicit field
        assert result.winning_strategy == "explicit_posted_at_field"

    def test_falls_back_to_now(self) -> None:
        """Should fall back to current time when no date found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
        )

        result = extract_field(context, "posted_at", run_all=True)

        assert result.final_value is not None
        assert result.winning_strategy == "now_fallback_posted_at"
