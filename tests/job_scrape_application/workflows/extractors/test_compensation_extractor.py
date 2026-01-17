"""
Unit tests for the compensation extractor.

These tests verify that the CompensationExtractor produces the CORRECT expected values
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
from job_scrape_application.workflows.extractors.compensation_extractor import (
    CompensationExtractor,
    ContentPatternCompensationStrategy,
    ExplicitCompensationFieldStrategy,
    HintedCompensationStrategy,
    StructuredDataCompensationStrategy,
    _is_valid_compensation,
    _normalize_comp,
    _parse_comp_value,
)

from job_scrape_application.workflows.extractors.test_support import (
    ALL_TEST_CASES,
    ExtractorTestCase,
    get_test_ids,
    run_extractor_test,
)


# =============================================================================
# Parametrized tests using real fixtures and assertions
# =============================================================================


# Filter test cases that have compensation expectations
# Note: assertions may use cost_milli_cents_min which is cost, not compensation
COMPENSATION_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if "compensation" in tc.expected or "salary" in tc.expected
]


@pytest.mark.parametrize(
    "test_case",
    COMPENSATION_TEST_CASES,
    ids=get_test_ids(COMPENSATION_TEST_CASES),
)
def test_compensation_extraction_accuracy(test_case: ExtractorTestCase) -> None:
    """
    Test that compensation extraction produces the CORRECT expected value.

    The assertion file specifies what the compensation SHOULD be.
    If this test fails, the compensation extractor logic needs to be fixed.
    """
    expected_key = "compensation" if "compensation" in test_case.expected else "salary"
    result = run_extractor_test(test_case, "compensation", expected_key, match_type="exact")

    if not result.passed:
        pytest.fail(result.format_failure())


# =============================================================================
# Unit tests for parsing helpers
# =============================================================================


class TestCompensationParsing:
    """Tests for compensation parsing helpers."""

    def test_parse_comp_value_simple(self) -> None:
        """Should parse simple numeric strings."""
        assert _parse_comp_value("150000") == 150000
        assert _parse_comp_value("200000") == 200000

    def test_parse_comp_value_with_commas(self) -> None:
        """Should parse values with commas."""
        assert _parse_comp_value("150,000") == 150000
        assert _parse_comp_value("1,200,000") == 1200000

    def test_parse_comp_value_k_notation(self) -> None:
        """Should multiply small values by 1000 (K notation)."""
        assert _parse_comp_value("150") == 150000
        assert _parse_comp_value("200") == 200000

    def test_normalize_comp_valid_range(self) -> None:
        """Should accept values within valid range."""
        assert _normalize_comp(50000) == 50000
        assert _normalize_comp(150000) == 150000
        assert _normalize_comp(500000) == 500000

    def test_normalize_comp_too_low(self) -> None:
        """Should reject values below minimum."""
        assert _normalize_comp(15000) is None  # Below $20k

    def test_normalize_comp_too_high(self) -> None:
        """Should reject values above maximum."""
        assert _normalize_comp(3000000) is None  # Above $2M


class TestCompensationValidation:
    """Tests for compensation validation."""

    def test_rejects_none(self) -> None:
        """Should reject None."""
        is_valid, reason = _is_valid_compensation(None)
        assert not is_valid

    def test_rejects_too_low(self) -> None:
        """Should reject compensation below $20k."""
        is_valid, reason = _is_valid_compensation(15000)
        assert not is_valid
        assert "too low" in reason

    def test_rejects_too_high(self) -> None:
        """Should reject compensation above $2M."""
        is_valid, reason = _is_valid_compensation(3000000)
        assert not is_valid
        assert "too high" in reason

    def test_accepts_valid_compensation(self) -> None:
        """Should accept valid compensation values."""
        is_valid, reason = _is_valid_compensation(150000)
        assert is_valid


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestExplicitCompensationFieldStrategy:
    """Tests for ExplicitCompensationFieldStrategy."""

    def test_extracts_numeric_value(self) -> None:
        """Should extract numeric compensation from raw row."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"salary": 150000},
        )
        strategy = ExplicitCompensationFieldStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 150000

    def test_extracts_float_value(self) -> None:
        """Should extract float compensation."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"compensation": 175000.0},
        )
        strategy = ExplicitCompensationFieldStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 175000

    def test_skips_when_no_field(self) -> None:
        """Should skip when no compensation field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={},
        )
        strategy = ExplicitCompensationFieldStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestStructuredDataCompensationStrategy:
    """Tests for StructuredDataCompensationStrategy."""

    def test_extracts_from_salary_key(self) -> None:
        """Should extract from salary key."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"salary": 180000},
        )
        strategy = StructuredDataCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 180000

    def test_extracts_from_nested_baseSalary(self) -> None:
        """Should extract from Schema.org nested salary structure."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={
                "baseSalary": {
                    "value": 200000
                }
            },
        )
        strategy = StructuredDataCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 200000


class TestHintedCompensationStrategy:
    """Tests for HintedCompensationStrategy."""

    def test_extracts_from_compensation_range(self) -> None:
        """Should extract average from compensation range."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"compensation_range": {"low": 150000, "high": 200000}},
        )
        strategy = HintedCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 175000  # Average

    def test_extracts_from_range_list(self) -> None:
        """Should extract from [low, high] list format."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"compensation_range": [140000, 180000]},
        )
        strategy = HintedCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 160000  # Average

    def test_extracts_single_compensation(self) -> None:
        """Should extract single compensation value."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"compensation": 165000},
        )
        strategy = HintedCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 165000


class TestContentPatternCompensationStrategy:
    """Tests for ContentPatternCompensationStrategy."""

    def test_extracts_usd_range(self) -> None:
        """Should extract $X-$Y range pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Compensation: $150,000 - $200,000 per year",
        )
        strategy = ContentPatternCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 175000  # Average

    def test_extracts_k_range(self) -> None:
        """Should extract $Xk-$Yk range pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Salary: $150K - $180K",
        )
        strategy = ContentPatternCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid

    def test_extracts_single_value(self) -> None:
        """Should extract single $X pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Competitive salary starting at $160,000.",
        )
        strategy = ContentPatternCompensationStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == 160000

    def test_skips_401k(self) -> None:
        """Should skip 401k (retirement plan, not compensation)."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Benefits include 401k matching and health insurance.",
        )
        strategy = ContentPatternCompensationStrategy()
        result = strategy.extract(context)

        # Should not match 401k as compensation
        assert not result.is_valid or result.value != 401000


# =============================================================================
# Integration tests
# =============================================================================


class TestCompensationExtractorIntegration:
    """Integration tests for the full compensation extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = CompensationExtractor()

        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_explicit_wins_over_content(self) -> None:
        """Explicit field should have higher priority than content patterns."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"salary": 180000},
            description_body="Salary: $150,000 - $160,000",
        )

        result = extract_field(context, "compensation", run_all=True)

        assert result.final_value == 180000
        assert result.winning_strategy == "explicit_compensation_field"

    def test_defaults_to_zero_when_unknown(self) -> None:
        """Should return 0 when compensation is unknown."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Competitive compensation offered.",
        )

        result = extract_field(context, "compensation", run_all=True)

        assert result.final_value == 0
        assert result.winning_strategy == "unknown_compensation"
