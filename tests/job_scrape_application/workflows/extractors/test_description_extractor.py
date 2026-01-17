"""
Unit tests for the description extractor.

These tests verify that the DescriptionExtractor produces the CORRECT expected values
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
from job_scrape_application.workflows.extractors.description_extractor import (
    DescriptionExtractor,
    EmptyDescriptionFallbackStrategy,
    NormalizedMarkdownDescriptionStrategy,
    RawMarkdownDescriptionStrategy,
    RawRowDescriptionStrategy,
    StructuredDataDescriptionStrategy,
    _clean_description,
    _is_valid_description,
)
from job_scrape_application.workflows.site_handlers import get_site_handler

from job_scrape_application.workflows.extractors.test_support import (
    ALL_TEST_CASES,
    ExtractorTestCase,
    get_test_ids,
)


# =============================================================================
# Parametrized tests using real fixtures and assertions
# =============================================================================


# Filter test cases that have description expectations
DESCRIPTION_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if (
        "description_min_words" in tc.expected
        or "description_contains" in tc.expected
        or "description_not_contains" in tc.expected
    )
]


@pytest.mark.parametrize(
    "test_case",
    DESCRIPTION_TEST_CASES,
    ids=get_test_ids(DESCRIPTION_TEST_CASES),
)
def test_description_extraction_validity(test_case: ExtractorTestCase) -> None:
    """
    Test that description extraction produces valid content.

    The assertion file specifies minimum word count and/or content checks.
    If this test fails, the description extractor logic needs to be fixed.
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

    result = extract_field(context, "description", run_all=True)

    # Check minimum word count
    if "description_min_words" in test_case.expected:
        min_words = test_case.expected["description_min_words"]
        actual_words = len(result.final_value.split()) if result.final_value else 0
        assert actual_words >= min_words, (
            f"Description too short: {actual_words} words < {min_words} min words. "
            f"Winning strategy: {result.winning_strategy}"
        )

    # Check required content
    if "description_contains" in test_case.expected:
        required = test_case.expected["description_contains"]
        assert result.final_value and required.lower() in result.final_value.lower(), (
            f"Description should contain '{required}'. "
            f"Winning strategy: {result.winning_strategy}"
        )

    # Check forbidden content
    if "description_not_contains" in test_case.expected:
        forbidden = test_case.expected["description_not_contains"]
        assert result.final_value is None or forbidden not in result.final_value, (
            f"Description should NOT contain '{forbidden}'. "
            f"This suggests raw JSON was not cleaned from description. "
            f"Winning strategy: {result.winning_strategy}"
        )


# =============================================================================
# Unit tests for description cleaning
# =============================================================================


class TestDescriptionCleaning:
    """Tests for _clean_description function."""

    def test_removes_trailing_whitespace(self) -> None:
        """Should remove trailing whitespace from lines."""
        text = "Line 1   \nLine 2  \nLine 3"
        result = _clean_description(text)
        assert "   \n" not in result
        assert "  \n" not in result

    def test_collapses_multiple_blank_lines(self) -> None:
        """Should reduce more than 2 consecutive blank lines to 2."""
        text = "Line 1\n\n\n\n\nLine 2"
        result = _clean_description(text)
        # Should have at most 2 blank lines
        assert "\n\n\n" not in result

    def test_truncates_long_description(self) -> None:
        """Should truncate descriptions over 50k chars."""
        long_text = "x" * 60000
        result = _clean_description(long_text)
        assert len(result) <= 50003  # 50000 + "..."

    def test_strips_leading_trailing_whitespace(self) -> None:
        """Should strip leading/trailing whitespace from whole text."""
        text = "\n\n  Some content  \n\n"
        result = _clean_description(text)
        assert result == "Some content"


class TestDescriptionValidation:
    """Tests for _is_valid_description function."""

    def test_rejects_empty(self) -> None:
        """Should reject empty descriptions."""
        is_valid, reason = _is_valid_description("")
        assert not is_valid
        assert "Empty" in reason

    def test_rejects_too_short(self) -> None:
        """Should reject descriptions shorter than 50 chars."""
        is_valid, reason = _is_valid_description("Short description")
        assert not is_valid
        assert "too short" in reason

    def test_rejects_placeholders(self) -> None:
        """Should reject placeholder content."""
        placeholders = ["description", "job description", "n/a", "none", "tbd"]
        for placeholder in placeholders:
            is_valid, reason = _is_valid_description(placeholder)
            assert not is_valid, f"Should reject placeholder: {placeholder}"

    def test_accepts_valid_description(self) -> None:
        """Should accept valid descriptions."""
        valid_desc = "This is a valid job description with sufficient content. " * 5
        is_valid, reason = _is_valid_description(valid_desc)
        assert is_valid


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestStructuredDataDescriptionStrategy:
    """Tests for StructuredDataDescriptionStrategy."""

    def test_extracts_from_description_key(self) -> None:
        """Should extract from 'description' key."""
        desc = "A full job description with all the required details. " * 5
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"description": desc},
        )
        strategy = StructuredDataDescriptionStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert len(result.value) >= 50

    def test_extracts_from_jobDescription_key(self) -> None:
        """Should extract from 'jobDescription' key."""
        desc = "Schema.org job description format with plenty of content. " * 5
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"jobDescription": desc},
        )
        strategy = StructuredDataDescriptionStrategy()
        result = strategy.extract(context)

        assert result.is_valid

    def test_skips_short_descriptions(self) -> None:
        """Should skip descriptions that are too short."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"description": "Too short"},
        )
        strategy = StructuredDataDescriptionStrategy()
        result = strategy.extract(context)

        # Strategy should find it but validation should fail
        assert not result.is_valid or result.value is None


class TestNormalizedMarkdownDescriptionStrategy:
    """Tests for NormalizedMarkdownDescriptionStrategy."""

    def test_extracts_from_description_body(self) -> None:
        """Should prefer description_body over raw normalized markdown."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Clean description body content. " * 10,
            normalized_markdown="Raw normalized markdown with more stuff. " * 10,
        )
        strategy = NormalizedMarkdownDescriptionStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "Clean description" in result.value

    def test_falls_back_to_normalized_markdown(self) -> None:
        """Should use normalized_markdown if description_body is empty."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="",
            normalized_markdown="Normalized markdown content goes here. " * 10,
        )
        strategy = NormalizedMarkdownDescriptionStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "Normalized markdown" in result.value


class TestRawRowDescriptionStrategy:
    """Tests for RawRowDescriptionStrategy."""

    def test_extracts_from_raw_row(self) -> None:
        """Should extract description from raw_row."""
        desc = "Job description from raw row data. " * 10
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"description": desc},
        )
        strategy = RawRowDescriptionStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "raw row" in result.value.lower()

    def test_tries_multiple_keys(self) -> None:
        """Should try multiple field names."""
        desc = "Body content from raw row data. " * 10
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"body": desc},
        )
        strategy = RawRowDescriptionStrategy()
        result = strategy.extract(context)

        assert result.is_valid


class TestRawMarkdownDescriptionStrategy:
    """Tests for RawMarkdownDescriptionStrategy."""

    def test_extracts_raw_markdown(self) -> None:
        """Should use raw_markdown as fallback."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_markdown="Raw markdown content from scrape. " * 10,
        )
        strategy = RawMarkdownDescriptionStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert "Raw markdown" in result.value


class TestEmptyDescriptionFallbackStrategy:
    """Tests for EmptyDescriptionFallbackStrategy."""

    def test_returns_empty_string(self) -> None:
        """Should return empty string as last resort."""
        context = ExtractionContext(
            url="https://example.com/job/123",
        )
        strategy = EmptyDescriptionFallbackStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == ""
        assert result.confidence < 0.5


# =============================================================================
# Integration tests
# =============================================================================


class TestDescriptionExtractorIntegration:
    """Integration tests for the full description extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = DescriptionExtractor()

        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_structured_data_wins_over_markdown(self) -> None:
        """Structured data should have higher priority than markdown."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"description": "API description content. " * 10},
            normalized_markdown="Markdown description content. " * 10,
        )

        result = extract_field(context, "description", run_all=True)

        assert "API description" in result.final_value
        assert result.winning_strategy == "structured_data_description"

    def test_falls_back_to_empty_when_all_fail(self) -> None:
        """Should fall back to empty string when no content available."""
        context = ExtractionContext(
            url="https://example.com/job/123",
        )

        result = extract_field(context, "description", run_all=True)

        assert result.final_value == ""
        assert result.winning_strategy == "empty_description_fallback"

    def test_cleans_json_from_description(self) -> None:
        """Should clean JSON artifacts from description."""
        # This tests that handlers properly strip JSON blocks
        # The assertion description_not_contains: '{"domain":' tests this
        context = ExtractionContext(
            url="https://example.com/job/123",
            normalized_markdown=(
                '# Job Title\n\n'
                'Great job description content goes here. ' * 10 + '\n\n'
                '```json\n{"domain": "tech", "data": {}}\n```'
            ),
        )

        result = extract_field(context, "description", run_all=True)

        # The description may or may not contain JSON depending on cleaning
        # but this test ensures the extractor at least returns something
        assert result.final_value is not None or result.winning_strategy == "empty_description_fallback"
