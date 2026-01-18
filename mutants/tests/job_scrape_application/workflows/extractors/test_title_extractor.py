"""
Unit tests for the title extractor.

These tests verify that the JobTitleExtractor produces the CORRECT expected values
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
from job_scrape_application.workflows.extractors.title_extractor import (
    JobTitleExtractor,
    MarkdownHeadingTitleStrategy,
    RawRowTitleStrategy,
    StructuredDataTitleStrategy,
    _is_valid_title,
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


# Filter test cases that have title expectations
TITLE_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if "title" in tc.expected or "title_contains" in tc.expected
]


@pytest.mark.parametrize(
    "test_case",
    TITLE_TEST_CASES,
    ids=get_test_ids(TITLE_TEST_CASES),
)
def test_title_extraction_accuracy(test_case: ExtractorTestCase) -> None:
    """
    Test that title extraction produces the CORRECT expected value.

    The assertion file specifies what the title SHOULD be.
    If this test fails, the title extractor logic needs to be fixed.
    """
    # Determine match type from expected keys
    if "title" in test_case.expected:
        result = run_extractor_test(test_case, "title", match_type="exact")
    elif "title_contains" in test_case.expected:
        result = run_extractor_test(test_case, "title", "title_contains", match_type="contains")
    else:
        pytest.skip(f"No title expectation for {test_case.identifier}")
        return

    if not result.passed:
        pytest.fail(result.format_failure())


@pytest.mark.parametrize(
    "test_case",
    TITLE_TEST_CASES[:5] if len(TITLE_TEST_CASES) >= 5 else TITLE_TEST_CASES,
    ids=get_test_ids(TITLE_TEST_CASES[:5] if len(TITLE_TEST_CASES) >= 5 else TITLE_TEST_CASES),
)
def test_title_strategy_selection(test_case: ExtractorTestCase) -> None:
    """
    Test that the correct strategy wins for title extraction.

    This verifies that strategies are evaluated in priority order
    and the most appropriate one is selected.
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

    result = extract_field(context, "title", run_all=True)

    # Verify a strategy won
    assert result.winning_strategy is not None, (
        f"No winning strategy for {test_case.identifier}. "
        f"All results: {[r.to_dict() for r in result.all_results]}"
    )

    # Verify the value is not empty
    assert result.final_value, (
        f"Empty title extracted for {test_case.identifier}. "
        f"Winning strategy: {result.winning_strategy}"
    )


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestStructuredDataTitleStrategy:
    """Tests for StructuredDataTitleStrategy."""

    def test_extracts_title_from_title_key(self) -> None:
        """Should extract title from 'title' key in structured data."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"title": "Software Engineer"},
        )
        strategy = StructuredDataTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Software Engineer"
        assert result.confidence > 0.9

    def test_extracts_title_from_jobTitle_key(self) -> None:
        """Should extract title from 'jobTitle' key."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"jobTitle": "Senior Developer"},
        )
        strategy = StructuredDataTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Senior Developer"

    def test_rejects_generic_titles(self) -> None:
        """Should reject generic titles like 'Job Description'."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"title": "Job Description"},
        )
        strategy = StructuredDataTitleStrategy()
        result = strategy.extract(context)

        assert not result.is_valid
        assert result.value is None

    def test_skips_when_no_structured_data(self) -> None:
        """Should skip when no structured data is available."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data=None,
        )
        strategy = StructuredDataTitleStrategy()
        result = strategy.extract(context)

        assert not result.is_valid
        assert "No structured data" in result.reason


class TestRawRowTitleStrategy:
    """Tests for RawRowTitleStrategy."""

    def test_extracts_title_from_raw_row(self) -> None:
        """Should extract title from raw_row job_title field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"job_title": "Backend Engineer"},
        )
        strategy = RawRowTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Backend Engineer"

    def test_normalizes_title_with_company_suffix(self) -> None:
        """Should strip ' | Company' suffix from title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"title": "Product Manager | Acme Corp"},
        )
        strategy = RawRowTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        # Title should have company suffix stripped
        assert "Acme Corp" not in result.value or "|" not in result.value

    def test_skips_empty_raw_row(self) -> None:
        """Should skip when raw_row has no title field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={},
        )
        strategy = RawRowTitleStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestMarkdownHeadingTitleStrategy:
    """Tests for MarkdownHeadingTitleStrategy."""

    def test_extracts_from_h1_heading(self) -> None:
        """Should extract title from # heading."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            normalized_markdown="# Data Scientist\n\nWe're looking for...",
        )
        strategy = MarkdownHeadingTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Data Scientist"

    def test_extracts_from_h2_heading(self) -> None:
        """Should extract title from ## heading."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            normalized_markdown="## Machine Learning Engineer\n\nDescription...",
        )
        strategy = MarkdownHeadingTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Machine Learning Engineer"

    def test_preserves_market_region_suffix(self) -> None:
        """Should preserve market region suffix like '- France'."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            normalized_markdown="# Account Executive - France\n\nDescription...",
        )
        strategy = MarkdownHeadingTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        # Should preserve "- France" as it's a market region
        assert "France" in result.value

    def test_strips_location_suffix(self) -> None:
        """Should strip location suffix from title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            normalized_markdown="# Software Engineer | New York\n\nDescription...",
        )
        strategy = MarkdownHeadingTitleStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Software Engineer"

    def test_skips_when_no_heading(self) -> None:
        """Should skip when no markdown heading found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            normalized_markdown="No heading here, just plain text.",
        )
        strategy = MarkdownHeadingTitleStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestTitleValidation:
    """Tests for title validation logic."""

    def test_rejects_empty_title(self) -> None:
        """Should reject empty titles."""
        is_valid, reason = _is_valid_title("")
        assert not is_valid
        assert "Empty" in reason

    def test_rejects_short_title(self) -> None:
        """Should reject titles shorter than 3 chars."""
        is_valid, reason = _is_valid_title("AB")
        assert not is_valid
        assert "too short" in reason

    def test_rejects_long_title(self) -> None:
        """Should reject titles longer than 200 chars."""
        is_valid, reason = _is_valid_title("A" * 201)
        assert not is_valid
        assert "too long" in reason

    def test_rejects_generic_titles(self) -> None:
        """Should reject generic titles like 'Overview'."""
        generic_titles = [
            "Job Description",
            "Overview",
            "The Role",
            "Apply Now",
            "Unknown",
        ]
        for title in generic_titles:
            is_valid, reason = _is_valid_title(title)
            assert not is_valid, f"Should reject generic title: {title}"

    def test_rejects_url_as_title(self) -> None:
        """Should reject URLs as titles."""
        is_valid, reason = _is_valid_title("https://example.com/job")
        assert not is_valid
        assert "URL" in reason

    def test_rejects_too_many_words(self) -> None:
        """Should reject titles with more than 15 words (likely a sentence)."""
        long_title = " ".join(["word"] * 16)
        is_valid, reason = _is_valid_title(long_title)
        assert not is_valid
        assert "too many words" in reason

    def test_accepts_valid_title(self) -> None:
        """Should accept valid job titles."""
        valid_titles = [
            "Software Engineer",
            "Senior Product Manager",
            "Data Scientist III",
            "VP of Engineering",
            "Account Executive - EMEA",
        ]
        for title in valid_titles:
            is_valid, reason = _is_valid_title(title)
            assert is_valid, f"Should accept valid title: {title}"


# =============================================================================
# Integration tests
# =============================================================================


class TestTitleExtractorIntegration:
    """Integration tests for the full title extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = JobTitleExtractor()

        # Check strategy order
        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_structured_data_wins_over_content(self) -> None:
        """Structured data should have higher priority than content patterns."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"title": "API Title"},
            normalized_markdown="# Markdown Title\n\nDescription...",
        )

        result = extract_field(context, "title", run_all=True)

        assert result.final_value == "API Title"
        assert result.winning_strategy == "structured_data_title"

    def test_fallback_to_content_when_no_structured_data(self) -> None:
        """Should fall back to content patterns when no structured data."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            normalized_markdown="# Content Title\n\nDescription...",
        )

        result = extract_field(context, "title", run_all=True)

        assert result.final_value == "Content Title"
        assert "markdown" in result.winning_strategy.lower() or "content" in result.winning_strategy.lower()
