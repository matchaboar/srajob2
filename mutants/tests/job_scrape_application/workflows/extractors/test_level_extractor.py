"""
Unit tests for the level extractor.

These tests verify that the LevelExtractor produces the CORRECT expected values
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
from job_scrape_application.workflows.extractors.level_extractor import (
    ContentPatternLevelStrategy,
    DefaultLevelStrategy,
    ExplicitLevelFieldStrategy,
    LevelExtractor,
    TitleLevelStrategy,
    _normalize_level,
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


# Filter test cases that have level expectations
LEVEL_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if "level" in tc.expected
]


@pytest.mark.parametrize(
    "test_case",
    LEVEL_TEST_CASES,
    ids=get_test_ids(LEVEL_TEST_CASES),
)
def test_level_extraction_accuracy(test_case: ExtractorTestCase) -> None:
    """
    Test that level extraction produces the CORRECT expected value.

    The assertion file specifies what the level SHOULD be.
    If this test fails, the level extractor logic needs to be fixed.
    """
    result = run_extractor_test(test_case, "level", match_type="exact")

    if not result.passed:
        pytest.fail(result.format_failure())


@pytest.mark.parametrize(
    "test_case",
    LEVEL_TEST_CASES[:5] if len(LEVEL_TEST_CASES) >= 5 else LEVEL_TEST_CASES,
    ids=get_test_ids(LEVEL_TEST_CASES[:5] if len(LEVEL_TEST_CASES) >= 5 else LEVEL_TEST_CASES),
)
def test_level_strategy_selection(test_case: ExtractorTestCase) -> None:
    """
    Test that the correct strategy wins for level extraction.
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

    result = extract_field(context, "level", run_all=True)

    # Verify a strategy won
    assert result.winning_strategy is not None, (
        f"No winning strategy for {test_case.identifier}. "
        f"All results: {[r.to_dict() for r in result.all_results]}"
    )


# =============================================================================
# Unit tests for level normalization
# =============================================================================


class TestLevelNormalization:
    """Tests for _normalize_level function."""

    def test_normalizes_intern(self) -> None:
        """Should normalize intern variants."""
        assert _normalize_level("intern") == "junior"
        assert _normalize_level("Internship") == "junior"

    def test_normalizes_junior(self) -> None:
        """Should normalize junior variants."""
        assert _normalize_level("junior") == "junior"
        assert _normalize_level("Jr") == "junior"
        assert _normalize_level("Jr.") == "junior"

    def test_normalizes_mid(self) -> None:
        """Should normalize mid-level."""
        assert _normalize_level("mid") == "mid"
        assert _normalize_level("mid-level") == "mid"

    def test_normalizes_senior(self) -> None:
        """Should normalize senior variants."""
        assert _normalize_level("senior") == "senior"
        assert _normalize_level("Sr") == "senior"
        assert _normalize_level("Sr.") == "senior"

    def test_normalizes_staff_level_titles(self) -> None:
        """Should normalize staff-level titles."""
        staff_titles = ["staff", "principal", "director", "vp", "chief", "head", "lead"]
        for title in staff_titles:
            assert _normalize_level(title) == "staff", f"{title} should normalize to staff"

    def test_manager_normalizes_to_senior(self) -> None:
        """Manager is ambiguous and normalizes to senior."""
        assert _normalize_level("manager") == "senior"

    def test_returns_empty_for_unknown(self) -> None:
        """Should return empty string for unknown levels."""
        assert _normalize_level("unknown") == ""
        assert _normalize_level("random") == ""


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestExplicitLevelFieldStrategy:
    """Tests for ExplicitLevelFieldStrategy."""

    def test_extracts_from_level_field(self) -> None:
        """Should extract and normalize level from raw_row."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"level": "senior"},
        )
        strategy = ExplicitLevelFieldStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "senior"

    def test_normalizes_seniority_field(self) -> None:
        """Should extract from seniority field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"seniority": "Principal"},
        )
        strategy = ExplicitLevelFieldStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "staff"

    def test_skips_when_no_field(self) -> None:
        """Should skip when no level field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={},
        )
        strategy = ExplicitLevelFieldStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestTitleLevelStrategy:
    """Tests for TitleLevelStrategy."""

    def test_detects_senior_in_title(self) -> None:
        """Should detect 'Senior' in title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Senior Software Engineer",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "senior"

    def test_detects_staff_in_title(self) -> None:
        """Should detect 'Staff' in title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Staff Engineer",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "staff"

    def test_detects_junior_in_title(self) -> None:
        """Should detect 'Junior' in title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Junior Developer",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "junior"

    def test_detects_intern_in_title(self) -> None:
        """Should detect 'Intern' in title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Software Engineering Intern",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "junior"

    def test_detects_director_as_staff(self) -> None:
        """Should detect 'Director' as staff-level."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Director of Engineering",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "staff"

    def test_detects_vp_as_staff(self) -> None:
        """Should detect 'VP' as staff-level."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="VP of Product",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "staff"

    def test_detects_lead_as_staff(self) -> None:
        """Should detect 'Lead' in title as staff-level."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Lead Data Scientist",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "staff"

    def test_skips_when_no_level_indicator(self) -> None:
        """Should skip when no level indicator in title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Software Engineer",
        )
        strategy = TitleLevelStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestContentPatternLevelStrategy:
    """Tests for ContentPatternLevelStrategy."""

    def test_detects_level_in_content(self) -> None:
        """Should detect level in content."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="We're looking for a senior engineer to join our team.",
        )
        strategy = ContentPatternLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "senior"

    def test_skips_verb_tokens(self) -> None:
        """Should skip 'lead' when used as verb."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="You will lead the analytics team and drive results.",
        )
        strategy = ContentPatternLevelStrategy()
        result = strategy.extract(context)

        # Should skip "lead" as it's likely a verb in this context
        # The strategy should either skip entirely or not return "staff" level
        # (since "lead" in this context is a verb, not a job title)
        assert not result.is_valid or result.value != "staff"


class TestDefaultLevelStrategy:
    """Tests for DefaultLevelStrategy."""

    def test_defaults_to_mid(self) -> None:
        """Should default to mid when no signals found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
        )
        strategy = DefaultLevelStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "mid"
        assert result.confidence < 0.5


# =============================================================================
# Integration tests
# =============================================================================


class TestLevelExtractorIntegration:
    """Integration tests for the full level extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = LevelExtractor()

        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_explicit_field_wins_over_title(self) -> None:
        """Explicit level field should have higher priority than title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"level": "junior"},
            extracted_title="Senior Engineer",  # Would suggest senior
        )

        result = extract_field(context, "level", run_all=True)

        assert result.final_value == "junior"
        assert result.winning_strategy == "explicit_level_field"

    def test_title_inference_when_no_explicit_field(self) -> None:
        """Should infer level from title when no explicit field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Principal Engineer",
        )

        result = extract_field(context, "level", run_all=True)

        assert result.final_value == "staff"
        assert result.winning_strategy == "title_level"

    def test_defaults_to_mid_when_no_signals(self) -> None:
        """Should default to mid when no level signals found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Software Engineer",  # No level indicator
        )

        result = extract_field(context, "level", run_all=True)

        assert result.final_value == "mid"
