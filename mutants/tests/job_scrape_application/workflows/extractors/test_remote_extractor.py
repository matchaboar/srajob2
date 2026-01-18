"""
Unit tests for the remote status extractor.

These tests verify that the RemoteExtractor produces the CORRECT expected values
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
from job_scrape_application.workflows.extractors.remote_extractor import (
    ContentRemotePatternStrategy,
    DefaultRemoteStrategy,
    ExplicitRemoteFlagStrategy,
    HintedRemoteStrategy,
    LocationRemoteStrategy,
    RemoteExtractor,
    TitleRemoteStrategy,
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


# Filter test cases that have is_remote expectations
REMOTE_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if "is_remote" in tc.expected
]


@pytest.mark.parametrize(
    "test_case",
    REMOTE_TEST_CASES,
    ids=get_test_ids(REMOTE_TEST_CASES),
)
def test_remote_extraction_accuracy(test_case: ExtractorTestCase) -> None:
    """
    Test that remote extraction produces the CORRECT expected value.

    The assertion file specifies what is_remote SHOULD be.
    If this test fails, the remote extractor logic needs to be fixed.
    """
    result = run_extractor_test(test_case, "remote", "is_remote", match_type="exact")

    if not result.passed:
        pytest.fail(result.format_failure())


@pytest.mark.parametrize(
    "test_case",
    REMOTE_TEST_CASES[:5] if len(REMOTE_TEST_CASES) >= 5 else REMOTE_TEST_CASES,
    ids=get_test_ids(REMOTE_TEST_CASES[:5] if len(REMOTE_TEST_CASES) >= 5 else REMOTE_TEST_CASES),
)
def test_remote_strategy_selection(test_case: ExtractorTestCase) -> None:
    """
    Test that the correct strategy wins for remote extraction.
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

    result = extract_field(context, "remote", run_all=True)

    # Verify a strategy won
    assert result.winning_strategy is not None, (
        f"No winning strategy for {test_case.identifier}. "
        f"All results: {[r.to_dict() for r in result.all_results]}"
    )


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestExplicitRemoteFlagStrategy:
    """Tests for ExplicitRemoteFlagStrategy."""

    def test_extracts_boolean_true(self) -> None:
        """Should extract True from boolean remote=True."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"remote": True},
        )
        strategy = ExplicitRemoteFlagStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True
        assert result.confidence > 0.9

    def test_extracts_boolean_false(self) -> None:
        """Should extract False from boolean remote=False."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"remote": False},
        )
        strategy = ExplicitRemoteFlagStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is False

    def test_parses_string_true(self) -> None:
        """Should parse string 'true' as True."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"remote": "true"},
        )
        strategy = ExplicitRemoteFlagStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True

    def test_parses_string_remote(self) -> None:
        """Should parse string 'remote' as True."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"is_remote": "remote"},
        )
        strategy = ExplicitRemoteFlagStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True

    def test_parses_string_onsite_as_false(self) -> None:
        """Should parse string 'onsite' as False."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"remote": "onsite"},
        )
        strategy = ExplicitRemoteFlagStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is False

    def test_skips_when_no_field(self) -> None:
        """Should skip when no remote field."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={},
        )
        strategy = ExplicitRemoteFlagStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestLocationRemoteStrategy:
    """Tests for LocationRemoteStrategy."""

    def test_detects_remote_in_location(self) -> None:
        """Should detect 'remote' in location as True."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_location="Remote - US",
        )
        strategy = LocationRemoteStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True

    def test_specific_location_skips_remote_inference(self) -> None:
        """Should skip specific city location (not infer remote status).

        Having a physical location doesn't mean a job is not remote -
        many hybrid jobs have physical locations but still allow remote work.
        """
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_location="San Francisco, CA",
        )
        strategy = LocationRemoteStrategy()
        result = strategy.extract(context)

        # Should skip - specific locations don't indicate remote status
        assert not result.is_valid

    def test_skips_country_level_location(self) -> None:
        """Should skip country-level locations as ambiguous."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_location="United States",
        )
        strategy = LocationRemoteStrategy()
        result = strategy.extract(context)

        # Country-level should be skipped as ambiguous
        assert not result.is_valid

    def test_skips_when_no_location(self) -> None:
        """Should skip when no location available."""
        context = ExtractionContext(
            url="https://example.com/job/123",
        )
        strategy = LocationRemoteStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestTitleRemoteStrategy:
    """Tests for TitleRemoteStrategy."""

    def test_detects_remote_in_title(self) -> None:
        """Should detect 'remote' in title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Senior Engineer (Remote)",
        )
        strategy = TitleRemoteStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True

    def test_detects_fully_remote_in_title(self) -> None:
        """Should detect 'fully remote' in title."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Fully Remote Product Manager",
        )
        strategy = TitleRemoteStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True

    def test_skips_when_no_remote_keyword(self) -> None:
        """Should skip when title has no remote keyword."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_title="Software Engineer",
        )
        strategy = TitleRemoteStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestContentRemotePatternStrategy:
    """Tests for ContentRemotePatternStrategy."""

    def test_detects_remote_in_content(self) -> None:
        """Should detect remote patterns in content."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="This is a fully remote position. Work from anywhere!",
        )
        strategy = ContentRemotePatternStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True

    def test_skips_when_no_remote_pattern(self) -> None:
        """Should skip when no remote pattern found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Join our team in San Francisco.",
        )
        strategy = ContentRemotePatternStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestHintedRemoteStrategy:
    """Tests for HintedRemoteStrategy."""

    def test_extracts_from_boolean_hint(self) -> None:
        """Should extract boolean from hints."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"remote": True},
        )
        strategy = HintedRemoteStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True

    def test_parses_string_hint(self) -> None:
        """Should parse string hint."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            hints={"remote": "yes"},
        )
        strategy = HintedRemoteStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is True


class TestDefaultRemoteStrategy:
    """Tests for DefaultRemoteStrategy."""

    def test_defaults_to_false(self) -> None:
        """Should default to False when no signals found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
        )
        strategy = DefaultRemoteStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value is False
        assert result.confidence < 0.5


# =============================================================================
# Integration tests
# =============================================================================


class TestRemoteExtractorIntegration:
    """Integration tests for the full remote extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = RemoteExtractor()

        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_explicit_flag_wins_over_location(self) -> None:
        """Explicit flag should have higher priority than location inference."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            raw_row={"remote": True},
            extracted_location="San Francisco, CA",  # Would suggest non-remote
        )

        result = extract_field(context, "remote", run_all=True)

        assert result.final_value is True
        assert result.winning_strategy == "explicit_remote_flag"

    def test_location_infers_remote_when_contains_remote(self) -> None:
        """Location containing 'remote' should infer remote."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_location="Remote - United States",
        )

        result = extract_field(context, "remote", run_all=True)

        assert result.final_value is True

    def test_defaults_to_false_when_no_signals(self) -> None:
        """Should default to False when no remote signals found."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            extracted_location="New York, NY",
        )

        result = extract_field(context, "remote", run_all=True)

        assert result.final_value is False
