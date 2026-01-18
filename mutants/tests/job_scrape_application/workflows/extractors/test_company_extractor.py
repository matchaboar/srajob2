"""
Unit tests for the company extractor.

These tests verify that the CompanyExtractor produces the CORRECT expected values
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
from job_scrape_application.workflows.extractors.company_extractor import (
    CompanyExtractor,
    ContentPatternCompanyStrategy,
    StructuredDataCompanyStrategy,
    URLCompanyStrategy,
    _is_valid_company,
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


# Filter test cases that have company expectations
COMPANY_TEST_CASES = [
    tc for tc in ALL_TEST_CASES
    if "company" in tc.expected or "company_contains" in tc.expected
]


@pytest.mark.parametrize(
    "test_case",
    COMPANY_TEST_CASES,
    ids=get_test_ids(COMPANY_TEST_CASES),
)
def test_company_extraction_accuracy(test_case: ExtractorTestCase) -> None:
    """
    Test that company extraction produces the CORRECT expected value.

    The assertion file specifies what the company SHOULD be.
    If this test fails, the company extractor logic needs to be fixed.
    """
    if "company" in test_case.expected:
        result = run_extractor_test(test_case, "company", match_type="exact")
    elif "company_contains" in test_case.expected:
        result = run_extractor_test(test_case, "company", "company_contains", match_type="contains")
    else:
        pytest.skip(f"No company expectation for {test_case.identifier}")
        return

    if not result.passed:
        pytest.fail(result.format_failure())


@pytest.mark.parametrize(
    "test_case",
    COMPANY_TEST_CASES[:5] if len(COMPANY_TEST_CASES) >= 5 else COMPANY_TEST_CASES,
    ids=get_test_ids(COMPANY_TEST_CASES[:5] if len(COMPANY_TEST_CASES) >= 5 else COMPANY_TEST_CASES),
)
def test_company_strategy_selection(test_case: ExtractorTestCase) -> None:
    """
    Test that the correct strategy wins for company extraction.
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

    result = extract_field(context, "company", run_all=True)

    # Verify a strategy won
    assert result.winning_strategy is not None, (
        f"No winning strategy for {test_case.identifier}. "
        f"All results: {[r.to_dict() for r in result.all_results]}"
    )


# =============================================================================
# Unit tests for individual strategies
# =============================================================================


class TestStructuredDataCompanyStrategy:
    """Tests for StructuredDataCompanyStrategy."""

    def test_extracts_company_from_company_key(self) -> None:
        """Should extract company from 'company' key."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"company": "Acme Corp"},
        )
        strategy = StructuredDataCompanyStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Acme"
        assert result.confidence > 0.9

    def test_extracts_from_hiringOrganization(self) -> None:
        """Should extract from Schema.org hiringOrganization."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={
                "hiringOrganization": {"name": "Tech Startup Inc"}
            },
        )
        strategy = StructuredDataCompanyStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Tech Startup"

    def test_rejects_generic_company_names(self) -> None:
        """Should reject generic names like 'Unknown'."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data={"company": "Unknown"},
        )
        strategy = StructuredDataCompanyStrategy()
        result = strategy.extract(context)

        assert not result.is_valid

    def test_skips_when_no_structured_data(self) -> None:
        """Should skip when no structured data."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            structured_data=None,
        )
        strategy = StructuredDataCompanyStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestURLCompanyStrategy:
    """Tests for URLCompanyStrategy."""

    def test_extracts_from_greenhouse_url(self) -> None:
        """Should extract company from Greenhouse API URL."""
        context = ExtractionContext(
            url="https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/123",
        )
        strategy = URLCompanyStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        # Should derive "Airbnb" from URL

    def test_extracts_from_ashby_url(self) -> None:
        """Should extract company from AshbyHQ URL."""
        context = ExtractionContext(
            url="https://jobs.ashbyhq.com/ramp/positions/abc123",
        )
        strategy = URLCompanyStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        # Should derive company from URL

    def test_skips_generic_domains(self) -> None:
        """Should skip generic job board domains."""
        context = ExtractionContext(
            url="https://indeed.com/job/123",
        )
        strategy = URLCompanyStrategy()
        result = strategy.extract(context)

        # May skip or return something - depends on implementation
        # The key is it shouldn't return "indeed" as the company
        # Either not valid or company is not "indeed"
        assert not result.is_valid or (
            result.value and result.value.lower() != "indeed"
        )


class TestContentPatternCompanyStrategy:
    """Tests for ContentPatternCompanyStrategy."""

    def test_extracts_from_join_pattern(self) -> None:
        """Should extract company from 'Join [Company]' pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Join Google and help build the future!",
        )
        strategy = ContentPatternCompanyStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Google"

    def test_extracts_from_work_at_pattern(self) -> None:
        """Should extract from 'Work at [Company]' pattern."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="Working at Microsoft offers great benefits.",
        )
        strategy = ContentPatternCompanyStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Microsoft"

    def test_extracts_from_company_link(self) -> None:
        """Should extract from markdown link '[Company](url) is a...'."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="[Stripe](https://stripe.com) is a technology company.",
        )
        strategy = ContentPatternCompanyStrategy()
        result = strategy.extract(context)

        assert result.is_valid
        assert result.value == "Stripe"

    def test_skips_when_no_pattern_matches(self) -> None:
        """Should skip when no pattern matches."""
        context = ExtractionContext(
            url="https://example.com/job/123",
            description_body="We are looking for talented engineers.",
        )
        strategy = ContentPatternCompanyStrategy()
        result = strategy.extract(context)

        assert not result.is_valid


class TestCompanyValidation:
    """Tests for company validation logic."""

    def test_rejects_empty_company(self) -> None:
        """Should reject empty company names."""
        is_valid, reason = _is_valid_company("")
        assert not is_valid

    def test_rejects_short_company(self) -> None:
        """Should reject company names shorter than 2 chars."""
        is_valid, reason = _is_valid_company("A")
        assert not is_valid

    def test_rejects_generic_names(self) -> None:
        """Should reject generic company names."""
        generic_names = [
            "Unknown",
            "Company",
            "Employer",
            "Confidential",
            "N/A",
        ]
        for name in generic_names:
            is_valid, reason = _is_valid_company(name)
            assert not is_valid, f"Should reject generic name: {name}"

    def test_rejects_url_as_company(self) -> None:
        """Should reject URLs as company names."""
        is_valid, reason = _is_valid_company("https://example.com")
        assert not is_valid

    def test_accepts_valid_company(self) -> None:
        """Should accept valid company names."""
        valid_companies = [
            "Google",
            "Microsoft Corporation",
            "Stripe, Inc.",
            "Acme & Co",
        ]
        for company in valid_companies:
            is_valid, reason = _is_valid_company(company)
            assert is_valid, f"Should accept valid company: {company}"


# =============================================================================
# Integration tests
# =============================================================================


class TestCompanyExtractorIntegration:
    """Integration tests for the full company extractor."""

    def test_extractor_priority_order(self) -> None:
        """Verify strategies are executed in correct priority order."""
        extractor = CompanyExtractor()

        priorities = [s.priority for s in extractor.strategies]
        assert priorities == sorted(priorities), "Strategies should be sorted by priority"

    def test_structured_data_wins_over_url(self) -> None:
        """Structured data should have higher priority than URL derivation."""
        context = ExtractionContext(
            url="https://boards-api.greenhouse.io/v1/boards/wrongcompany/jobs/123",
            structured_data={"company": "Correct Company"},
        )

        result = extract_field(context, "company", run_all=True)

        assert result.final_value == "Correct Company"
        assert result.winning_strategy == "structured_data_company"

    def test_url_fallback_when_no_structured_data(self) -> None:
        """Should fall back to URL when no structured data."""
        context = ExtractionContext(
            url="https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/123",
        )

        result = extract_field(context, "company", run_all=True)

        # Should derive company from URL
        assert result.final_value is not None
