"""Tests for company_normalization module."""

import os
import sys

sys.path.insert(0, os.path.abspath("."))

import pytest

from job_scrape_application.workflows.helpers.company_normalization import (
    _COMPANY_SUFFIX_RE,
    _GENERIC_COMPANY_HINTS,
    _INVALID_COMPANY_TOKENS,
    _JOB_BOARD_COMPANY_TOKENS,
    apply_company_hint,
    derive_company_from_url,
    is_generic_company_name,
    normalize_company_hint,
    normalize_title_from_bar,
)


class TestConstants:
    """Tests for company normalization constants."""

    def test_company_suffix_regex_matches_inc(self):
        match = _COMPANY_SUFFIX_RE.search("Acme Inc")
        assert match is not None

    def test_company_suffix_regex_matches_llc(self):
        match = _COMPANY_SUFFIX_RE.search("Acme LLC")
        assert match is not None

    def test_company_suffix_regex_matches_corp(self):
        match = _COMPANY_SUFFIX_RE.search("Acme Corp")
        assert match is not None

    def test_generic_hints_contains_careers(self):
        assert "careers" in _GENERIC_COMPANY_HINTS

    def test_generic_hints_contains_jobs(self):
        assert "jobs" in _GENERIC_COMPANY_HINTS

    def test_job_board_tokens_contains_greenhouse(self):
        assert "greenhouse" in _JOB_BOARD_COMPANY_TOKENS

    def test_job_board_tokens_contains_lever(self):
        assert "lever" in _JOB_BOARD_COMPANY_TOKENS

    def test_invalid_tokens_contains_language_codes(self):
        """Language codes should be in invalid tokens."""
        assert "en" in _INVALID_COMPANY_TOKENS
        assert "ko" in _INVALID_COMPANY_TOKENS
        assert "ja" in _INVALID_COMPANY_TOKENS
        assert "de" in _INVALID_COMPANY_TOKENS
        assert "fr" in _INVALID_COMPANY_TOKENS

    def test_invalid_tokens_contains_url_segments(self):
        """URL segments that aren't company names should be in invalid tokens."""
        assert "api" in _INVALID_COMPANY_TOKENS
        assert "land" in _INVALID_COMPANY_TOKENS
        assert "www" in _INVALID_COMPANY_TOKENS


class TestNormalizeCompanyHint:
    """Tests for normalize_company_hint function."""

    def test_valid_company_name(self):
        result = normalize_company_hint("Acme Corporation")
        assert result == "Acme"

    def test_strips_whitespace(self):
        result = normalize_company_hint("  Acme  ")
        assert result == "Acme"

    def test_removes_inc_suffix(self):
        result = normalize_company_hint("Acme Inc")
        assert result == "Acme"

    def test_removes_llc_suffix(self):
        result = normalize_company_hint("Acme LLC")
        assert result == "Acme"

    def test_returns_none_for_generic(self):
        result = normalize_company_hint("careers")
        assert result is None

    def test_returns_none_for_empty(self):
        result = normalize_company_hint("")
        assert result is None

    def test_returns_none_for_none(self):
        result = normalize_company_hint(None)
        assert result is None

    def test_extracts_company_from_at_pattern(self):
        result = normalize_company_hint("Software Engineer at Acme")
        assert result == "Acme"

    def test_removes_markdown_emphasis(self):
        result = normalize_company_hint("**Acme**")
        assert result == "Acme"

    def test_removes_brackets(self):
        result = normalize_company_hint("[Acme]")
        assert result == "Acme"


class TestNormalizeTitleFromBar:
    """Tests for normalize_title_from_bar function."""

    def test_extracts_title_from_pipe_separator(self):
        result = normalize_title_from_bar("Software Engineer | Acme")
        assert "Software Engineer" in result

    def test_extracts_title_from_dash_separator(self):
        result = normalize_title_from_bar("Software Engineer - Acme")
        assert "Software Engineer" in result

    def test_returns_original_if_no_separator(self):
        result = normalize_title_from_bar("Software Engineer")
        assert result == "Software Engineer"

    def test_normalizes_whitespace(self):
        result = normalize_title_from_bar("Software   Engineer")
        assert result == "Software Engineer"

    def test_handles_empty_string(self):
        result = normalize_title_from_bar("")
        assert result == ""

    def test_decodes_html_entities(self):
        result = normalize_title_from_bar("Software &amp; Engineer")
        assert "&" in result


class TestIsGenericCompanyName:
    """Tests for is_generic_company_name function."""

    def test_returns_true_for_none(self):
        assert is_generic_company_name(None) is True

    def test_returns_true_for_empty(self):
        assert is_generic_company_name("") is True

    def test_returns_true_for_unknown(self):
        assert is_generic_company_name("Unknown") is True

    def test_returns_true_for_greenhouse(self):
        assert is_generic_company_name("Greenhouse") is True

    def test_returns_true_for_lever(self):
        assert is_generic_company_name("Lever") is True

    def test_returns_false_for_real_company(self):
        assert is_generic_company_name("Acme") is False

    def test_returns_false_for_google(self):
        assert is_generic_company_name("Google") is False

    def test_returns_true_for_language_codes(self):
        """Language codes like 'En', 'Ko' should be rejected as company names.

        This fixes the bug where Greenhouse API URLs like /en/land/jobsnotice
        resulted in company='En' being extracted.
        """
        assert is_generic_company_name("En") is True
        assert is_generic_company_name("Ko") is True
        assert is_generic_company_name("en") is True
        assert is_generic_company_name("ko") is True
        assert is_generic_company_name("Ja") is True
        assert is_generic_company_name("De") is True

    def test_returns_true_for_url_segments(self):
        """URL segments that aren't company names should be rejected."""
        assert is_generic_company_name("Api") is True
        assert is_generic_company_name("Land") is True
        assert is_generic_company_name("Www") is True


class TestApplyCompanyHint:
    """Tests for apply_company_hint function."""

    def test_uses_hint_when_company_is_generic(self):
        result = apply_company_hint("Greenhouse", {"company": "Acme"})
        assert result == "Acme"

    def test_keeps_company_when_hint_is_none(self):
        result = apply_company_hint("Acme", {})
        assert result == "Acme"

    def test_keeps_company_when_hint_is_empty(self):
        result = apply_company_hint("Acme", {"company": ""})
        assert result == "Acme"

    def test_uses_better_formatted_hint(self):
        # Same name but hint has better formatting
        result = apply_company_hint("acme", {"company": "Acme"})
        assert result == "Acme"

    def test_keeps_company_when_different_from_hint(self):
        result = apply_company_hint("Acme", {"company": "Different"})
        assert result == "Acme"


class TestDeriveCompanyFromUrl:
    """Tests for derive_company_from_url function."""

    def test_extracts_from_greenhouse_url(self):
        url = "https://boards.greenhouse.io/acme/jobs/12345"
        result = derive_company_from_url(url)
        assert result == "Acme"

    def test_extracts_from_workday_url(self):
        url = "https://acme.myworkdayjobs.com/careers"
        result = derive_company_from_url(url)
        assert result == "Acme"

    def test_extracts_from_avature_url(self):
        url = "https://acme.avature.net/careers"
        result = derive_company_from_url(url)
        assert result == "Acme"

    def test_extracts_from_careers_subdomain(self):
        url = "https://careers.acme.com/jobs"
        result = derive_company_from_url(url)
        assert result == "Acme"

    def test_extracts_from_jobs_subdomain(self):
        url = "https://jobs.acme.com/listings"
        result = derive_company_from_url(url)
        assert result == "Acme"

    def test_returns_empty_for_invalid_url(self):
        result = derive_company_from_url("not a url")
        assert result == ""

    def test_extracts_from_simple_domain(self):
        url = "https://www.acme.com/jobs"
        result = derive_company_from_url(url)
        assert result == "Acme"

    def test_returns_empty_for_greenhouse_api_language_code(self):
        """Greenhouse API URLs with language codes should not extract company.

        This fixes the bug where URLs like /en/land/jobsnotice resulted in
        company='En' being extracted.
        """
        url = "https://api.greenhouse.io/en/land/jobsnotice"
        result = derive_company_from_url(url)
        assert result == ""

    def test_returns_empty_for_greenhouse_ko_language_code(self):
        """Korean language code should not be extracted as company."""
        url = "https://api.greenhouse.io/ko/land/jobs"
        result = derive_company_from_url(url)
        assert result == ""

    def test_extracts_valid_company_from_greenhouse(self):
        """Valid company slugs should still be extracted from Greenhouse URLs."""
        url = "https://boards.greenhouse.io/robinhood/jobs/7155938"
        result = derive_company_from_url(url)
        assert result == "Robinhood"


class TestBackwardCompatibility:
    """Tests for backward compatibility with scrape_utils imports."""

    def test_imports_from_scrape_utils(self):
        from job_scrape_application.workflows.helpers.scrape_utils import (
            _COMPANY_SUFFIX_RE as SU_SUFFIX_RE,
            _GENERIC_COMPANY_HINTS as SU_GENERIC_HINTS,
            _JOB_BOARD_COMPANY_TOKENS as SU_BOARD_TOKENS,
            apply_company_hint as su_apply,
            derive_company_from_url as su_derive,
            is_generic_company_name as su_is_generic,
            normalize_company_hint as su_normalize_hint,
            normalize_title_from_bar as su_normalize_title,
        )

        # Verify they're the same objects
        assert SU_SUFFIX_RE is _COMPANY_SUFFIX_RE
        assert SU_GENERIC_HINTS is _GENERIC_COMPANY_HINTS
        assert SU_BOARD_TOKENS is _JOB_BOARD_COMPANY_TOKENS
        assert su_apply is apply_company_hint
        assert su_derive is derive_company_from_url
        assert su_is_generic is is_generic_company_name
        assert su_normalize_hint is normalize_company_hint
        assert su_normalize_title is normalize_title_from_bar
