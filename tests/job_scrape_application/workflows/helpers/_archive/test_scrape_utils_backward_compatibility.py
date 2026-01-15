"""Backward compatibility tests for scrape_utils re-exports.

These tests verify that imports from scrape_utils.py continue to work
after the module was split into separate helper files. The scrape_utils
module re-exports symbols from the individual helper modules.

Archived: 2026-01-15
Reason: These tests primarily verify import aliasing works, which is
        low-value after the refactor has stabilized. The re-exports
        have been removed from scrape_utils.py, so these tests document
        the historical backward compatibility layer that is no longer
        maintained.
"""

import pytest

pytestmark = pytest.mark.skip(
    reason="Archived: scrape_utils re-exports have been removed"
)


class TestUrlHandlingBackwardCompatibility:
    """Tests for backward compatibility with url_handling imports."""

    def test_imports_from_scrape_utils(self):
        from job_scrape_application.workflows.helpers.scrape_utils import (
            _apply_url_candidates as su_candidates,
            _first_url as su_first_url,
            _score_apply_url as su_score,
            _strip_ashby_application_url as su_strip,
            prefer_apply_url as su_prefer,
        )
        from job_scrape_application.workflows.helpers.url_handling import (
            _apply_url_candidates,
            _first_url,
            _score_apply_url,
            _strip_ashby_application_url,
            prefer_apply_url,
        )

        # Verify they're the same objects
        assert su_score is _score_apply_url
        assert su_strip is _strip_ashby_application_url
        assert su_candidates is _apply_url_candidates
        assert su_prefer is prefer_apply_url
        assert su_first_url is _first_url


class TestLocationNormalizationBackwardCompatibility:
    """Tests for backward compatibility with location_normalization imports."""

    def test_imports_from_scrape_utils(self):
        from job_scrape_application.workflows.helpers.scrape_utils import (
            _CITY_KEYWORD_KEYS as SU_CITY_KEYS,
            _CITY_KEYWORDS as SU_CITY_KW,
            _COUNTRY_KEY_TO_LABEL as SU_COUNTRY,
            _LOCATION_DICTIONARY as SU_LOC_DICT,
            _LOCATION_DICTIONARY_KEYS as SU_LOC_DICT_KEYS,
            _STATE_ABBR_BY_KEY as SU_STATE_BY_KEY,
            _STATE_ABBR_BY_NAME as SU_STATE_BY_NAME,
            _STATE_NAME_BY_ABBR as SU_STATE_NAME,
            _find_city_in_text as su_find_city,
            _format_location_label as su_format,
            _is_plausible_location as su_is_plausible,
            _normalize_country_label as su_normalize_country,
            _normalize_location_key as su_normalize_key,
            _normalize_locations as su_normalize_locs,
            _normalize_us_city_state as su_normalize_us,
            _reorder_by_us_preference as su_reorder,
            _resolve_location_from_dictionary as su_resolve,
        )
        from job_scrape_application.workflows.helpers.location_normalization import (
            _CITY_KEYWORD_KEYS,
            _CITY_KEYWORDS,
            _COUNTRY_KEY_TO_LABEL,
            _LOCATION_DICTIONARY,
            _LOCATION_DICTIONARY_KEYS,
            _STATE_ABBR_BY_KEY,
            _STATE_ABBR_BY_NAME,
            _STATE_NAME_BY_ABBR,
            _find_city_in_text,
            _format_location_label,
            _is_plausible_location,
            _normalize_country_label,
            _normalize_location_key,
            _normalize_locations,
            _normalize_us_city_state,
            _reorder_by_us_preference,
            _resolve_location_from_dictionary,
        )

        # Verify they're the same objects
        assert SU_STATE_NAME is _STATE_NAME_BY_ABBR
        assert SU_STATE_BY_NAME is _STATE_ABBR_BY_NAME
        assert SU_STATE_BY_KEY is _STATE_ABBR_BY_KEY
        assert SU_LOC_DICT is _LOCATION_DICTIONARY
        assert SU_LOC_DICT_KEYS is _LOCATION_DICTIONARY_KEYS
        assert SU_CITY_KW is _CITY_KEYWORDS
        assert SU_CITY_KEYS is _CITY_KEYWORD_KEYS
        assert SU_COUNTRY is _COUNTRY_KEY_TO_LABEL
        assert su_normalize_key is _normalize_location_key
        assert su_normalize_us is _normalize_us_city_state
        assert su_format is _format_location_label
        assert su_resolve is _resolve_location_from_dictionary
        assert su_find_city is _find_city_in_text
        assert su_normalize_country is _normalize_country_label
        assert su_is_plausible is _is_plausible_location
        assert su_reorder is _reorder_by_us_preference
        assert su_normalize_locs is _normalize_locations


class TestCompensationParsingBackwardCompatibility:
    """Tests for backward compatibility with compensation_parsing imports."""

    def test_imports_from_scrape_utils(self):
        from job_scrape_application.workflows.helpers.scrape_utils import (
            DEFAULT_TOTAL_COMPENSATION as SU_DEFAULT,
            HOURLY_TO_ANNUAL_MULTIPLIER as SU_HOURLY,
            MAX_TOTAL_COMPENSATION as SU_MAX,
            MIN_TOTAL_COMPENSATION as SU_MIN,
            UNKNOWN_COMPENSATION_REASON as SU_REASON,
            normalize_compensation_value as su_normalize,
            parse_compensation as su_parse,
        )
        from job_scrape_application.workflows.helpers.compensation_parsing import (
            DEFAULT_TOTAL_COMPENSATION,
            HOURLY_TO_ANNUAL_MULTIPLIER,
            MAX_TOTAL_COMPENSATION,
            MIN_TOTAL_COMPENSATION,
            UNKNOWN_COMPENSATION_REASON,
            normalize_compensation_value,
            parse_compensation,
        )

        # Verify they're the same objects
        assert SU_DEFAULT == DEFAULT_TOTAL_COMPENSATION
        assert SU_MIN == MIN_TOTAL_COMPENSATION
        assert SU_MAX == MAX_TOTAL_COMPENSATION
        assert SU_HOURLY == HOURLY_TO_ANNUAL_MULTIPLIER
        assert SU_REASON == UNKNOWN_COMPENSATION_REASON
        assert su_normalize is normalize_compensation_value
        assert su_parse is parse_compensation


class TestCompanyNormalizationBackwardCompatibility:
    """Tests for backward compatibility with company_normalization imports."""

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
        from job_scrape_application.workflows.helpers.company_normalization import (
            _COMPANY_SUFFIX_RE,
            _GENERIC_COMPANY_HINTS,
            _JOB_BOARD_COMPANY_TOKENS,
            apply_company_hint,
            derive_company_from_url,
            is_generic_company_name,
            normalize_company_hint,
            normalize_title_from_bar,
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


class TestPageDetectionBackwardCompatibility:
    """Tests for backward compatibility with page_detection imports."""

    def test_imports_from_scrape_utils(self):
        from job_scrape_application.workflows.helpers.scrape_utils import (
            _ERROR_LANDING_PHRASES as SU_ERROR,
            _JOB_DETAIL_MARKERS as SU_DETAIL,
            _LISTING_CARD_APPLY_MARKERS as SU_APPLY,
            _LISTING_CARD_POSTED_RE as SU_POSTED_RE,
            _LISTING_FILTER_TERMS as SU_FILTER,
            _LISTING_URL_TOKENS as SU_TOKENS,
            _description_mentions_listing_url as su_mentions,
            _looks_like_listing_card_snippet as su_snippet,
            _url_is_listing_root as su_root,
            _url_suggests_listing as su_suggests,
            looks_like_error_landing as su_error,
            looks_like_job_listing_page as su_listing,
        )
        from job_scrape_application.workflows.helpers.page_detection import (
            _ERROR_LANDING_PHRASES,
            _JOB_DETAIL_MARKERS,
            _LISTING_CARD_APPLY_MARKERS,
            _LISTING_CARD_POSTED_RE,
            _LISTING_FILTER_TERMS,
            _LISTING_URL_TOKENS,
            _description_mentions_listing_url,
            _looks_like_listing_card_snippet,
            _url_is_listing_root,
            _url_suggests_listing,
            looks_like_error_landing,
            looks_like_job_listing_page,
        )

        # Verify they're the same objects
        assert SU_ERROR is _ERROR_LANDING_PHRASES
        assert SU_DETAIL is _JOB_DETAIL_MARKERS
        assert SU_APPLY is _LISTING_CARD_APPLY_MARKERS
        assert SU_POSTED_RE is _LISTING_CARD_POSTED_RE
        assert SU_FILTER is _LISTING_FILTER_TERMS
        assert SU_TOKENS is _LISTING_URL_TOKENS
        assert su_mentions is _description_mentions_listing_url
        assert su_snippet is _looks_like_listing_card_snippet
        assert su_root is _url_is_listing_root
        assert su_suggests is _url_suggests_listing
        assert su_error is looks_like_error_landing
        assert su_listing is looks_like_job_listing_page


class TestTimestampParsingBackwardCompatibility:
    """Tests for backward compatibility with timestamp_parsing imports."""

    def test_imports_from_scrape_utils(self):
        from job_scrape_application.workflows.helpers.scrape_utils import (
            _RELATIVE_POSTED_MIN_DAYS as SU_MIN_DAYS,
            _RELATIVE_TIME_RE as SU_REGEX,
            _parse_relative_posted_at as su_parse_relative,
            parse_posted_at as su_parse,
            parse_posted_at_with_unknown as su_parse_unknown,
        )
        from job_scrape_application.workflows.helpers.timestamp_parsing import (
            _RELATIVE_POSTED_MIN_DAYS,
            _RELATIVE_TIME_RE,
            _parse_relative_posted_at,
            parse_posted_at,
            parse_posted_at_with_unknown,
        )

        # Verify they're the same objects
        assert SU_MIN_DAYS == _RELATIVE_POSTED_MIN_DAYS
        assert SU_REGEX is _RELATIVE_TIME_RE
        assert su_parse_relative is _parse_relative_posted_at
        assert su_parse is parse_posted_at
        assert su_parse_unknown is parse_posted_at_with_unknown
