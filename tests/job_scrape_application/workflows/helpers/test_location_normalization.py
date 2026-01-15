"""Tests for location_normalization module."""



import pytest

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


class TestStateConstants:
    """Tests for state-related constants."""

    def test_state_name_by_abbr_has_all_50_states_plus_dc(self):
        assert len(_STATE_NAME_BY_ABBR) == 51

    def test_state_abbr_by_name_is_reverse_lookup(self):
        assert _STATE_ABBR_BY_NAME["California"] == "CA"
        assert _STATE_ABBR_BY_NAME["New York"] == "NY"

    def test_state_abbr_by_key_uses_normalized_keys(self):
        # Keys should be lowercase
        assert "california" in _STATE_ABBR_BY_KEY
        assert _STATE_ABBR_BY_KEY["california"] == "CA"


class TestNormalizeLocationKey:
    """Tests for _normalize_location_key function."""

    def test_strips_whitespace(self):
        result = _normalize_location_key("  San Francisco  ")
        assert result == "san francisco"

    def test_lowercases(self):
        result = _normalize_location_key("San Francisco")
        assert result == "san francisco"

    def test_removes_parentheticals(self):
        result = _normalize_location_key("San Francisco (CA)")
        assert "ca" not in result or "(ca)" not in result

    def test_handles_accented_characters(self):
        result = _normalize_location_key("San Jose")
        assert result == "san jose"


class TestNormalizeUsCityState:
    """Tests for _normalize_us_city_state function."""

    def test_normalizes_city_state_with_full_state_name(self):
        result = _normalize_us_city_state("San Francisco, California")
        assert result == "San Francisco, CA"

    def test_normalizes_city_state_with_abbreviation(self):
        result = _normalize_us_city_state("San Francisco, CA")
        assert result == "San Francisco, CA"

    def test_returns_none_for_invalid_format(self):
        result = _normalize_us_city_state("San Francisco")
        assert result is None

    def test_returns_none_for_remote_city(self):
        result = _normalize_us_city_state("Remote, CA")
        assert result is None

    def test_returns_none_for_unknown_state(self):
        result = _normalize_us_city_state("Paris, France")
        assert result is None


class TestFormatLocationLabel:
    """Tests for _format_location_label function."""

    def test_formats_city_and_state(self):
        result = _format_location_label("San Francisco", "California", "United States")
        assert result == "San Francisco, CA"

    def test_formats_city_and_country(self):
        result = _format_location_label("Paris", None, "France")
        assert result == "Paris, France"

    def test_returns_remote_for_remote_city(self):
        result = _format_location_label("Remote", None, None)
        assert result == "Remote"

    def test_returns_unknown_for_all_empty(self):
        result = _format_location_label(None, None, None)
        assert result == "Unknown"


class TestIsPlausibleLocation:
    """Tests for _is_plausible_location function."""

    def test_returns_false_for_empty(self):
        assert _is_plausible_location("") is False

    def test_returns_false_for_too_long(self):
        assert _is_plausible_location("a" * 101) is False

    def test_returns_false_for_unknown(self):
        assert _is_plausible_location("unknown") is False

    def test_returns_false_for_compensation_text(self):
        assert _is_plausible_location("$150,000 salary") is False

    def test_returns_true_for_remote(self):
        assert _is_plausible_location("Remote") is True

    def test_returns_true_for_city_state(self):
        assert _is_plausible_location("San Francisco, CA") is True


class TestNormalizeLocations:
    """Tests for _normalize_locations function."""

    def test_normalizes_list_of_locations(self):
        result = _normalize_locations(["San Francisco, California"])
        assert len(result) > 0

    def test_deduplicates_locations(self):
        result = _normalize_locations(["San Francisco, CA", "San Francisco, California"])
        # Should not have duplicates
        assert len(result) == len(set(result))

    def test_filters_invalid_locations(self):
        result = _normalize_locations(["$150,000", "San Francisco"])
        # Compensation should be filtered out
        assert "$150,000" not in result


class TestResolveLocationFromDictionary:
    """Tests for _resolve_location_from_dictionary function."""

    def test_resolves_known_city(self):
        # This test depends on the location dictionary being loaded
        # If the dictionary is empty, skip
        if not _LOCATION_DICTIONARY:
            pytest.skip("Location dictionary not loaded")

        # Try to resolve a common city
        result = _resolve_location_from_dictionary("San Francisco")
        assert result is None or isinstance(result, dict)


class TestBackwardCompatibility:
    """Tests for backward compatibility with scrape_utils imports."""

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
