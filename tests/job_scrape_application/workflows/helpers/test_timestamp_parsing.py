"""Tests for timestamp_parsing module."""

import os
import sys
import time

sys.path.insert(0, os.path.abspath("."))

import pytest

from job_scrape_application.workflows.helpers.timestamp_parsing import (
    _RELATIVE_POSTED_MIN_DAYS,
    _RELATIVE_TIME_RE,
    _parse_relative_posted_at,
    parse_posted_at,
    parse_posted_at_with_unknown,
)


class TestConstants:
    """Tests for timestamp parsing constants."""

    def test_relative_time_regex_matches_days_ago(self):
        match = _RELATIVE_TIME_RE.search("Posted 3 days ago")
        assert match is not None
        assert match.group("value") == "3"
        assert match.group("unit") == "days"

    def test_relative_time_regex_matches_hours(self):
        match = _RELATIVE_TIME_RE.search("5 hours ago")
        assert match is not None
        assert match.group("value") == "5"
        assert match.group("unit") == "hours"

    def test_relative_time_regex_matches_weeks(self):
        match = _RELATIVE_TIME_RE.search("2 weeks ago")
        assert match is not None
        assert match.group("value") == "2"
        assert match.group("unit") == "weeks"

    def test_relative_posted_min_days_is_30(self):
        assert _RELATIVE_POSTED_MIN_DAYS == 30


class TestParseRelativePostedAt:
    """Tests for _parse_relative_posted_at function."""

    def test_today_returns_now(self):
        now_ms = 1700000000000
        result = _parse_relative_posted_at("Posted today", now_ms)
        assert result == now_ms

    def test_yesterday_returns_one_day_ago(self):
        now_ms = 1700000000000
        result = _parse_relative_posted_at("Yesterday", now_ms)
        assert result == now_ms - 86_400_000

    def test_days_ago_with_posted_keyword(self):
        now_ms = 1700000000000
        result = _parse_relative_posted_at("Posted 5 days ago", now_ms)
        assert result is not None
        # 5 days = 5 * 86400 * 1000 ms
        expected = now_ms - (5 * 86_400_000)
        assert result == expected

    def test_days_ago_without_posted_keyword_under_min(self):
        # Without "posted" keyword, days under _RELATIVE_POSTED_MIN_DAYS should return None
        now_ms = 1700000000000
        result = _parse_relative_posted_at("5 days ago", now_ms)
        assert result is None

    def test_hours_ago(self):
        now_ms = 1700000000000
        result = _parse_relative_posted_at("3 hours ago", now_ms)
        assert result is not None
        expected = now_ms - (3 * 3_600_000)
        assert result == expected

    def test_no_ago_returns_none(self):
        now_ms = 1700000000000
        result = _parse_relative_posted_at("3 days", now_ms)
        assert result is None

    def test_invalid_string_returns_none(self):
        now_ms = 1700000000000
        result = _parse_relative_posted_at("not a time", now_ms)
        assert result is None


class TestParsePostedAt:
    """Tests for parse_posted_at function."""

    def test_none_returns_current_time(self):
        result = parse_posted_at(None)
        assert isinstance(result, int)
        assert result > 0

    def test_millisecond_timestamp_returned_as_is(self):
        ts_ms = 1700000000000
        result = parse_posted_at(ts_ms)
        assert result == ts_ms

    def test_second_timestamp_converted_to_ms(self):
        ts_sec = 1700000000
        result = parse_posted_at(ts_sec)
        assert result == ts_sec * 1000

    def test_iso_string_parsed(self):
        iso = "2023-11-14T10:30:00Z"
        result = parse_posted_at(iso)
        assert isinstance(result, int)
        assert result > 0

    def test_iso_string_with_timezone(self):
        iso = "2023-11-14T10:30:00+05:00"
        result = parse_posted_at(iso)
        assert isinstance(result, int)
        assert result > 0

    def test_relative_string(self):
        now_ms = 1700000000000
        result = parse_posted_at("Posted 30 days ago", now_ms)
        expected = now_ms - (30 * 86_400_000)
        assert result == expected

    def test_invalid_string_returns_now(self):
        now_ms = 1700000000000
        result = parse_posted_at("invalid", now_ms)
        assert result == now_ms


class TestParsePostedAtWithUnknown:
    """Tests for parse_posted_at_with_unknown function."""

    def test_none_returns_now_and_unknown_true(self):
        now_ms = 1700000000000
        result, is_unknown = parse_posted_at_with_unknown(None, now_ms)
        assert result == now_ms
        assert is_unknown is True

    def test_valid_timestamp_returns_false(self):
        ts_ms = 1700000000000
        result, is_unknown = parse_posted_at_with_unknown(ts_ms)
        assert result == ts_ms
        assert is_unknown is False

    def test_valid_iso_returns_false(self):
        iso = "2023-11-14T10:30:00Z"
        result, is_unknown = parse_posted_at_with_unknown(iso)
        assert isinstance(result, int)
        assert is_unknown is False

    def test_invalid_string_returns_true(self):
        now_ms = 1700000000000
        result, is_unknown = parse_posted_at_with_unknown("invalid", now_ms)
        assert result == now_ms
        assert is_unknown is True

    def test_max_age_days_filters_old_dates(self):
        now_ms = 1700000000000
        # A date 60 days ago
        old_date = "Posted 60 days ago"
        result, is_unknown = parse_posted_at_with_unknown(old_date, now_ms, max_age_days=30)
        # Should return now_ms and is_unknown=True because it's older than 30 days
        assert result == now_ms
        assert is_unknown is True

    def test_allows_zero_max_age_days(self):
        now_ms = 1700000000000
        result, is_unknown = parse_posted_at_with_unknown(now_ms, max_age_days=0)
        assert is_unknown is False


class TestBackwardCompatibility:
    """Tests for backward compatibility with scrape_utils imports."""

    def test_imports_from_scrape_utils(self):
        from job_scrape_application.workflows.helpers.scrape_utils import (
            _RELATIVE_POSTED_MIN_DAYS as SU_MIN_DAYS,
            _RELATIVE_TIME_RE as SU_REGEX,
            _parse_relative_posted_at as su_parse_relative,
            parse_posted_at as su_parse,
            parse_posted_at_with_unknown as su_parse_unknown,
        )

        # Verify they're the same objects
        assert SU_MIN_DAYS == _RELATIVE_POSTED_MIN_DAYS
        assert SU_REGEX is _RELATIVE_TIME_RE
        assert su_parse_relative is _parse_relative_posted_at
        assert su_parse is parse_posted_at
        assert su_parse_unknown is parse_posted_at_with_unknown
