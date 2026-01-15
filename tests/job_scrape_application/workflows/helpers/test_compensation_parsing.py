"""Tests for compensation_parsing module."""




from job_scrape_application.workflows.helpers.compensation_parsing import (
    DEFAULT_TOTAL_COMPENSATION,
    HOURLY_TO_ANNUAL_MULTIPLIER,
    MAX_TOTAL_COMPENSATION,
    MIN_TOTAL_COMPENSATION,
    UNKNOWN_COMPENSATION_REASON,
    normalize_compensation_value,
    parse_compensation,
)


class TestConstants:
    """Tests for compensation constants."""

    def test_default_total_compensation_is_zero(self):
        assert DEFAULT_TOTAL_COMPENSATION == 0

    def test_min_total_compensation_is_30k(self):
        assert MIN_TOTAL_COMPENSATION == 30_000

    def test_max_total_compensation_is_5m(self):
        assert MAX_TOTAL_COMPENSATION == 5_000_000

    def test_hourly_to_annual_multiplier_is_2080(self):
        assert HOURLY_TO_ANNUAL_MULTIPLIER == 2080

    def test_unknown_compensation_reason_is_string(self):
        assert isinstance(UNKNOWN_COMPENSATION_REASON, str)
        assert len(UNKNOWN_COMPENSATION_REASON) > 0


class TestNormalizeCompensationValue:
    """Tests for normalize_compensation_value function."""

    def test_valid_compensation_returns_int(self):
        assert normalize_compensation_value(150_000) == 150_000

    def test_float_compensation_returns_int(self):
        assert normalize_compensation_value(150_000.5) == 150_000

    def test_below_min_returns_none(self):
        assert normalize_compensation_value(20_000) is None

    def test_at_min_returns_none(self):
        assert normalize_compensation_value(MIN_TOTAL_COMPENSATION) is None

    def test_above_max_returns_none(self):
        assert normalize_compensation_value(6_000_000) is None

    def test_at_max_returns_none(self):
        assert normalize_compensation_value(MAX_TOTAL_COMPENSATION) is None

    def test_string_returns_none(self):
        assert normalize_compensation_value("150000") is None

    def test_none_returns_none(self):
        assert normalize_compensation_value(None) is None

    def test_zero_returns_none(self):
        assert normalize_compensation_value(0) is None

    def test_negative_returns_none(self):
        assert normalize_compensation_value(-50_000) is None


class TestParseCompensation:
    """Tests for parse_compensation function."""

    def test_int_value(self):
        assert parse_compensation(150_000) == 150_000

    def test_float_value(self):
        assert parse_compensation(150_000.0) == 150_000

    def test_string_with_dollar_sign(self):
        assert parse_compensation("$150,000") == 150_000

    def test_string_with_range(self):
        # Should return the max value in the range
        result = parse_compensation("$100,000 - $150,000")
        assert result == 150_000

    def test_string_with_spaces_not_supported(self):
        # European number format with spaces is not currently supported
        result = parse_compensation("150 000")
        assert result == 0  # Returns 0 since space-separated numbers aren't parsed

    def test_invalid_string_returns_zero(self):
        assert parse_compensation("not a number") == 0

    def test_zero_value_returns_zero(self):
        assert parse_compensation(0) == 0

    def test_negative_value_returns_zero(self):
        assert parse_compensation(-50_000) == 0

    def test_below_min_returns_zero(self):
        assert parse_compensation(20_000) == 0

    def test_above_max_returns_zero(self):
        assert parse_compensation(6_000_000) == 0

    def test_with_meta_returns_tuple(self):
        result = parse_compensation(150_000, with_meta=True)
        assert result == (150_000, False)

    def test_with_meta_invalid_returns_tuple_with_true(self):
        result = parse_compensation("not a number", with_meta=True)
        assert result == (0, True)

    def test_401k_only_returns_zero(self):
        # 401k contribution amounts should be ignored
        assert parse_compensation("401(k) with 4% match") == 0

    def test_salary_with_401k_mention_uses_salary(self):
        result = parse_compensation("$150,000 base salary + 401k")
        assert result == 150_000
