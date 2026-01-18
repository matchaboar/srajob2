"""Tests for wait_for config timeout synchronization.

This module tests the extract_wait_for_timeout_seconds helper which extracts
timeout values from SpiderCloud wait_for configurations. This is used to
synchronize Python-side asyncio timeouts with SpiderCloud's JavaScript
wait_for timeouts.
"""




from job_scrape_application.workflows.site_handlers.base import BaseSiteHandler  # noqa: E402
from job_scrape_application.workflows.site_handlers.workday import WorkdayHandler  # noqa: E402


class TestExtractWaitForTimeoutSeconds:
    """Tests for BaseSiteHandler.extract_wait_for_timeout_seconds."""

    def test_no_wait_for_returns_zero(self):
        """Config without wait_for should return 0."""
        config = {"return_format": ["commonmark"]}
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 0

    def test_empty_config_returns_zero(self):
        """Empty config should return 0."""
        assert BaseSiteHandler.extract_wait_for_timeout_seconds({}) == 0

    def test_empty_wait_for_returns_zero(self):
        """Empty wait_for dict should return 0."""
        assert BaseSiteHandler.extract_wait_for_timeout_seconds({"wait_for": {}}) == 0

    def test_invalid_wait_for_type_returns_zero(self):
        """Non-dict wait_for should return 0."""
        assert BaseSiteHandler.extract_wait_for_timeout_seconds({"wait_for": "bad"}) == 0
        assert BaseSiteHandler.extract_wait_for_timeout_seconds({"wait_for": 123}) == 0
        assert BaseSiteHandler.extract_wait_for_timeout_seconds({"wait_for": None}) == 0

    def test_selector_only_timeout(self):
        """Config with only selector timeout should return selector + buffer."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"secs": 40, "nanos": 0}}
            }
        }
        # 40s selector + 15s buffer = 55s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 55

    def test_selector_plus_idle_network(self):
        """Config with selector and idle_network should add both + buffer."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"secs": 40, "nanos": 0}},
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
        }
        # 40s selector + 5s idle + 15s buffer = 60s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 60

    def test_multiple_idle_networks(self):
        """Multiple idle_network entries should all be added."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"secs": 30, "nanos": 0}},
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
                "idle_network1": {"timeout": {"secs": 3, "nanos": 0}},
            }
        }
        # 30s selector + 5s idle0 + 3s idle1 + 15s buffer = 53s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 53

    def test_idle_network_only(self):
        """Config with only idle_network should still calculate timeout."""
        config = {
            "wait_for": {
                "idle_network0": {"timeout": {"secs": 10, "nanos": 0}},
            }
        }
        # 10s idle + 15s buffer = 25s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 25

    def test_custom_buffer_seconds(self):
        """Custom buffer_seconds should be respected."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"secs": 20, "nanos": 0}}
            }
        }
        # 20s selector + 30s custom buffer = 50s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config, buffer_seconds=30) == 50

    def test_zero_buffer_seconds(self):
        """Zero buffer should return just the timeout values."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"secs": 25, "nanos": 0}},
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
        }
        # 25s + 5s + 0s buffer = 30s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config, buffer_seconds=0) == 30

    def test_missing_timeout_in_selector(self):
        """Selector without timeout should be handled gracefully."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job"}
            }
        }
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 0

    def test_missing_secs_in_timeout(self):
        """Timeout without secs should be handled gracefully."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"nanos": 500000000}}
            }
        }
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 0

    def test_invalid_selector_type(self):
        """Non-dict selector should be handled gracefully."""
        config = {
            "wait_for": {
                "selector": "bad_selector_type"
            }
        }
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 0

    def test_invalid_idle_network_type(self):
        """Non-dict idle_network should be handled gracefully."""
        config = {
            "wait_for": {
                "idle_network0": "bad_type",
                "selector": {"selector": "div.job", "timeout": {"secs": 20, "nanos": 0}},
            }
        }
        # Only selector timeout is counted: 20s + 15s buffer = 35s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 35

    def test_float_seconds_value(self):
        """Float seconds values should be converted to int."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"secs": 30.5, "nanos": 0}}
            }
        }
        # 30s (truncated) + 15s buffer = 45s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 45

    def test_non_idle_network_keys_ignored(self):
        """Keys that don't start with idle_network should not be added."""
        config = {
            "wait_for": {
                "selector": {"selector": "div.job", "timeout": {"secs": 20, "nanos": 0}},
                "other_key": {"timeout": {"secs": 100, "nanos": 0}},
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
        }
        # 20s selector + 5s idle (other_key ignored) + 15s buffer = 40s
        assert BaseSiteHandler.extract_wait_for_timeout_seconds(config) == 40


class TestWorkdayHandlerTimeout:
    """Integration tests using real Workday handler config."""

    def test_workday_config_timeout(self):
        """Workday handler config should yield expected timeout."""
        handler = WorkdayHandler()
        config = handler.get_spidercloud_config("https://company.wd5.myworkdayjobs.com/jobs")
        timeout = BaseSiteHandler.extract_wait_for_timeout_seconds(config)
        # Workday: 90s selector + 5s idle + 15s buffer = 110s
        assert timeout == 110

    def test_workday_normalized_config_timeout(self):
        """Normalized Workday config should also yield expected timeout."""
        handler = WorkdayHandler()
        raw_config = handler.get_spidercloud_config("https://company.wd5.myworkdayjobs.com/jobs")
        normalized_config = handler.normalize_spidercloud_config(raw_config)
        timeout = BaseSiteHandler.extract_wait_for_timeout_seconds(normalized_config)
        # Should be same as raw: 110s
        assert timeout == 110
