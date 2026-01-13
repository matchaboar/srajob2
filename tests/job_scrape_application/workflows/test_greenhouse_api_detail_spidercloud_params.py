"""Test that Greenhouse API detail URLs use correct SpiderCloud params.

This test ensures that boards-api.greenhouse.io URLs (JSON API endpoints) use
return_format=["raw"] and request="basic" instead of ["commonmark", "raw_html"].

The JSON API returns raw JSON, not HTML. Using commonmark/raw_html causes
SpiderCloud to return empty responses.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pytest

from job_scrape_application.workflows.site_handlers import GreenhouseHandler  # noqa: E402


class TestGreenhouseApiDetailSpidercloudParams:
    """Verify SpiderCloud config for Greenhouse API detail URLs."""

    @pytest.fixture
    def handler(self) -> GreenhouseHandler:
        return GreenhouseHandler()

    def test_api_detail_url_uses_raw_format(self, handler: GreenhouseHandler) -> None:
        """API detail URLs should use 'raw' format for JSON endpoints."""
        api_url = "https://boards-api.greenhouse.io/v1/boards/axon/jobs/6658491003"
        config = handler.get_spidercloud_config(api_url)

        assert config.get("return_format") == ["raw"], (
            "API detail URLs must use return_format=['raw'] to fetch raw JSON. "
            "Using ['commonmark', 'raw_html'] causes SpiderCloud to return empty responses."
        )

    def test_api_detail_url_uses_basic_request(self, handler: GreenhouseHandler) -> None:
        """API detail URLs should use 'basic' request type (no JS rendering needed)."""
        api_url = "https://boards-api.greenhouse.io/v1/boards/axon/jobs/6658491003"
        config = handler.get_spidercloud_config(api_url)

        assert config.get("request") == "basic", (
            "API detail URLs should use request='basic' since JSON endpoints "
            "don't require JavaScript rendering."
        )

    def test_api_detail_url_preserve_host_false(self, handler: GreenhouseHandler) -> None:
        """API detail URLs should have preserve_host=False."""
        api_url = "https://boards-api.greenhouse.io/v1/boards/axon/jobs/6658491003"
        config = handler.get_spidercloud_config(api_url)

        assert config.get("preserve_host") is False

    def test_marketing_detail_url_uses_commonmark(self, handler: GreenhouseHandler) -> None:
        """Marketing site detail URLs should still use commonmark/raw_html for HTML pages."""
        marketing_url = "https://boards.greenhouse.io/axon/jobs/6658491003"
        config = handler.get_spidercloud_config(marketing_url)

        # Marketing pages are HTML, so commonmark/raw_html is correct
        assert config.get("return_format") == ["commonmark", "raw_html"]
        assert config.get("preserve_host") is True

    def test_listing_api_url_uses_raw_format(self, handler: GreenhouseHandler) -> None:
        """Listing API URLs should also use 'raw' format."""
        listing_api_url = "https://api.greenhouse.io/v1/boards/axon/jobs"
        config = handler.get_spidercloud_config(listing_api_url)

        assert config.get("return_format") == ["raw"], (
            "Listing API URLs must use return_format=['raw'] like detail API URLs."
        )

    @pytest.mark.parametrize(
        "url",
        [
            "https://boards-api.greenhouse.io/v1/boards/axon/jobs/6658491003",
            "https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7472241",
            "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4607747006",
        ],
    )
    def test_various_api_detail_urls(self, handler: GreenhouseHandler, url: str) -> None:
        """Various boards-api.greenhouse.io detail URLs should all use raw format."""
        config = handler.get_spidercloud_config(url)
        assert config.get("return_format") == ["raw"]
        assert config.get("request") == "basic"
