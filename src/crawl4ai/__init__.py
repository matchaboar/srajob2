"""Lightweight runtime shim for crawl4ai when the real package isn't installed."""

from __future__ import annotations

from typing import Any


class BrowserConfig:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")


class CacheMode:
    BYPASS = "BYPASS"


class CrawlerRunConfig:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")


class JsonXPathExtractionStrategy:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.schema = kwargs.get("schema")

    @staticmethod
    def generate_schema(*args: Any, **kwargs: Any):
        return {}


class LLMConfig:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("api_token")


class AsyncWebCrawler:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.config = kwargs.get("config")

    async def __aenter__(self) -> "AsyncWebCrawler":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None

    async def arun(self, *args: Any, **kwargs: Any) -> Any:
        return kwargs.get("result")
