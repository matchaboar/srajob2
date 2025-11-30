from __future__ import annotations

from typing import Any, Mapping

__all__ = [
    "AsyncWebCrawler",
    "BrowserConfig",
    "CacheMode",
    "CrawlerRunConfig",
    "JsonXPathExtractionStrategy",
    "LLMConfig",
]


class BrowserConfig:
    headless: bool | None
    browser_type: str | None
    verbose: bool | None
    browser_mode: str | None
    user_agent_mode: str | None

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...


class CacheMode:
    BYPASS: str


class CrawlerRunConfig:
    cache_mode: Any
    exclude_external_links: bool | None
    word_count_threshold: int | None
    extraction_strategy: Any
    scan_full_page: bool | None
    remove_overlay_elements: bool | None
    magic: bool | None
    simulate_user: bool | None
    session_id: str | None
    wait_for: str | None
    js_code: str | None
    js_only: bool | None
    delay_before_return_html: int | None

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...


class JsonXPathExtractionStrategy:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    @staticmethod
    def generate_schema(
        html_document: str,
        schema_type: str = ...,
        llm_config: Any = ...,
        query: str | None = ...,
        target_json_example: Any = ...,
    ) -> Mapping[str, Any]: ...


class LLMConfig:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...


class AsyncWebCrawler:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    async def __aenter__(self) -> "AsyncWebCrawler": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...
    async def arun(self, *args: Any, **kwargs: Any) -> Any: ...
