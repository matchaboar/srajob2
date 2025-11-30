from __future__ import annotations

from typing import Any, Mapping, Sequence

from .v2.types import PaginationConfig, ScrapeOptions

__all__ = ["Firecrawl"]


class Firecrawl:
    def __init__(
        self,
        api_key: str | None = ...,
        api_url: str = ...,
        timeout: float | None = ...,
        max_retries: int = ...,
        backoff_factor: float = ...,
    ) -> None: ...

    def start_batch_scrape(
        self,
        urls: Sequence[str],
        *,
        formats: Sequence[Any] | None = ...,
        headers: Mapping[str, str] | None = ...,
        include_tags: Sequence[str] | None = ...,
        exclude_tags: Sequence[str] | None = ...,
        only_main_content: bool | None = ...,
        timeout: int | None = ...,
        wait_for: int | None = ...,
        mobile: bool | None = ...,
        parsers: Sequence[Any] | None = ...,
        actions: Sequence[Any] | None = ...,
        location: Any | None = ...,
        skip_tls_verification: bool | None = ...,
        remove_base64_images: bool | None = ...,
        fast_mode: bool | None = ...,
        use_mock: str | None = ...,
        block_ads: bool | None = ...,
        webhook: Mapping[str, Any] | None = ...,
        proxy: str | None = ...,
        max_age: int | None = ...,
        store_in_cache: bool | None = ...,
        append_to_id: str | None = ...,
        ignore_invalid_urls: bool | None = ...,
        max_concurrency: int | None = ...,
        zero_data_retention: bool | None = ...,
        integration: str | None = ...,
        idempotency_key: str | None = ...,
    ) -> Any: ...

    def start_crawl(
        self,
        url: str,
        *,
        include_paths: Sequence[str] | None = ...,
        exclude_paths: Sequence[str] | None = ...,
        max_discovery_depth: int | None = ...,
        ignore_sitemap: bool | None = ...,
        limit: int | None = ...,
        crawl_entire_domain: bool | None = ...,
        allow_subdomains: bool | None = ...,
        allow_external_links: bool | None = ...,
        scrape_options: ScrapeOptions | None = ...,
        webhook: Mapping[str, Any] | None = ...,
    ) -> Any: ...

    def scrape(self, url: str, *, formats: Sequence[Any] | None = ..., **kwargs: Any) -> Any: ...

    def batch_scrape(
        self,
        urls: Sequence[str],
        *,
        formats: Sequence[Any] | None = ...,
        headers: Mapping[str, str] | None = ...,
        include_tags: Sequence[str] | None = ...,
        exclude_tags: Sequence[str] | None = ...,
        only_main_content: bool | None = ...,
        timeout: int | None = ...,
        wait_for: int | None = ...,
        mobile: bool | None = ...,
        parsers: Sequence[Any] | None = ...,
        actions: Sequence[Any] | None = ...,
        location: Any | None = ...,
        skip_tls_verification: bool | None = ...,
        remove_base64_images: bool | None = ...,
        fast_mode: bool | None = ...,
        use_mock: str | None = ...,
        block_ads: bool | None = ...,
        proxy: str | None = ...,
        max_age: int | None = ...,
        store_in_cache: bool | None = ...,
        webhook: Mapping[str, Any] | None = ...,
        append_to_id: str | None = ...,
        ignore_invalid_urls: bool | None = ...,
        max_concurrency: int | None = ...,
        zero_data_retention: bool | None = ...,
        integration: str | None = ...,
        idempotency_key: str | None = ...,
        poll_interval: int | None = ...,
        wait_timeout: int | None = ...,
    ) -> Any: ...

    def get_batch_scrape_status(
        self, job_id: str, *, pagination_config: PaginationConfig | None = ...
    ) -> Any: ...

    def get_crawl_status(
        self, job_id: str, *, pagination_config: PaginationConfig | None = ...
    ) -> Any: ...
