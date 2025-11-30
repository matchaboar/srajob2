from __future__ import annotations

from typing import Any, Mapping, Sequence

__all__ = ["PaginationConfig", "ScrapeOptions"]


class PaginationConfig:
    auto_paginate: bool | None
    max_pages: int | None
    max_results: int | None
    max_wait_time: int | None

    def __init__(
        self,
        auto_paginate: bool | None = ...,
        max_results: int | None = ...,
        max_pages: int | None = ...,
        max_wait_time: int | None = ...,
        **_: Any,
    ) -> None: ...


class ScrapeOptions:
    formats: Sequence[Any] | None
    headers: Mapping[str, str] | None
    include_tags: Sequence[str] | None
    exclude_tags: Sequence[str] | None
    only_main_content: bool | None
    timeout: int | None
    wait_for: int | None
    mobile: bool | None
    parsers: Sequence[Any] | None
    actions: Sequence[Any] | None
    location: Any | None
    skip_tls_verification: bool | None
    remove_base64_images: bool | None
    fast_mode: bool | None
    use_mock: str | None
    block_ads: bool | None
    proxy: str | None
    max_age: int | None
    store_in_cache: bool | None
    integration: str | None

    def __init__(
        self,
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
        integration: str | None = ...,
        **_: Any,
    ) -> None: ...

    def model_dump(self, *, mode: str | None = ..., exclude_none: bool | None = ...) -> dict[str, Any]: ...
