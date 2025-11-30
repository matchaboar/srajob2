from __future__ import annotations

from typing import Any, Mapping

__all__ = ["FetchFox"]


class FetchFox:
    def __init__(self, api_key: str | None = ...) -> None: ...

    def scrape(self, payload: Mapping[str, Any] | Any) -> Any: ...
