"""Shim module for pyright to locate next_page_pattern."""

from __future__ import annotations

from typing import Any


async def try_xpath_schema(
    url: str | None,
    local_file_path: str | None,
    schema_text: dict[str, Any],
    schema_next_page: dict[str, Any] | None,
) -> Any:
    return None
