"""Dynamic timeout calculation for DBOS step functions.

This module provides helpers to calculate appropriate timeouts based on:
- Handler's wait_for config (if present)
- URL type (API vs browser, listing vs detail)
- Base timeout from runtime config
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from ...config.runtime_config import RuntimeConfig
    from ..site_handlers.base import BaseSiteHandler


# Default timeout values (in seconds)
CONVEX_QUERY_TIMEOUT = 30
CONVEX_MUTATION_TIMEOUT = 30
CONVEX_ACTION_TIMEOUT = 60
HTTP_POST_TIMEOUT = 60
TELEMETRY_TIMEOUT = 10
API_CALL_TIMEOUT = 120  # Extended timeout for API-based scrapes (no browser)


def calculate_step_timeout(
    url: str,
    handler: BaseSiteHandler | None,
    runtime_config: RuntimeConfig,
    is_listing: bool = False,
) -> int:
    """Calculate timeout for a scrape step.

    Args:
        url: The URL being scraped
        handler: Site handler for the URL (may be None)
        runtime_config: Runtime configuration with base timeouts
        is_listing: Whether this is a listing page scrape

    Returns:
        Timeout in seconds for the step
    """
    if is_listing:
        return runtime_config.spidercloud_listing_timeout_seconds  # 300s

    base = runtime_config.spidercloud_http_timeout_seconds  # 60s

    if handler:
        from ..site_handlers.base import BaseSiteHandler as HandlerClass

        config = handler.normalize_spidercloud_config(handler.get_spidercloud_config(url))
        wait_for_timeout = HandlerClass.extract_wait_for_timeout_seconds(config)
        if wait_for_timeout > 0:
            return min(max(base, wait_for_timeout), 180)

        # API calls (request: "basic") get extended timeout (120s)
        if config.get("request") == "basic":
            return API_CALL_TIMEOUT

    return base


def calculate_http_step_timeout(
    handler_config: Dict[str, Any] | None,
    base_timeout: int = 60,
) -> int:
    """Calculate timeout for an HTTP-based step.

    Args:
        handler_config: Normalized SpiderCloud config from handler
        base_timeout: Base timeout in seconds (default 60)

    Returns:
        Timeout in seconds for the HTTP step
    """
    if not handler_config:
        return base_timeout

    from ..site_handlers.base import BaseSiteHandler

    wait_for_timeout = BaseSiteHandler.extract_wait_for_timeout_seconds(handler_config)
    if wait_for_timeout > 0:
        timeout = max(base_timeout, wait_for_timeout)
        return min(timeout, 180)

    # API calls (request: "basic") get extended timeout
    if handler_config.get("request") == "basic":
        return API_CALL_TIMEOUT

    return base_timeout
