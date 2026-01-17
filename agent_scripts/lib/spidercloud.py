"""SpiderCloud utilities for agent scripts.

Consolidates common patterns for:
- Collecting async/sync/iterator responses
- Building default SpiderCloud params
- Fetching and saving fixtures
- Loading API keys from environment
"""

from __future__ import annotations

import orjson
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    from job_scrape_application.workflows.site_handlers.base import BaseSiteHandler

# Project root for loading .env files
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def load_api_key(project_root: Path | None = None) -> str:
    """Load SpiderCloud API key from environment.

    Loads .env files in order:
    1. {project_root}/.env
    2. {project_root}/job_board_application/.env.production

    Args:
        project_root: Root directory of the project. Defaults to auto-detected.

    Returns:
        The API key string.

    Raises:
        SystemExit: If no API key is found.
    """
    root = project_root or _PROJECT_ROOT
    load_dotenv(root / ".env")
    load_dotenv(root / "job_board_application/.env.production", override=False)

    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    return api_key


async def collect_response(response: Any) -> List[Any]:
    """Collect SpiderCloud response from various async patterns.

    Handles:
    - Async iterators (streaming responses)
    - Awaitables (single responses)
    - Direct values (sync responses)

    Args:
        response: The SpiderCloud response object.

    Returns:
        List of response items.
    """
    if hasattr(response, "__aiter__"):
        items: List[Any] = []
        async for item in response:
            items.append(item)
        return items

    if hasattr(response, "__await__"):
        result = await response
        return [result] if result is not None else []

    return [response] if response is not None else []


def build_default_params(
    url: str,
    handler: Optional["BaseSiteHandler"] = None,
    *,
    return_format: str | List[str] | None = None,
    request: str = "chrome",
    **overrides: Any,
) -> Dict[str, Any]:
    """Build SpiderCloud params with sensible defaults.

    Merges handler-specific config with standard defaults and any overrides.

    Args:
        url: The URL to scrape (used for handler config lookup).
        handler: Optional site handler for URL-specific configuration.
        return_format: SpiderCloud return_format value. Defaults to ["raw_html"].
        request: SpiderCloud request type. Defaults to "chrome".
        **overrides: Additional params to merge/override.

    Returns:
        Complete params dict for SpiderCloud scrape_url().
    """
    params: Dict[str, Any] = {}

    # Apply handler-specific config if available
    if handler is not None:
        handler_config = handler.get_spidercloud_config(url)
        normalized = handler.normalize_spidercloud_config(handler_config)
        params.update(normalized)

    # Set return format
    if return_format is not None:
        if isinstance(return_format, str):
            params["return_format"] = [return_format]
        else:
            params["return_format"] = return_format
    elif "return_format" not in params:
        params["return_format"] = ["raw_html"]

    # Standard defaults
    params.setdefault("request", request)
    params.setdefault("follow_redirects", True)
    params.setdefault("redirect_policy", "Loose")
    params.setdefault("external_domains", ["*"])
    params.setdefault("preserve_host", True)
    params.setdefault("metadata", True)
    params.setdefault("limit", 1)

    # Apply any overrides
    params.update(overrides)

    return params


async def fetch_and_save_fixture(
    url: str,
    output_path: Path,
    params: Dict[str, Any] | None = None,
    api_key: str | None = None,
    handler: Optional["BaseSiteHandler"] = None,
) -> Dict[str, Any]:
    """Fetch from SpiderCloud and save as a test fixture.

    Args:
        url: URL to scrape.
        output_path: Path to write the fixture JSON.
        params: Optional pre-built params. If None, uses build_default_params().
        api_key: Optional API key. If None, loads from environment.
        handler: Optional site handler for URL-specific config.

    Returns:
        The fixture dict with 'request' and 'response' keys.
    """
    from spider import AsyncSpider

    if api_key is None:
        api_key = load_api_key()

    if params is None:
        params = build_default_params(url, handler=handler)

    async with AsyncSpider(api_key=api_key) as client:
        response_items = await collect_response(
            client.scrape_url(
                url,
                params=params,
                stream=False,
                content_type="application/json",
            )
        )

    fixture = {
        "request": {
            "url": url,
            "params": params,
            "stream": False,
        },
        "response": response_items[0] if response_items else {},
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        orjson.dumps(fixture, option=orjson.OPT_INDENT_2).decode("utf-8"),
        encoding="utf-8",
    )

    return fixture
