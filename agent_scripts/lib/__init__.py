"""Shared utilities for agent scripts.

This module provides common functionality for scripts that interact with
SpiderCloud, parse URLs, generate fixtures, and create test assertions.
"""

from __future__ import annotations

from .spidercloud import (
    collect_response,
    build_default_params,
    fetch_and_save_fixture,
    load_api_key,
)
from .site_utils import (
    SiteInfo,
    extract_site_info,
    get_canonical_detail_url,
)
from .fixtures import (
    save_fixture,
    load_fixture,
)
from .assertions import (
    generate_assertion_yaml,
    generate_placeholder_assertion_yaml,
)

__all__ = [
    # spidercloud
    "collect_response",
    "build_default_params",
    "fetch_and_save_fixture",
    "load_api_key",
    # site_utils
    "SiteInfo",
    "extract_site_info",
    "get_canonical_detail_url",
    # fixtures
    "save_fixture",
    "load_fixture",
    # assertions
    "generate_assertion_yaml",
    "generate_placeholder_assertion_yaml",
]
