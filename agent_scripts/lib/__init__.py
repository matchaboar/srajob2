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
    extract_site_info_from_url,
    get_canonical_detail_url,
)
from .fixtures import (
    save_fixture,
    load_fixture,
    build_fixture_structure,
    get_fixture_paths,
)
from .assertions import (
    generate_assertion_yaml,
    generate_placeholder_assertion_yaml,
    generate_listing_assertion_yaml,
)
from .convex import (
    extract_job_id_from_url,
    fetch_job_by_id,
    fetch_site_by_id,
    run_convex_query,
)
from .dbos import (
    get_queue_status,
    clear_site_queue,
    get_queue_summary_for_companies,
    list_queue_entries,
)

__all__ = [
    # spidercloud
    "collect_response",
    "build_default_params",
    "fetch_and_save_fixture",
    "load_api_key",
    # site_utils
    "SiteInfo",
    "extract_site_info_from_url",
    "get_canonical_detail_url",
    # fixtures
    "save_fixture",
    "load_fixture",
    "build_fixture_structure",
    "get_fixture_paths",
    # assertions
    "generate_assertion_yaml",
    "generate_placeholder_assertion_yaml",
    "generate_listing_assertion_yaml",
    # convex
    "extract_job_id_from_url",
    "fetch_job_by_id",
    "fetch_site_by_id",
    "run_convex_query",
    # dbos
    "get_queue_status",
    "clear_site_queue",
    "get_queue_summary_for_companies",
    "list_queue_entries",
]
