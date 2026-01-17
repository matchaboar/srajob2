from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import time
import re
from urllib.parse import parse_qs, urlparse, urljoin
from html.parser import HTMLParser
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from firecrawl import Firecrawl
from firecrawl.v2.types import PaginationConfig
from fetchfox_sdk import FetchFox
import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from ...config import settings, runtime_config
from ...components.models import (
    FetchFoxPriority,
    MAX_FETCHFOX_VISITS,
    GreenhouseBoardResponse,
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)
from ...constants import (
    DEFAULT_US_STATE_CODES,
    DEFAULT_US_STATE_NAMES,
    is_remote_company,
    location_matches_usa,
    title_matches_required_keywords,
)
from ..helpers.firecrawl import (
    build_firecrawl_webhook as _build_firecrawl_webhook,
    extract_first_json_doc as _extract_first_json_doc,
    extract_first_text_doc as _extract_first_text_doc,
    metadata_urls_to_list as _metadata_urls_to_list,
    should_mock_convex_webhooks as _should_mock_convex_webhooks,
    should_use_mock_firecrawl as _should_use_mock_firecrawl,
    stringify_firecrawl_metadata as _stringify_firecrawl_metadata,
)
from ..helpers.provider import (
    build_provider_status_url,
    build_request_snapshot,
    log_provider_dispatch,
    log_sync_response,
    mask_secret,
    sanitize_headers,
)
from ..helpers.scrape_utils import (
    _extract_job_detail_seed_from_json,
    _jobs_from_scrape_items,
    _shrink_payload,
    build_description_preview,
    build_firecrawl_schema,
    derive_company_from_url,
    normalize_compensation_value,
    parse_markdown_hints,
    parse_posted_at,
    split_description_metadata,
    strip_known_nav_blocks,
    fetch_seen_urls_for_site,
    normalize_fetchfox_items,
    normalize_firecrawl_items,
    trim_scrape_for_convex,
    looks_like_truncated_description,
)
from ..helpers.page_detection import is_invalid_job_url
from ..helpers.url_handling import _strip_ashby_application_url
from ..helpers.link_extractors import (
    gather_strings,
    extract_job_urls_from_json_payload,
    extract_links_from_payload,
    normalize_url,
    strip_wrapping_url,
)
from ..helpers.regex_patterns import (
    APPLY_WORD_PATTERN,
    ASHBY_JOB_SLUG_PATTERN,
    CODE_FENCE_CONTENT_PATTERN,
    CODE_FENCE_END_PATTERN,
    CODE_FENCE_START_PATTERN,
    CONFLUENT_JOB_PATH_PATTERN,
    COUNTRY_CODE_PATTERN,
    DIGIT_PATTERN,
    GREENHOUSE_BOARDS_PATH_PATTERN,
    GREENHOUSE_URL_PATTERN,
    JOB_ID_PATH_PATTERN,
    INVALID_JSON_ESCAPE_PATTERN,
    LOCATION_ANYWHERE_PATTERN,
    LOCATION_CITY_STATE_PATTERN,
    LOCATION_FULL_PATTERN,
    LOCATION_LABEL_PATTERN,
    LOCATION_LINE_PATTERN,
    LOCATION_PAREN_PATTERN,
    LOCATION_SPLIT_PATTERN,
    LOCATION_TOKEN_SPLIT_PATTERN,
    MARKDOWN_LINK_PATTERN,
    MULTI_SPACE_PATTERN,
    NON_NUMERIC_DOT_PATTERN,
    NON_NUMERIC_PATTERN,
    REQUEST_ID_PATTERN,
    RETIREMENT_PLAN_PATTERN,
    TITLE_IN_BAR_PATTERN,
    TITLE_LOCATION_PAREN_PATTERN,
    URL_PATTERN,
    CAD_CURRENCY_PATTERNS,
    GBP_CURRENCY_PATTERNS,
    INR_CURRENCY_PATTERNS,
    EUR_CURRENCY_PATTERNS,
    AUD_CURRENCY_PATTERNS,
    COMP_INR_RANGE_PATTERN,
    COMP_K_PATTERN,
    COMP_LPA_PATTERN,
    COMP_USD_RANGE_PATTERN,
)
from ..scrapers import BaseScraper, FetchfoxScraper, FirecrawlScraper, SpiderCloudScraper
from ..site_handlers import get_site_handler
from ..site_handlers.base import BaseSiteHandler
from .constants import (
    FIRECRAWL_CACHE_MAX_AGE_MS,
    FIRECRAWL_STATUS_EXPIRATION_MS,
    FIRECRAWL_STATUS_WARN_MS,
    FirecrawlJobKind,
)
from .errors import ScrapeErrorInput, clean_scrape_error_payload
from .step import log_scrape_error as _log_scrape_error
from .factories import (
    build_fetchfox_scraper as _build_fetchfox_scraper,
    build_firecrawl_scraper as _build_firecrawl_scraper,
    build_spidercloud_scraper as _build_spidercloud_scraper,
    select_scraper_for_site as _select_scraper_for_site,
)
from .firecrawl import (
    WebhookModel as _WebhookModel,
    mock_firecrawl_status_response as _mock_firecrawl_status_response,
    record_pending_firecrawl_webhook as _record_pending_firecrawl_webhook,
    serialize_firecrawl_job as _serialize_firecrawl_job,
    start_firecrawl_batch as _start_firecrawl_batch,
)
from .types import FirecrawlWebhookEvent, Site
from ...services import telemetry
from ...dbos_runtime import queue as dbos_queue

from .step import record_scrape_url_attempts as _record_scrape_url_attempts
from .step import (
    _to_greenhouse_marketing_url,
    fetch_pending_firecrawl_webhooks_step,
    filter_new_job_urls,
    get_firecrawl_webhook_status_step,
    ingest_jobs_from_scrape_step,
    insert_ignored_job_step,
    insert_scrape_record_step,
    list_job_detail_configs_step,
    lookup_job_id_for_url as _lookup_job_id_for_url,
    mark_firecrawl_webhook_processed_step,
    record_ignored_job_step,
    record_job_detail_heuristic_step,
    resolve_pagination_limit_step,
    store_job_description_step,
)
from .url_processing import (
    _is_base_listing_page,
    _looks_like_auth_url,
    _is_probable_listing_url,
    _compile_url_pattern,
    _matches_url_pattern,
    _looks_like_job_detail_url,
    _handler_allows_url,
    _classify_filtered_urls,
    _filter_job_urls,
)
from .heuristics import (
    _normalize_locations,
    _is_plausible_location,
    _derive_location_states,
    _derive_countries,
    _build_location_search,
    _looks_like_location_anywhere,
    _describe_exception,
    _extract_request_id,
    _extract_pending_count,
    _parse_comp_int,
    _parse_comp_float,
    _match_has_comp_magnitude_suffix,
    _select_compensation_from_bounds,
    _parse_compensation_match,
    _extract_compensation_from_text,
    _first_match,
    _build_ordered_regexes,
    _detect_currency_code,
    _domain_from_url,
)
# Use new normalizers pipeline (backwards-compatible adapter)
from ..normalizers.pipeline import build_job_update as _build_job_detail_heuristic_patch
from ..normalizers.types import NORMALIZATION_VERSION as HEURISTIC_VERSION

# Import factory convenience functions (patchable by tests)
from .factories import (
    _make_fetchfox_scraper,
    _make_firecrawl_scraper,
    _make_spidercloud_scraper,
    select_scraper_for_site_with_defaults as select_scraper_for_site,
)

_log_provider_dispatch = log_provider_dispatch

DEFAULT_PAGINATION_LIMIT = 0
_log_sync_response = log_sync_response
_build_request_snapshot = build_request_snapshot
_build_provider_status_url = build_provider_status_url
_mask_secret = mask_secret
_sanitize_headers = sanitize_headers
_trim_scrape_for_convex = trim_scrape_for_convex
_clean_scrape_error_payload = clean_scrape_error_payload

__all__ = [
    # Helpers
    "fetch_seen_urls_for_site",
    "normalize_fetchfox_items",
    # Factory functions (patchable by tests)
    "_make_fetchfox_scraper",
    "_make_firecrawl_scraper",
    "_make_spidercloud_scraper",
    "select_scraper_for_site",
    # Types
    "Site",
    "FirecrawlWebhookEvent",
]

COMP_MAGNITUDE_SUFFIX_PATTERN = r"^\s*(?:[kmb]|bn|mm|million|billion|trillion)\b"
COMP_MAGNITUDE_SUFFIX_RE = re.compile(COMP_MAGNITUDE_SUFFIX_PATTERN, flags=re.IGNORECASE)

PAGINATION_ENQUEUE_STAGGER_MS = 30_000

SCRAPE_URL_QUEUE_TTL_MS = 48 * 60 * 60 * 1000
SCRAPE_URL_QUEUE_MAX_ATTEMPTS = 3
SPIDERCLOUD_BATCH_SIZE = runtime_config.spidercloud_job_details_batch_size
SCRAPE_URL_QUEUE_LIST_LIMIT = 500
TEMPORAL_PAYLOAD_MAX_CHARS = 10 * 1024 * 1024
SPIDERCLOUD_ACTIVITY_PAYLOAD_MAX_CHARS = 64_000

logger = logging.getLogger("temporal.worker.activities")
scheduling_logger = logging.getLogger("temporal.scheduler")
