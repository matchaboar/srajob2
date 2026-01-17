"""Archived main activity functions.

These Temporal activity functions are deprecated and kept for backward compatibility.
New code should use the step functions from job_scrape_application.workflows.activities.step
or the workflows from job_scrape_application.workflows.workflow instead.

.. deprecated::
    All @activity.defn functions in this module are deprecated.
"""

from __future__ import annotations

import asyncio
import inspect
import orjson
import logging
import os
import re
import time
import uuid
from html.parser import HTMLParser
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from fetchfox_sdk import FetchFox
from firecrawl import Firecrawl
from firecrawl.v2.types import PaginationConfig
from temporalio import activity
from temporalio.exceptions import ApplicationError

# Config imports - go up three levels from _archive
from ....config import runtime_config, settings

# Component imports
from ....components.models import (
    FetchFoxPriority,
    GreenhouseBoardResponse,
    MAX_FETCHFOX_VISITS,
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)

# Constants imports
from ....constants import (
    DEFAULT_US_STATE_CODES,
    DEFAULT_US_STATE_NAMES,
    is_remote_company,
    location_matches_usa,
    title_matches_required_keywords,
)

# DBOS queue imports
from ....dbos_runtime import queue as dbos_queue

# Services imports
from ....services import telemetry
from ....services.convex_client import convex_mutation, convex_query

# Workflow helper imports
from ...helpers.firecrawl import (
    build_firecrawl_webhook as _build_firecrawl_webhook,
    extract_first_json_doc as _extract_first_json_doc,
    extract_first_text_doc as _extract_first_text_doc,
    metadata_urls_to_list as _metadata_urls_to_list,
    should_mock_convex_webhooks as _should_mock_convex_webhooks,
    should_use_mock_firecrawl as _should_use_mock_firecrawl,
    stringify_firecrawl_metadata as _stringify_firecrawl_metadata,
)
from ...helpers.link_extractors import (
    extract_job_urls_from_json_payload,
    extract_links_from_payload,
    gather_strings,
    normalize_url,
    strip_wrapping_url,
)
from ...helpers.page_detection import is_invalid_job_url
from ...helpers.provider import (
    build_provider_status_url,
    build_request_snapshot,
    log_provider_dispatch,
    log_sync_response,
    mask_secret,
    sanitize_headers,
)
from ...helpers.regex_patterns import (
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
    INVALID_JSON_ESCAPE_PATTERN,
    JOB_ID_PATH_PATTERN,
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
)
from ...helpers.scrape_utils import (
    _extract_job_detail_seed_from_json,
    _jobs_from_scrape_items,
    _shrink_payload,
    build_description_preview,
    build_firecrawl_schema,
    derive_company_from_url,
    fetch_seen_urls_for_site,
    looks_like_truncated_description,
    normalize_compensation_value,
    normalize_fetchfox_items,
    normalize_firecrawl_items,
    parse_markdown_hints,
    parse_posted_at,
    split_description_metadata,
    strip_known_nav_blocks,
    trim_scrape_for_convex,
)
from ...helpers.url_handling import _strip_ashby_application_url
from ...normalizers.pipeline import build_job_update as _build_job_detail_heuristic_patch
from ...normalizers.types import NORMALIZATION_VERSION as HEURISTIC_VERSION
from ...scrapers import BaseScraper, FetchfoxScraper, FirecrawlScraper, SpiderCloudScraper
from ...site_handlers import get_site_handler
from ...site_handlers.base import BaseSiteHandler

# Import from parent module's constants and errors
from ..constants import (
    FIRECRAWL_CACHE_MAX_AGE_MS,
    FIRECRAWL_STATUS_EXPIRATION_MS,
    FIRECRAWL_STATUS_WARN_MS,
    FirecrawlJobKind,
)
from ..errors import ScrapeErrorInput, clean_scrape_error_payload

# Import from parent module's factories
from ..factories import (
    build_fetchfox_scraper as _build_fetchfox_scraper,
    build_firecrawl_scraper as _build_firecrawl_scraper,
    build_spidercloud_scraper as _build_spidercloud_scraper,
    select_scraper_for_site as _select_scraper_for_site,
)

# Import from parent module's firecrawl
from ..firecrawl import (
    WebhookModel as _WebhookModel,
    mock_firecrawl_status_response as _mock_firecrawl_status_response,
    record_pending_firecrawl_webhook as _record_pending_firecrawl_webhook,
    serialize_firecrawl_job as _serialize_firecrawl_job,
    start_firecrawl_batch as _start_firecrawl_batch,
)

# Import from parent module's step functions
from ..step import (
    _to_greenhouse_marketing_url,
    fetch_pending_firecrawl_webhooks_step,
    filter_new_job_urls,
    get_firecrawl_webhook_status_step,
    ingest_jobs_from_scrape_step,
    insert_ignored_job_step,
    insert_scrape_record_step,
    list_job_detail_configs_step,
    log_scrape_error as _log_scrape_error,
    lookup_job_id_for_url as _lookup_job_id_for_url,
    mark_firecrawl_webhook_processed_step,
    record_ignored_job_step,
    record_job_detail_heuristic_step,
    record_scrape_url_attempts as _record_scrape_url_attempts,
    resolve_pagination_limit_step,
    store_job_description_step,
)
from ..types import FirecrawlWebhookEvent, Site

# Import from parent module's url_processing
from ..url_processing import (
    _classify_filtered_urls,
    _compile_url_pattern,
    _filter_job_urls,
    _handler_allows_url,
    _is_base_listing_page,
    _is_probable_listing_url,
    _looks_like_auth_url,
    _looks_like_job_detail_url,
    _matches_url_pattern,
)

# Import from archive submodules
from .convex_operations import _convex_http_base_url, _convex_site_id
from .logging_activities import _build_log_message, _coerce_workflow_id, _short_preview
from .site_management import _looks_like_convex_id, _strip_none_values

# Import from parent activities module (heuristics)
from ..heuristics import (
    _build_location_search,
    _build_ordered_regexes,
    _derive_countries,
    _derive_location_states,
    _describe_exception,
    _detect_currency_code,
    _domain_from_url,
    _extract_compensation_from_text,
    _extract_pending_count,
    _extract_request_id,
    _first_match,
    _is_plausible_location,
    _looks_like_location_anywhere,
    _match_has_comp_magnitude_suffix,
    _normalize_locations,
    _parse_comp_float,
    _parse_comp_int,
    _parse_compensation_match,
    _select_compensation_from_bounds,
)

# Import large activity functions from temporal_activities
# These are deprecated and kept for backward compatibility
from .temporal_activities import (
    process_spidercloud_job_batch,
    process_spidercloud_listing_batch,
    collect_firecrawl_job_result,
    store_scrape,
    _extract_job_urls_from_scrape,
    process_pending_job_details_batch,
    batch_store_scrapes_background,
)

# Type aliases
_log_provider_dispatch = log_provider_dispatch
_log_sync_response = log_sync_response
_build_request_snapshot = build_request_snapshot
_build_provider_status_url = build_provider_status_url
_mask_secret = mask_secret
_sanitize_headers = sanitize_headers
_trim_scrape_for_convex = trim_scrape_for_convex
_clean_scrape_error_payload = clean_scrape_error_payload


# Constants
DEFAULT_PAGINATION_LIMIT = 0
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


# =============================================================================
# Helper functions used by activity functions
# =============================================================================


def _build_listing_zero_url_context(
    scrape_payload: Dict[str, Any],
    base_url: str | None,
) -> Dict[str, Any]:
    if not base_url:
        return {}
    items_block = scrape_payload.get("items") if isinstance(scrape_payload, dict) else {}
    raw_block = items_block.get("raw") if isinstance(items_block, dict) else None
    raw_items: list[Any]
    if isinstance(raw_block, list):
        raw_items = raw_block
    elif isinstance(raw_block, dict):
        raw_items = [raw_block]
    else:
        raw_items = []

    normalized_target = normalize_url(base_url) or base_url
    selected_raw: Dict[str, Any] | None = None
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        raw_url = raw.get("url")
        if not isinstance(raw_url, str):
            continue
        normalized_raw = normalize_url(raw_url) or raw_url
        if normalized_raw == normalized_target:
            selected_raw = raw
            break
    if selected_raw is None:
        for raw in raw_items:
            if isinstance(raw, dict):
                selected_raw = raw
                break

    markdown_val = None
    html_val = None
    events_val = None
    links_val = None
    selected_url = None
    if selected_raw:
        selected_url = selected_raw.get("url") if isinstance(selected_raw.get("url"), str) else None
        markdown_val = (
            selected_raw.get("markdown")
            or selected_raw.get("commonmark")
            or selected_raw.get("content")
        )
        html_val = selected_raw.get("raw_html") or selected_raw.get("html")
        events_val = selected_raw.get("events")
        links_val = selected_raw.get("job_urls") or selected_raw.get("links")

    context = {
        "pageUrl": base_url,
        "pageUrlMatched": selected_url,
        "rawItemsCount": len(raw_items) if raw_items else None,
        "markdownLength": len(markdown_val) if isinstance(markdown_val, str) else None,
        "eventCount": len(events_val) if isinstance(events_val, list) else None,
        "linkCount": len(links_val) if isinstance(links_val, list) else None,
        "markdownSample": _shrink_payload(markdown_val, 6000),
        "htmlSample": _shrink_payload(html_val, 6000),
        "eventSample": _shrink_payload(events_val, 3000),
        "linkSample": links_val[:50] if isinstance(links_val, list) else None,
    }
    return _strip_none_values(context)


def _get_activity_worker_id() -> str | None:
    env_worker_id = os.getenv("SCRAPE_WORKER_ID", "").strip()
    if env_worker_id:
        return env_worker_id
    try:
        info = activity.info()
    except Exception:
        return None
    worker_identity = getattr(info, "worker_identity", None)
    if isinstance(worker_identity, str):
        worker_identity = worker_identity.strip()
        if worker_identity:
            return worker_identity
    return None


def _store_job_descriptions_via_http(
    jobs: List[Dict[str, Any]],
    source_url: str | None,
    provider: str | None,
    workflow_name: str | None,
    log_workflow_event: Callable[..., None] | None = None,
) -> None:
    base_url = _convex_http_base_url()
    if not base_url:
        logger.warning("Convex HTTP URL missing; skipping description uploads")
        return

    stored = 0
    for job in jobs:
        description = job.get("description")
        if not isinstance(description, str) or not description.strip():
            continue
        if looks_like_truncated_description(description):
            continue
        raw_url = job.get("url")
        if not isinstance(raw_url, str) or not raw_url.strip():
            continue
        normalized = normalize_url(raw_url) or raw_url.strip()
        job_id = _lookup_job_id_for_url(normalized)
        if not job_id:
            continue

        success = store_job_description_step(
            base_url=base_url,
            job_id=job_id,
            description=description,
        )
        if success:
            stored += 1
        else:
            logger.warning("Description upload failed for %s", normalized)

    if stored:
        logger.info(
            "Stored %s job descriptions for %s (provider=%s workflow=%s)",
            stored,
            source_url or "unknown site",
            provider or "unknown",
            workflow_name or "unknown",
        )


def _activity_cancellation_payload() -> Dict[str, Any]:
    """Best-effort capture of Temporal cancellation details for logging."""

    payload: Dict[str, Any] = {}
    try:
        details = activity.cancellation_details()
    except Exception:
        details = None
    if details is not None:
        payload.update(
            {
                "cancelNotFound": details.not_found,
                "cancelRequested": details.cancel_requested,
                "cancelPaused": details.paused,
                "cancelTimedOut": details.timed_out,
                "cancelWorkerShutdown": details.worker_shutdown,
            }
        )
    try:
        is_cancelled = activity.is_cancelled()
    except Exception:
        is_cancelled = None
    if is_cancelled is not None:
        payload["cancelled"] = bool(is_cancelled)
    return payload


def _summarize_scrape_payload(res: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(res, dict):
        return {"provider": "unknown"}

    items = res.get("items") if isinstance(res, dict) else {}
    normalized = items.get("normalized") if isinstance(items, dict) else None
    jobs = len(normalized) if isinstance(normalized, list) else 0
    skipped_urls = res.get("skippedUrls") if isinstance(res, dict) else None
    summary: Dict[str, Any] = {
        "provider": res.get("provider") or items.get("provider") if isinstance(items, dict) else None,
        "queued": items.get("queued") if isinstance(items, dict) else None,
        "jobId": items.get("jobId") if isinstance(items, dict) else None,
        "statusUrl": items.get("statusUrl") if isinstance(items, dict) else None,
        "jobs": jobs,
    }
    if skipped_urls:
        summary["skippedUrls"] = skipped_urls
    if res.get("workflowName"):
        summary["workflowName"] = res.get("workflowName")
    if res.get("costMilliCents"):
        summary["costMilliCents"] = res.get("costMilliCents")
    return {k: v for k, v in summary.items() if v is not None}


def _apply_workflow_context(
    scrape: Dict[str, Any],
    workflow_context: Dict[str, Any] | None,
    site: Site | None,
) -> Dict[str, Any]:
    if not isinstance(scrape, dict):
        return scrape
    if workflow_context:
        for key in ("workflowName", "workflowId", "runId"):
            value = workflow_context.get(key)
            if value is not None and scrape.get(key) is None:
                scrape[key] = value
    if site and scrape.get("siteId") is None:
        site_id = _convex_site_id(site)
        if site_id:
            scrape["siteId"] = site_id
    return scrape


def _build_recovery_payload(scrape: Dict[str, Any], site: Site | None) -> Dict[str, Any] | None:
    if not isinstance(scrape, dict):
        return None
    items_raw = scrape.get("items")
    items = items_raw if isinstance(items_raw, dict) else {}
    job_id = scrape.get("jobId") or items.get("jobId")
    queued = items.get("queued") if isinstance(items, dict) else None
    if not queued or not job_id:
        return None
    payload = {
        "jobId": str(job_id),
        "webhookId": scrape.get("webhookId") or items.get("webhookId"),
        "metadata": scrape.get("metadata"),
        "siteId": _convex_site_id(site) if site else None,
        "siteUrl": site.get("url") if isinstance(site, dict) else None,
        "statusUrl": scrape.get("statusUrl") or items.get("statusUrl"),
        "receivedAt": scrape.get("receivedAt") or items.get("receivedAt"),
    }
    return {k: v for k, v in payload.items() if v is not None}


def _make_fetchfox_scraper() -> FetchfoxScraper:
    return _build_fetchfox_scraper(
        build_request_snapshot=_build_request_snapshot,
        log_provider_dispatch=_log_provider_dispatch,
        log_sync_response=_log_sync_response,
    )


def _make_firecrawl_scraper() -> FirecrawlScraper:
    # Note: start_firecrawl_webhook_scrape is defined below as an activity
    from . import start_firecrawl_webhook_scrape as _start_firecrawl_webhook_scrape_activity
    return _build_firecrawl_scraper(
        start_firecrawl_webhook_scrape=_start_firecrawl_webhook_scrape_activity,
        log_scrape_error=_log_scrape_error,
        build_request_snapshot=_build_request_snapshot,
        log_provider_dispatch=_log_provider_dispatch,
        log_sync_response=_log_sync_response,
        firecrawl_cls=Firecrawl,
    )


def _make_spidercloud_scraper() -> SpiderCloudScraper:
    return _build_spidercloud_scraper(
        mask_secret=_mask_secret,
        sanitize_headers=_sanitize_headers,
        build_request_snapshot=_build_request_snapshot,
        log_provider_dispatch=_log_provider_dispatch,
        log_sync_response=_log_sync_response,
        trim_scrape_for_convex=_trim_scrape_for_convex,
    )


def select_scraper_for_site(site: Site) -> tuple[BaseScraper, Optional[List[str]]]:
    """Return the scraper instance and any precomputed skip URLs for a site."""

    scraper, skip_urls = _select_scraper_for_site(
        site,
        make_fetchfox=_make_fetchfox_scraper,
        make_firecrawl=_make_firecrawl_scraper,
        make_spidercloud=_make_spidercloud_scraper,
    )

    # Allow callers/tests to monkeypatch fetch_seen_urls_for_site and still forward skip URLs
    if isinstance(scraper, FirecrawlScraper) and not skip_urls:
        url = site.get("url")
        if url:
            skip_urls = fetch_seen_urls_for_site(url, site.get("pattern"))

    return scraper, skip_urls


def _firecrawl_key_suffix() -> Optional[str]:
    key = settings.firecrawl_api_key
    if not key:
        return None

    trimmed = key.strip()
    if not trimmed:
        return None

    return trimmed[-4:]


def _is_firecrawl_related(entry: Dict[str, Any]) -> bool:
    event = str(entry.get("event") or "").lower()
    if "firecrawl" in event:
        return True

    data = entry.get("data")
    if not isinstance(data, dict):
        return False

    provider = data.get("provider")
    if isinstance(provider, str) and "firecrawl" in provider.lower():
        return True

    items = data.get("items")
    if isinstance(items, dict):
        items_provider = items.get("provider")
        if isinstance(items_provider, str) and "firecrawl" in items_provider.lower():
            return True

    async_response = data.get("asyncResponse")
    if isinstance(async_response, dict):
        async_provider = async_response.get("provider")
        if isinstance(async_provider, str) and "firecrawl" in async_provider.lower():
            return True

    return False


def _with_firecrawl_suffix(entry: Dict[str, Any]) -> Dict[str, Any]:
    suffix = _firecrawl_key_suffix()
    if not suffix or not _is_firecrawl_related(entry):
        return entry

    data = entry.get("data")
    if isinstance(data, dict):
        entry["data"] = {**data, "firecrawlKeySuffix": suffix}
    else:
        payload: Dict[str, Any] = {"firecrawlKeySuffix": suffix}
        if data is not None:
            payload["value"] = data
        entry["data"] = payload
    return entry


# =============================================================================
# Activity Functions
# =============================================================================


@activity.defn
async def _scrape_spidercloud_greenhouse(  # deprecated, mixed async+convex
    scraper: SpiderCloudScraper,
    site: Site,
    skip_urls: list[str],
) -> Dict[str, Any]:
    """Fetch Greenhouse listing via SpiderCloud and scrape individual jobs.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """

    try:
        listing = await scraper.fetch_greenhouse_listing(site)
    except Exception as exc:  # noqa: BLE001
        payload = {
            "event": "scrape.greenhouse_listing.activity_failed",
            "level": "error",
            "siteUrl": site.get("url") or "",
            "data": {
                "provider": getattr(scraper, "provider", "spidercloud"),
                "siteId": site.get("_id"),
                "error": str(exc),
            },
        }
        try:
            telemetry.emit_posthog_log(payload)
        except Exception:
            pass
        try:
            telemetry.emit_posthog_exception(
                exc,
                properties={
                    "event": "scrape.greenhouse_listing.activity_failed",
                    "siteUrl": site.get("url"),
                    "siteId": site.get("_id"),
                    "provider": getattr(scraper, "provider", "spidercloud"),
                },
            )
        except Exception:
            pass
        raise
    job_urls = (listing.get("job_urls") or []) if isinstance(listing, dict) else []
    posted_at_by_url: Dict[str, int] = {}
    if isinstance(listing, dict):
        raw_posted = listing.get("posted_at_by_url")
        if isinstance(raw_posted, dict):
            for key, value in raw_posted.items():
                if not isinstance(key, str):
                    continue
                if not isinstance(value, (int, float)):
                    continue
                normalized_key = normalize_url(key) or key
                posted_at_by_url[normalized_key] = int(value)
    urls: list[str] = []
    seen_urls: set[str] = set()
    for candidate in job_urls:
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        normalized = normalize_url(candidate)
        if not normalized or normalized in seen_urls:
            continue
        seen_urls.add(normalized)
        urls.append(normalized)

    seen_for_site: list[str] = []
    try:
        source_url = site.get("url") or ""
        if source_url:
            seen_for_site = fetch_seen_urls_for_site(source_url, site.get("pattern"), urls)
    except Exception:
        seen_for_site = []
    skip_set = set(skip_urls or [])
    skip_set.update(seen_for_site)
    site_id = _convex_site_id(site)
    logger.info(
        "SpiderCloud greenhouse skip list source_url=%s precomputed=%s seen=%s total=%s",
        site.get("url"),
        len(skip_urls or []),
        len(seen_for_site),
        len(skip_set),
    )

    if not urls:
        return {
            "provider": scraper.provider,
            "sourceUrl": site.get("url"),
            "items": {
                "normalized": [],
                "provider": scraper.provider,
                "job_urls": [],
                "existing": list(skip_set),
                "queued": False,
            },
            "skippedUrls": [],
        }

    pending_urls = [u for u in urls if u not in skip_set]
    # Use filter_new_job_urls for efficiency - returns only non-existing URLs (less network transfer)
    new_urls = filter_new_job_urls(pending_urls)
    new_urls_set = set(new_urls)
    existing_urls = [u for u in pending_urls if u not in new_urls_set]
    skipped_existing = len(existing_urls)
    urls_to_scrape = new_urls
    posted_ats_to_enqueue: list[int | None] | None = None
    if posted_at_by_url and urls_to_scrape:
        posted_ats_to_enqueue = [posted_at_by_url.get(url) for url in urls_to_scrape]
        if not any(isinstance(val, (int, float)) for val in posted_ats_to_enqueue):
            posted_ats_to_enqueue = None
    logger.info(
        "SpiderCloud greenhouse urls total=%s pending=%s skipped_existing=%s to_scrape=%s",
        len(urls),
        len(pending_urls),
        skipped_existing,
        len(urls_to_scrape),
    )

    # Persist URLs so they can be retried later even if the worker dies mid-scrape.
    try:
        dbos_queue.enqueue_scrape_urls(
            _strip_none_values(
                {
                    "urls": urls_to_scrape,
                    "sourceUrl": site.get("url") or "",
                    "provider": scraper.provider,
                    "siteId": site_id,
                    "pattern": site.get("pattern"),
                    "postedAts": posted_ats_to_enqueue,
                    "urlTypes": ["detail" for _ in urls_to_scrape],
                }
            )
        )
    except Exception:
        # best-effort; continue to scrape even if enqueue fails
        pass

    # Pull queued URLs for this site/provider (pending or processing)
    queued_urls: list[Dict[str, Any]] = []
    try:
        list_args = _strip_none_values(
            {"site_id": site_id, "provider": scraper.provider, "limit": SCRAPE_URL_QUEUE_LIST_LIMIT}
        )
        batch = dbos_queue.list_scrape_urls(**list_args)
        if isinstance(batch, list):
            queued_urls.extend(batch)
    except Exception:
        queued_urls = []

    stale_urls: list[str] = []
    fresh_urls: list[str] = []
    now = int(time.time() * 1000)
    for row in queued_urls:
        created = int(row.get("createdAt") or 0)
        url = row.get("url")
        if not isinstance(url, str):
            continue
        status = str(row.get("status") or "").lower()
        if status not in {"pending", "processing", ""}:
            continue
        if created and created < now - SCRAPE_URL_QUEUE_TTL_MS:
            stale_urls.append(url)
        else:
            fresh_urls.append(url)

    # Cap batch size and drop invalid URLs
    fresh_urls = [u for u in fresh_urls if isinstance(u, str) and u.strip() and u.startswith("http")]
    fresh_urls = fresh_urls[:SPIDERCLOUD_BATCH_SIZE]

    if stale_urls:
        try:
            dbos_queue.complete_scrape_urls(
                {
                    "items": [{"url": url} for url in stale_urls],
                    "status": "failed",
                    "error": "stale (>48h)",
                }
            )
        except Exception:
            pass

    urls_to_scrape = [u for u in fresh_urls if u in new_urls_set]

    # Listing flow now only enqueues; job detail scrape handled by separate workflow.
    return {
        "provider": scraper.provider,
        "sourceUrl": site.get("url"),
        "items": {
            "normalized": [],
            "provider": scraper.provider,
            "job_urls": urls,
            "existing": existing_urls,
            "queued": True,
            "queuedCount": len(urls_to_scrape),
        },
        "skippedUrls": stale_urls,
    }


@activity.defn
async def scrape_site(
    site: Site,
    workflow_context: Dict[str, Any] | None = None,
    persist_scrape: bool = False,
) -> Dict[str, Any]:
    """Scrape a site, selecting provider based on per-site preference.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """

    selection = select_scraper_for_site(site)
    scraper, skip_urls = (
        await selection if inspect.isawaitable(selection) else selection
    )
    precomputed_skip = skip_urls
    skip_count = len(precomputed_skip or [])

    try:
        logger.info(
            "Scrape dispatch provider=%s site=%s pattern=%s skip_count=%s",
            getattr(scraper, "provider", "unknown"),
            site.get("url"),
            site.get("pattern"),
            skip_count,
        )
    except Exception:
        pass

    site_type = (site.get("type") or "general").lower()
    try:
        if isinstance(scraper, SpiderCloudScraper) and site_type == "greenhouse":
            result = await _scrape_spidercloud_greenhouse(scraper, site, precomputed_skip or [])
        else:
            # Tests expect skip_urls to be forwarded for firecrawl so it can dedupe visited URLs
            result = await scraper.scrape_site(site, skip_urls=precomputed_skip)
    except asyncio.CancelledError:
        # Handle cancellation gracefully - don't let it block the queue
        logger.warning(
            "Scrape activity cancelled site=%s provider=%s",
            site.get("url"),
            getattr(scraper, "provider", "unknown"),
        )
        # Return an empty result rather than propagating cancellation
        # The workflow can decide whether to retry
        return {
            "sourceUrl": site.get("url"),
            "items": {"normalized": [], "failed": []},
            "error": "cancelled",
            "errorType": "CancelledError",
        }
    except asyncio.TimeoutError:
        # Handle timeout gracefully at activity level
        logger.error(
            "Scrape activity timed out site=%s provider=%s",
            site.get("url"),
            getattr(scraper, "provider", "unknown"),
        )
        return {
            "sourceUrl": site.get("url"),
            "items": {"normalized": [], "failed": []},
            "error": "timeout",
            "errorType": "TimeoutError",
        }

    if not persist_scrape:
        return result

    if not isinstance(result, dict):
        raise ApplicationError("Scrape payload missing/invalid", non_retryable=True)

    result = _apply_workflow_context(result, workflow_context, site)
    scrape_id = store_scrape(result)
    return {
        "scrapeId": scrape_id,
        "summary": _summarize_scrape_payload(result),
        "recoveryPayload": _build_recovery_payload(result, site),
    }


@activity.defn
async def start_firecrawl_webhook_scrape(  # deprecated, mixed async+convex
    site: Site,
) -> Dict[str, Any]:
    """Kick off a Firecrawl batch scrape with a Convex webhook callback.

    .. deprecated::
        Use step functions from ``job_scrape_application.workflows.activities.step`` instead.
    """

    site_type = site.get("type") or "general"
    kind = (
        FirecrawlJobKind.GREENHOUSE_LISTING
        if site_type == "greenhouse"
        else FirecrawlJobKind.SITE_CRAWL
    )
    logger.info(
        "start_firecrawl_webhook_scrape site=%s type=%s use_mock=%s mock_convex=%s",
        site.get("url"),
        site_type,
        _should_use_mock_firecrawl(site.get("url")),
        _should_mock_convex_webhooks(),
    )

    webhook_dict = _build_firecrawl_webhook(site, kind)
    site_url = site.get("url")
    if site_url:
        metadata_block = webhook_dict.setdefault("metadata", {})
        metadata_block.setdefault("urls", [site_url])

    webhook_metadata_raw = webhook_dict.get("metadata") or {}
    webhook_dict["metadata"] = _stringify_firecrawl_metadata(webhook_metadata_raw)

    webhook_model = _WebhookModel(webhook_dict)
    webhook_payload: Dict[str, Any] = webhook_model.model_dump(exclude_none=True)

    if _should_use_mock_firecrawl(site.get("url")):
        from ....testing.firecrawl_mock import MockFirecrawl

        mock_client = MockFirecrawl()
        logger.info("firecrawl.start mock client path site=%s", site.get("url"))
        provider_request = {
            "urls": [site.get("url")],
            "webhook": webhook_payload,
            "kind": kind,
        }
        job = mock_client.start_batch_scrape([site["url"]], webhook=webhook_model)
        raw_start: dict[str, Any]
        if hasattr(job, "model_dump"):
            raw_start = job.model_dump(mode="json", exclude_none=True)  # type: ignore[union-attr]
        else:
            raw_start = {
                "jobId": getattr(job, "jobId", None),
                "statusUrl": getattr(job, "statusUrl", None),
                "status": "queued",
                "kind": kind,
                "mock": True,
            }
        payload = _serialize_firecrawl_job(job, site, webhook_payload, kind)
        payload["metadata"] = webhook_payload.get("metadata")
        payload["receivedAt"] = int(time.time() * 1000)
        payload["rawStart"] = raw_start
        payload["providerRequest"] = provider_request
        payload["request"] = _build_request_snapshot(
            provider_request,
            provider="firecrawl_mock",
            method="POST",
            url="mock://firecrawl/batch",
        )
        payload["webhookId"] = await _record_pending_firecrawl_webhook(
            payload, site, webhook_payload, kind
        )
        _log_provider_dispatch(
            "firecrawl_mock",
            site["url"],
            kind=kind,
            webhook=webhook_payload.get("url"),
            siteId=site.get("_id"),
            pattern=site.get("pattern"),
        )
        _log_sync_response(
            "firecrawl_mock",
            action="start",
            url=site["url"],
            job_id=payload.get("jobId"),
            status_url=payload.get("statusUrl") or f"mock://firecrawl/status/{payload.get('jobId')}",
            kind=kind,
            summary="mock start (example.com)",
            metadata={
                "siteId": site.get("_id"),
                "webhook": webhook_payload.get("url"),
                "pattern": site.get("pattern"),
            },
        )
        return payload

    firecrawl_api_key = settings.firecrawl_api_key
    if not firecrawl_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY env var is required for Firecrawl",
            non_retryable=True,
        )

    if site_type == "greenhouse":
        # Ask Firecrawl to return the full Greenhouse board JSON so we can parse jobs reliably
        json_format = {
            "type": "json",
            "prompt": "Return the full Greenhouse board JSON payload (jobs array and metadata) with no summary.",
            "schema": {
                "type": "object",
                "properties": {
                    "jobs": {"type": "array", "items": {"type": "object"}},
                },
                "required": ["jobs"],
                "additionalProperties": True,
            },
        }

        client = Firecrawl(api_key=firecrawl_api_key)
        provider_request = {
            "urls": [site["url"]],
            "options": {
                "formats": [json_format],
                "proxy": "auto",
                "max_age": FIRECRAWL_CACHE_MAX_AGE_MS,
                "store_in_cache": True,
            },
            "webhook": webhook_payload,
        }

        def _do_start_batch(webhook_arg: Any) -> Any:
            return client.start_batch_scrape(
                [site["url"]],
                formats=[json_format],
                webhook=webhook_arg,
                proxy="auto",
                max_age=FIRECRAWL_CACHE_MAX_AGE_MS,
                store_in_cache=True,
            )

        logger.info("firecrawl.start real client begin site=%s kind=%s", site.get("url"), kind)
        _log_provider_dispatch(
            "firecrawl",
            site["url"],
            kind=FirecrawlJobKind.GREENHOUSE_LISTING,
            webhook=webhook_payload.get("url"),
            siteId=site.get("_id"),
        )
        request_snapshot = _build_request_snapshot(
            provider_request,
            provider="firecrawl",
            method="POST",
            url="https://api.firecrawl.dev/v2/batch/scrape",
        )
        try:
            job = await _start_firecrawl_batch(_do_start_batch, webhook_model, webhook_payload)
        except Exception as exc:  # noqa: BLE001
            logger.exception("firecrawl.start greenhouse failed site=%s exc=%s", site.get("url"), exc)
            error_payload: ScrapeErrorInput = {
                "sourceUrl": site.get("url"),
                "event": "start_batch_scrape",
                "error": str(exc),
                "metadata": {"kind": FirecrawlJobKind.GREENHOUSE_LISTING},
            }
            site_id = site.get("_id")
            if site_id is not None:
                error_payload["siteId"] = site_id
            if not _should_mock_convex_webhooks():
                _log_scrape_error(error_payload)
            msg = str(exc).lower()
            retryable = "429" in msg or "rate" in msg or "timeout" in msg
            raise ApplicationError(f"Firecrawl batch start failed: {exc}", non_retryable=not retryable) from exc

        raw_start = (
            job.model_dump(mode="json", exclude_none=True)
            if hasattr(job, "model_dump")
            else job
        )
        payload = _serialize_firecrawl_job(
            job, site, webhook_payload, FirecrawlJobKind.GREENHOUSE_LISTING
        )
        payload["metadata"] = webhook_payload.get("metadata")
        payload["receivedAt"] = int(time.time() * 1000)
        payload["rawStart"] = raw_start
        payload["providerRequest"] = provider_request
        payload["request"] = request_snapshot
        payload["webhookId"] = await _record_pending_firecrawl_webhook(
            payload, site, webhook_payload, FirecrawlJobKind.GREENHOUSE_LISTING
        )
        _log_sync_response(
            "firecrawl",
            action="start",
            url=site["url"],
            job_id=payload.get("jobId"),
            status_url=_build_provider_status_url(
                "firecrawl",
                payload.get("jobId"),
                status_url=payload.get("statusUrl"),
                kind=FirecrawlJobKind.GREENHOUSE_LISTING,
            ),
            kind=FirecrawlJobKind.GREENHOUSE_LISTING,
            summary="greenhouse batch started",
            metadata={
                "siteId": site.get("_id"),
                "webhook": webhook_payload.get("url"),
                "jobs": len(raw_start.get("jobs", [])) if isinstance(raw_start, dict) else None,
                "startStatus": (raw_start.get("status") or raw_start.get("state")) if isinstance(raw_start, dict) else None,
            },
        )
        return payload

    pattern = site.get("pattern")
    job_schema = build_firecrawl_schema()
    scrape_formats: List[Any] = [
        "markdown",
        {"type": "json", "schema": job_schema},
    ]

    client = Firecrawl(api_key=firecrawl_api_key)

    provider_request = {
        "urls": [site["url"]],
        "options": {
            "formats": scrape_formats,
            "only_main_content": True,
            "proxy": "auto",
            "max_age": FIRECRAWL_CACHE_MAX_AGE_MS,
            "store_in_cache": True,
        },
        "webhook": webhook_payload,
        "ignore_invalid_urls": True,
    }

    def _do_start_batch_crawl(webhook_arg: Any) -> Any:
        return client.start_batch_scrape(
            [site["url"]],
            formats=scrape_formats,
            only_main_content=True,
            proxy="auto",
            max_age=FIRECRAWL_CACHE_MAX_AGE_MS,
            store_in_cache=True,
            webhook=webhook_arg,
            ignore_invalid_urls=True,
        )

    logger.info("firecrawl.start real client begin site=%s kind=%s", site.get("url"), kind)
    _log_provider_dispatch(
        "firecrawl",
        site["url"],
        kind=FirecrawlJobKind.SITE_CRAWL,
        webhook=webhook_payload.get("url"),
        siteId=site.get("_id"),
        pattern=pattern,
    )
    try:
        job = await _start_firecrawl_batch(_do_start_batch_crawl, webhook_model, webhook_payload)
    except Exception as exc:  # noqa: BLE001
        logger.exception("firecrawl.start site_crawl failed site=%s exc=%s", site.get("url"), exc)
        error_payload: ScrapeErrorInput = {
            "sourceUrl": site.get("url"),
            "event": "start_batch_scrape",
            "error": str(exc),
            "metadata": {"pattern": pattern},
        }
        site_id = site.get("_id")
        if site_id is not None:
            error_payload["siteId"] = site_id
        if not _should_mock_convex_webhooks():
            _log_scrape_error(error_payload)
        msg = str(exc).lower()
        retryable = "429" in msg or "rate" in msg or "timeout" in msg
        raise ApplicationError(f"Firecrawl batch start failed: {exc}", non_retryable=not retryable) from exc

    raw_start = (
        job.model_dump(mode="json", exclude_none=True)
        if hasattr(job, "model_dump")
        else job
    )
    payload = _serialize_firecrawl_job(job, site, webhook_payload, FirecrawlJobKind.SITE_CRAWL)
    payload["metadata"] = webhook_payload.get("metadata")
    payload["receivedAt"] = int(time.time() * 1000)
    payload["rawStart"] = raw_start
    payload["providerRequest"] = provider_request
    payload["request"] = _build_request_snapshot(
        provider_request,
        provider="firecrawl",
        method="POST",
        url="https://api.firecrawl.dev/v2/batch/scrape",
    )
    payload["webhookId"] = await _record_pending_firecrawl_webhook(
        payload, site, webhook_payload, FirecrawlJobKind.SITE_CRAWL
    )
    _log_sync_response(
        "firecrawl",
        action="start",
        url=site["url"],
        job_id=payload.get("jobId"),
        status_url=_build_provider_status_url(
            "firecrawl",
            payload.get("jobId"),
            status_url=payload.get("statusUrl"),
            kind=FirecrawlJobKind.SITE_CRAWL,
        ),
        kind=FirecrawlJobKind.SITE_CRAWL,
        summary="batch queued",
        metadata={
            "siteId": site.get("_id"),
            "webhook": webhook_payload.get("url"),
            "pattern": pattern,
            "startStatus": (raw_start.get("status") or raw_start.get("state")) if isinstance(raw_start, dict) else None,
        },
    )
    return payload


@activity.defn
async def crawl_site_fetchfox(  # deprecated, mixed async+convex
    site: Site,
    workflow_context: Dict[str, Any] | None = None,
    persist_scrape: bool = False,
) -> Dict[str, Any]:
    """Use FetchFox crawl to queue job detail URLs for SpiderCloud extraction.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """

    if not settings.fetchfox_api_key:
        raise ApplicationError("FETCHFOX_API_KEY env var is required for FetchFox", non_retryable=True)

    source_url = site.get("url") or ""
    if not source_url:
        raise ApplicationError("Site URL is required for FetchFox crawl", non_retryable=True)
    pattern = site.get("pattern")
    start_urls = [source_url] if source_url else []
    site_id = _convex_site_id(site)

    skip_urls: list[str] = []
    try:
        if source_url:
            skip_urls = fetch_seen_urls_for_site(source_url, pattern)
    except Exception:
        skip_urls = []

    queued_urls: list[str] = []
    try:
        now_ms = int(time.time() * 1000)
        processing_expiry_ms = runtime_config.spidercloud_job_details_processing_expire_minutes * 60 * 1000
        per_status_limit = 250
        for status_value in ("pending", "processing"):
            queued_rows = dbos_queue.list_scrape_urls(
                **_strip_none_values(
                    {
                        "site_id": site_id,
                        "provider": "spidercloud",
                        "status": status_value,
                        "limit": per_status_limit,
                    }
                )
            )
            if isinstance(queued_rows, list):
                for row in queued_rows:
                    if isinstance(row, dict):
                        url_val = row.get("url")
                        if isinstance(url_val, str) and url_val.strip():
                            if status_value == "processing":
                                updated_at = row.get("updatedAt")
                                if isinstance(updated_at, (int, float)):
                                    if updated_at < now_ms - processing_expiry_ms:
                                        continue
                                else:
                                    continue
                            queued_urls.append(url_val.strip())
    except Exception:
        queued_urls = []

    skip_set = {u for u in skip_urls if isinstance(u, str)}
    skip_set.update(u for u in queued_urls if isinstance(u, str))

    priority = FetchFoxPriority(skip=list(skip_set))
    crawl_request = {
        "pattern": pattern,
        "start_urls": start_urls,
        "max_depth": 5,
        "max_visits": MAX_FETCHFOX_VISITS,
        "priority": priority.model_dump(exclude_none=True),
    }
    request_snapshot = _build_request_snapshot(
        crawl_request,
        provider="fetchfox",
        method="POST",
        url="https://api.fetchfox.ai/crawl",
    )

    _log_provider_dispatch(
        "fetchfox",
        source_url,
        pattern=pattern,
        siteId=site.get("_id"),
        kind="crawl",
    )

    started_at = int(time.time() * 1000)
    try:
        fox = FetchFox(api_key=settings.fetchfox_api_key)
        result = await asyncio.to_thread(fox.crawl, crawl_request)
        result_obj: Dict[str, Any] | Any = result if isinstance(result, dict) else orjson.loads(result)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"FetchFox crawl failed: {exc}") from exc
    completed_at = int(time.time() * 1000)

    def _collect_urls(value: Any, acc: list[str]) -> None:
        if isinstance(value, str):
            if value.startswith("http"):
                acc.append(value.strip())
            return
        if isinstance(value, list):
            for item in value:
                _collect_urls(item, acc)
            return
        if isinstance(value, dict):
            for key in ("url", "href", "link", "target", "job_url", "absolute_url"):
                url_val = value.get(key)
                if isinstance(url_val, str):
                    _collect_urls(url_val, acc)
            for key in ("urls", "links", "visited_urls", "visitedUrls", "job_urls", "jobUrls", "results", "items", "data", "hits"):
                nested = value.get(key)
                if nested is not None:
                    _collect_urls(nested, acc)

    crawled_urls: list[str] = []
    _collect_urls(result_obj, crawled_urls)
    for row in normalize_fetchfox_items(result_obj):
        if isinstance(row, dict):
            url_val = row.get("url")
            if isinstance(url_val, str) and url_val.strip():
                crawled_urls.append(url_val.strip())

    # Deduplicate while preserving order
    seen_urls: set[str] = set()
    unique_urls: list[str] = []
    for url_val in crawled_urls:
        if not isinstance(url_val, str):
            continue
        cleaned = url_val.strip()
        if not cleaned or not cleaned.startswith("http"):
            continue
        if cleaned in seen_urls:
            continue
        seen_urls.add(cleaned)
        unique_urls.append(cleaned)

    # Use filter_new_job_urls for efficiency - returns only non-existing URLs (less network transfer)
    new_urls: list[str] = []
    try:
        new_urls = filter_new_job_urls(unique_urls)
    except Exception:
        new_urls = unique_urls  # On error, proceed with all URLs

    new_urls_set = set(new_urls)
    candidates = [u for u in unique_urls if u not in skip_set and u in new_urls_set]
    handler = get_site_handler(source_url) if isinstance(source_url, str) and source_url else None
    if candidates:
        filtered_candidates: list[str] = []
        for url in candidates:
            if handler:
                if handler.is_listing_url(url):
                    continue
            elif _is_probable_listing_url(url):
                continue
            filtered_candidates.append(url)
        candidates = filtered_candidates
    enqueued: list[str] = []
    if candidates:
        url_types = ["detail"] * len(candidates)
        try:
            res = dbos_queue.enqueue_scrape_urls(
                _strip_none_values(
                    {
                        "urls": candidates,
                        "sourceUrl": source_url,
                        "provider": "spidercloud",
                        "siteId": site_id,
                        "pattern": pattern,
                        "urlTypes": url_types,
                    }
                )
            )
            if isinstance(res, dict):
                queued = res.get("queued")
                if isinstance(queued, int):
                    enqueued = candidates[:queued]
        except Exception:
            enqueued = []

    skipped_urls = [u for u in unique_urls if u in skip_set or u not in new_urls_set]

    _log_sync_response(
        "fetchfox",
        action="crawl",
        url=source_url,
        kind="site_crawl",
        summary=f"urls={len(candidates)} queued={len(enqueued)}",
        metadata={
            "siteId": site.get("_id"),
            "pattern": pattern,
            "queueProvider": "spidercloud",
            "rawUrlCount": len(crawled_urls),
        },
        response=_shrink_payload(result_obj, 20000),
    )

    payload = {
        "provider": "fetchfox-crawl",
        "workflowName": "FetchfoxSpidercloud",
        "sourceUrl": source_url,
        "pattern": pattern,
        "startedAt": started_at,
        "completedAt": completed_at,
        "request": request_snapshot,
        "providerRequest": crawl_request,
        "items": {
            "provider": "spidercloud",
            "crawlProvider": "fetchfox",
            "job_urls": candidates,
            "rawUrls": unique_urls,
            "queued": bool(enqueued),
            "queuedCount": len(enqueued),
            "existing": list(skip_set),
            "request": request_snapshot,
            "seedUrls": start_urls,
        },
        "skippedUrls": skipped_urls,
        "response": {
            "queued": len(enqueued),
            "urls": unique_urls[:25],
            "totalUrls": len(unique_urls),
            "rawResponse": _shrink_payload(result_obj, 20000),
        },
    }

    if not persist_scrape:
        return payload

    payload = _apply_workflow_context(payload, workflow_context, site)
    scrape_id = store_scrape(payload)
    return {
        "scrapeId": scrape_id,
        "summary": _summarize_scrape_payload(payload),
    }


@activity.defn
async def scrape_site_fetchfox(site: Site) -> Dict[str, Any]:
    """Scrape a site using FetchFox.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """
    scraper = _build_fetchfox_scraper(
        build_request_snapshot=_build_request_snapshot,
        log_provider_dispatch=_log_provider_dispatch,
        log_sync_response=_log_sync_response,
    )
    return await scraper.scrape_site(site)


@activity.defn
async def scrape_site_firecrawl(  # deprecated, mixed async+convex
    site: Site,
    skip_urls: Optional[List[str]] = None,
    workflow_context: Dict[str, Any] | None = None,
    persist_scrape: bool = False,
) -> Dict[str, Any]:
    """Scrape a site using Firecrawl.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """
    scraper = _make_firecrawl_scraper()
    result = await scraper.scrape_site(site, skip_urls=skip_urls)
    if not persist_scrape:
        return result
    if not isinstance(result, dict):
        raise ApplicationError("Scrape payload missing/invalid", non_retryable=True)
    result = _apply_workflow_context(result, workflow_context, site)
    scrape_id = store_scrape(result)
    return {
        "scrapeId": scrape_id,
        "summary": _summarize_scrape_payload(result),
        "recoveryPayload": _build_recovery_payload(result, site),
    }


@activity.defn
async def fetch_greenhouse_listing(site: Site) -> Dict[str, Any]:
    """Fetch a Greenhouse job listing.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """
    scraper, _ = select_scraper_for_site(site)
    return await scraper.fetch_greenhouse_listing(site)


@activity.defn
async def fetch_greenhouse_listing_firecrawl(site: Site) -> Dict[str, Any]:
    """Fetch a Greenhouse job listing using Firecrawl.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """
    scraper = _make_firecrawl_scraper()
    return await scraper.fetch_greenhouse_listing(site)


@activity.defn
async def scrape_greenhouse_jobs(
    payload: Dict[str, Any],
    workflow_context: Dict[str, Any] | None = None,
    persist_scrape: bool = False,
) -> Dict[str, Any]:
    """Scrape new Greenhouse job URLs with a single FetchFox request.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """

    idempotency_key = payload.get("idempotency_key") or payload.get("webhook_id")
    if settings.spider_api_key and not idempotency_key:
        scraper = _make_spidercloud_scraper()
    elif settings.firecrawl_api_key:
        scraper = _make_firecrawl_scraper()
    else:
        scraper = _build_fetchfox_scraper(
            build_request_snapshot=_build_request_snapshot,
            log_provider_dispatch=_log_provider_dispatch,
            log_sync_response=_log_sync_response,
        )
    result = await scraper.scrape_greenhouse_jobs(payload)
    if not persist_scrape:
        return result

    scrape_payload = result.get("scrape") if isinstance(result, dict) else None
    if not isinstance(scrape_payload, dict):
        raise ApplicationError("Greenhouse scrape payload missing/invalid", non_retryable=True)

    scrape_payload = _apply_workflow_context(scrape_payload, workflow_context, None)
    scrape_id = store_scrape(scrape_payload)
    jobs_scraped = result.get("jobsScraped") if isinstance(result, dict) else None
    return {
        "scrapeId": scrape_id,
        "jobsScraped": jobs_scraped,
        "summary": _summarize_scrape_payload(scrape_payload),
    }


@activity.defn
async def scrape_greenhouse_jobs_firecrawl(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Scrape Greenhouse jobs using Firecrawl.

    .. deprecated::
        Use workflows from ``job_scrape_application.workflows.workflow`` instead.
    """
    scraper = _make_firecrawl_scraper()
    return await scraper.scrape_greenhouse_jobs(payload)


@activity.defn
def fetch_pending_firecrawl_webhooks(limit: int = 25, event: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return unprocessed Firecrawl webhook rows from Convex.

    .. deprecated::
        Use step functions from ``job_scrape_application.workflows.activities.step`` instead.
    """
    return fetch_pending_firecrawl_webhooks_step(limit=limit, event=event)


@activity.defn
def get_firecrawl_webhook_status(job_id: str) -> Dict[str, Any]:
    """Return the current Convex state for a Firecrawl job's webhook rows.

    .. deprecated::
        Use step functions from ``job_scrape_application.workflows.activities.step`` instead.
    """
    return get_firecrawl_webhook_status_step(job_id)


@activity.defn
def mark_firecrawl_webhook_processed(webhook_id: str, error: Optional[str] = None) -> None:
    """Mark a webhook row as processed and optionally attach an error.

    .. deprecated::
        Use step functions from ``job_scrape_application.workflows.activities.step`` instead.
    """
    mark_firecrawl_webhook_processed_step(webhook_id=webhook_id, error=error)


__all__ = [
    # Activity functions (defined in this module)
    "_scrape_spidercloud_greenhouse",
    "scrape_site",
    "start_firecrawl_webhook_scrape",
    "crawl_site_fetchfox",
    "scrape_site_fetchfox",
    "scrape_site_firecrawl",
    "fetch_greenhouse_listing",
    "fetch_greenhouse_listing_firecrawl",
    "scrape_greenhouse_jobs",
    "scrape_greenhouse_jobs_firecrawl",
    "fetch_pending_firecrawl_webhooks",
    "get_firecrawl_webhook_status",
    "mark_firecrawl_webhook_processed",
    # Large activity functions (imported from temporal_activities.py)
    "process_spidercloud_job_batch",
    "process_spidercloud_listing_batch",
    "collect_firecrawl_job_result",
    "store_scrape",
    "process_pending_job_details_batch",
    "batch_store_scrapes_background",
    # Helper functions
    "select_scraper_for_site",
    "_build_listing_zero_url_context",
    "_get_activity_worker_id",
    "_store_job_descriptions_via_http",
    "_activity_cancellation_payload",
    "_summarize_scrape_payload",
    "_apply_workflow_context",
    "_build_recovery_payload",
    "_make_fetchfox_scraper",
    "_make_firecrawl_scraper",
    "_make_spidercloud_scraper",
    "_extract_job_urls_from_scrape",
]
