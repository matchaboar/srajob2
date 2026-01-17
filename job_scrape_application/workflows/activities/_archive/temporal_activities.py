"""Deprecated Temporal activity functions.

These activity functions are kept for backward compatibility but should not be used in new code.
Use workflows from job_scrape_application.workflows.workflow and step functions from
job_scrape_application.workflows.activities.step instead.
"""

from __future__ import annotations

import asyncio
import inspect
import orjson
import logging
import re
import time
from html.parser import HTMLParser
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse, urljoin

from firecrawl import Firecrawl
from firecrawl.v2.types import PaginationConfig
from fetchfox_sdk import FetchFox
from temporalio import activity
from temporalio.exceptions import ApplicationError

from ....config import settings, runtime_config
from ....components.models import (
    FetchFoxPriority,
    GreenhouseBoardResponse,
    MAX_FETCHFOX_VISITS,
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)
from ....constants import (
    title_matches_required_keywords,
)
from ....dbos_runtime import queue as dbos_queue
from ....services import telemetry

from ...helpers.firecrawl import (
    build_firecrawl_webhook as _build_firecrawl_webhook,
    extract_first_json_doc as _extract_first_json_doc,
    extract_first_text_doc as _extract_first_text_doc,
    metadata_urls_to_list as _metadata_urls_to_list,
    should_mock_convex_webhooks as _should_mock_convex_webhooks,
    should_use_mock_firecrawl as _should_use_mock_firecrawl,
    stringify_firecrawl_metadata as _stringify_firecrawl_metadata,
)
from ...helpers.provider import (
    build_provider_status_url as _build_provider_status_url,
    build_request_snapshot as _build_request_snapshot,
    log_provider_dispatch as _log_provider_dispatch,
    log_sync_response as _log_sync_response,
    mask_secret as _mask_secret,
    sanitize_headers as _sanitize_headers,
)
from ...helpers.scrape_utils import (
    _jobs_from_scrape_items,
    _shrink_payload,
    build_description_preview,
    build_firecrawl_schema,
    derive_company_from_url,
    fetch_seen_urls_for_site,
    normalize_fetchfox_items,
    normalize_firecrawl_items,
    parse_posted_at,
    trim_scrape_for_convex,
    looks_like_truncated_description,
)
from ...helpers.page_detection import is_invalid_job_url
from ...helpers.url_handling import _strip_ashby_application_url
from ...helpers.link_extractors import (
    gather_strings,
    extract_job_urls_from_json_payload,
    extract_links_from_payload,
    normalize_url,
    strip_wrapping_url,
)
from ...helpers.regex_patterns import (
    APPLY_WORD_PATTERN,
    ASHBY_JOB_SLUG_PATTERN,
    CODE_FENCE_CONTENT_PATTERN,
    CODE_FENCE_END_PATTERN,
    CODE_FENCE_START_PATTERN,
    CONFLUENT_JOB_PATH_PATTERN,
    DIGIT_PATTERN,
    GREENHOUSE_BOARDS_PATH_PATTERN,
    GREENHOUSE_URL_PATTERN,
    INVALID_JSON_ESCAPE_PATTERN,
    JOB_ID_PATH_PATTERN,
    LOCATION_LINE_PATTERN,
    MARKDOWN_LINK_PATTERN,
    TITLE_IN_BAR_PATTERN,
    TITLE_LOCATION_PAREN_PATTERN,
    URL_PATTERN,
)
from ...scrapers import BaseScraper, FetchfoxScraper, FirecrawlScraper, SpiderCloudScraper
from ...site_handlers import get_site_handler
from ...site_handlers.base import BaseSiteHandler
from ...normalizers.pipeline import build_job_update as _build_job_detail_heuristic_patch

from ..constants import (
    FIRECRAWL_CACHE_MAX_AGE_MS,
    FIRECRAWL_STATUS_EXPIRATION_MS,
    FIRECRAWL_STATUS_WARN_MS,
    FirecrawlJobKind,
)
from ..errors import ScrapeErrorInput
from ..step import log_scrape_error as _log_scrape_error
# Import factories module (not individual functions) so tests can patch factories._make_*
from .. import factories as _factories
from ..firecrawl import (
    WebhookModel as _WebhookModel,
    mock_firecrawl_status_response as _mock_firecrawl_status_response,
    record_pending_firecrawl_webhook as _record_pending_firecrawl_webhook,
    serialize_firecrawl_job as _serialize_firecrawl_job,
    start_firecrawl_batch as _start_firecrawl_batch,
)
from ..types import FirecrawlWebhookEvent, Site
from ..step import (
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
    record_job_detail_heuristic_step,
    record_scrape_url_attempts as _record_scrape_url_attempts,
    resolve_pagination_limit_step,
    store_job_description_step,
)
from ..url_processing import (
    _is_base_listing_page,
    _looks_like_auth_url,
    _is_probable_listing_url,
    _filter_job_urls,
    _classify_filtered_urls,
)
from ..heuristics import (
    _describe_exception,
    _extract_request_id,
    _extract_pending_count,
    _domain_from_url,
)

# Import from other archive modules for helper functions
from .site_management import _strip_none_values
from .convex_operations import _convex_site_id, _convex_http_base_url
from .logging_activities import _build_log_message

# Constants
DEFAULT_PAGINATION_LIMIT = 0
PAGINATION_ENQUEUE_STAGGER_MS = 30_000
SCRAPE_URL_QUEUE_TTL_MS = 48 * 60 * 60 * 1000
SCRAPE_URL_QUEUE_MAX_ATTEMPTS = 3
SPIDERCLOUD_BATCH_SIZE = runtime_config.spidercloud_job_details_batch_size
SCRAPE_URL_QUEUE_LIST_LIMIT = 500
TEMPORAL_PAYLOAD_MAX_CHARS = 10 * 1024 * 1024
SPIDERCLOUD_ACTIVITY_PAYLOAD_MAX_CHARS = 64_000

logger = logging.getLogger("temporal.worker.activities")
scheduling_logger = logging.getLogger("temporal.scheduler")

__all__ = [
    # Private activity
    "_scrape_spidercloud_greenhouse",
    # Public activities
    "scrape_site",
    "start_firecrawl_webhook_scrape",
    "crawl_site_fetchfox",
    "scrape_site_fetchfox",
    "scrape_site_firecrawl",
    "fetch_greenhouse_listing",
    "fetch_greenhouse_listing_firecrawl",
    "process_spidercloud_job_batch",
    "process_spidercloud_listing_batch",
    "scrape_greenhouse_jobs",
    "scrape_greenhouse_jobs_firecrawl",
    "fetch_pending_firecrawl_webhooks",
    "get_firecrawl_webhook_status",
    "mark_firecrawl_webhook_processed",
    "collect_firecrawl_job_result",
    "store_scrape",
    "process_pending_job_details_batch",
    "batch_store_scrapes_background",
    # Helper functions used by other modules
    "_extract_job_urls_from_scrape",
    "select_scraper_for_site",
]


# ============================================================================
# Helper functions used by activities
# ============================================================================

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
    import os
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


# ============================================================================
# Factory wrappers - delegate to factories module so tests can patch correctly
# ============================================================================


def _make_fetchfox_scraper() -> FetchfoxScraper:
    """Delegate to factories module for test patchability."""
    return _factories._make_fetchfox_scraper()


def _make_firecrawl_scraper() -> FirecrawlScraper:
    """Delegate to factories module for test patchability."""
    return _factories._make_firecrawl_scraper()


def _make_spidercloud_scraper() -> SpiderCloudScraper:
    """Delegate to factories module for test patchability."""
    return _factories._make_spidercloud_scraper()


def select_scraper_for_site(site: Site) -> tuple[BaseScraper, Optional[List[str]]]:
    """Delegate to factories module for test patchability."""
    return _factories.select_scraper_for_site_with_defaults(site)


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


# ============================================================================
# Activity definitions
# ============================================================================

@activity.defn
async def _scrape_spidercloud_greenhouse(  # noqa: DBOS004 - deprecated, mixed async+convex
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
    """Scrape a site, selecting provider based on per-site preference."""

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
async def start_firecrawl_webhook_scrape(  # noqa: DBOS004 - deprecated, mixed async+convex
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
        from ...testing.firecrawl_mock import MockFirecrawl

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
async def crawl_site_fetchfox(  # noqa: DBOS004 - deprecated, mixed async+convex
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
    scraper = _factories.build_fetchfox_scraper(
        build_request_snapshot=_build_request_snapshot,
        log_provider_dispatch=_log_provider_dispatch,
        log_sync_response=_log_sync_response,
    )
    return await scraper.scrape_site(site)


@activity.defn
async def scrape_site_firecrawl(  # noqa: DBOS004 - deprecated, mixed async+convex
    site: Site,
    skip_urls: Optional[List[str]] = None,
    workflow_context: Dict[str, Any] | None = None,
    persist_scrape: bool = False,
) -> Dict[str, Any]:
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
    scraper, _ = select_scraper_for_site(site)
    return await scraper.fetch_greenhouse_listing(site)


@activity.defn
async def fetch_greenhouse_listing_firecrawl(site: Site) -> Dict[str, Any]:
    scraper = _make_firecrawl_scraper()
    return await scraper.fetch_greenhouse_listing(site)

@activity.defn
async def process_spidercloud_job_batch(  # noqa: DBOS004 - deprecated, use scrape_job_detail_batch workflow
    batch: Dict[str, Any],
    persist_scrapes: bool = False,
) -> Dict[str, Any]:
    """Process a batch of job URLs via SpiderCloud.

    .. deprecated::
        Use :func:`scrape_job_detail_batch` workflow from
        ``job_scrape_application.workflows.workflow`` instead.
    """

    def _extract_greenhouse_job_id(url: str) -> str | None:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        query = parse_qs(parsed.query)
        gh_jid = query.get("gh_jid", [])
        if gh_jid and isinstance(gh_jid[0], str) and gh_jid[0].strip():
            return gh_jid[0].strip()
        match = re.search(JOB_ID_PATH_PATTERN, parsed.path)
        if match:
            return match.group(1)
        return None

    def _extract_greenhouse_slug(url: str | None) -> str | None:
        if not isinstance(url, str) or not url.strip():
            return None
        match = re.search(GREENHOUSE_BOARDS_PATH_PATTERN, url)
        if match:
            return match.group(1)
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        query = parse_qs(parsed.query)
        board_param = query.get("board")
        if board_param and isinstance(board_param[0], str):
            candidate = board_param[0].strip()
            if candidate:
                return candidate
        host = (parsed.hostname or "").lower()
        parts = [p for p in parsed.path.split("/") if p]
        if host.endswith("boards.greenhouse.io") and parts:
            return parts[0]
        if "boards" in parts:
            idx = parts.index("boards")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        if len(parts) >= 2 and parts[1] == "jobs":
            return parts[0]
        return None

    def _to_greenhouse_api_url(url: str, source_url: str | None = None) -> str:
        """Prefer Greenhouse API detail URLs when available."""
        handler = get_site_handler(url) or (get_site_handler(source_url) if source_url else None)
        if not handler or handler.name != "greenhouse":
            return url
        if handler.is_api_detail_url(url):
            return url
        api_url = handler.get_api_uri(url)
        if api_url:
            return api_url
        return url

    def _shrink_for_activity(scrape: Dict[str, Any]) -> Dict[str, Any]:
        """
        Trim scrape payloads before returning them to the workflow to avoid blowing
        Temporal's activity result size limits. We keep enough data for downstream
        storage/ingestion while aggressively truncating large fields.
        """
        trimmed = trim_scrape_for_convex(
            scrape,
            max_items=50,  # we only ever keep one normalized row per scrape here
            max_description=SPIDERCLOUD_ACTIVITY_PAYLOAD_MAX_CHARS,
            raw_preview_chars=SPIDERCLOUD_ACTIVITY_PAYLOAD_MAX_CHARS,
            request_max_chars=SPIDERCLOUD_ACTIVITY_PAYLOAD_MAX_CHARS,
        )
        items = trimmed.get("items")
        if isinstance(items, dict):
            normalized_sample = items.get("normalizedSample")
            normalized = items.get("normalized")
            normalized_count = items.get("normalizedCount")
            if isinstance(normalized_sample, list) and normalized_sample:
                if normalized_count == 1 or (isinstance(normalized, list) and len(normalized) == 1):
                    items["normalized"] = normalized_sample
        return trimmed

    groups: dict[tuple[str, str | None], list[str]] = {}
    posted_at_groups: dict[tuple[str, str | None], Dict[str, int]] = {}
    # Track siteId by (source_url, pattern) key for dedup recording
    site_id_by_group: dict[tuple[str, str | None], str] = {}
    source_url_hint = ""
    attempt_entries: list[dict[str, Any]] = []
    for row in batch.get("urls", []):
        if not isinstance(row, dict):
            continue
        url_val = row.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            continue
        url_type_raw = row.get("urlType")
        url_type = url_type_raw if isinstance(url_type_raw, str) else None
        if url_type and url_type.lower() == "listing":
            continue
        source_val_raw = row.get("sourceUrl")
        source_val: str = source_val_raw if isinstance(source_val_raw, str) else ""
        pattern_val_raw = row.get("pattern")
        pattern_val: str | None = pattern_val_raw if isinstance(pattern_val_raw, str) else None
        if source_val and not source_url_hint:
            source_url_hint = source_val
        handler = get_site_handler(url_val) or (get_site_handler(source_val) if source_val else None)
        if handler and handler.is_listing_url(url_val):
            continue
        if not handler and _is_probable_listing_url(url_val):
            continue
        key = (source_val, pattern_val)
        normalized_url = _to_greenhouse_api_url(url_val, source_val)
        groups.setdefault(key, []).append(normalized_url)
        attempt_entries.append(
            {
                "url": normalized_url,
                "sourceUrl": source_val,
                "provider": row.get("provider") if isinstance(row.get("provider"), str) else None,
                "attempts": row.get("attempts"),
            }
        )
        posted_at_val = row.get("postedAt")
        if isinstance(posted_at_val, (int, float)):
            mapping = posted_at_groups.setdefault(key, {})
            mapping[normalize_url(normalized_url) or normalized_url] = int(posted_at_val)
        # Track siteId for this group (used for seen_job_urls recording)
        site_id_val = row.get("siteId")
        if isinstance(site_id_val, str) and site_id_val.strip() and key not in site_id_by_group:
            site_id_by_group[key] = site_id_val.strip()

    _record_scrape_url_attempts(attempt_entries)

    if not groups:
        response = {"provider": "spidercloud", "items": {"normalized": []}, "sourceUrl": source_url_hint}
        if persist_scrapes:
            response.update({"scrapeIds": [], "stored": 0, "invalid": 0, "failed": 0})
        return response

    # Filter out URLs that already exist in Convex to avoid wasting SpiderCloud credits
    all_urls_to_check: list[str] = []
    for urls in groups.values():
        all_urls_to_check.extend(urls)

    new_urls_set: set[str] = set()
    skipped_existing_count = 0
    if all_urls_to_check:
        try:
            new_urls = filter_new_job_urls(all_urls_to_check)
            new_urls_set = set(new_urls)
            skipped_existing_count = len(all_urls_to_check) - len(new_urls_set)
        except Exception:
            # On error, proceed with all URLs to avoid blocking scraping
            new_urls_set = set(all_urls_to_check)

    # Filter groups to only include new URLs (when some were skipped)
    if skipped_existing_count > 0:
        filtered_groups: dict[tuple[str, str | None], list[str]] = {}
        for key, urls in groups.items():
            filtered_urls = [u for u in urls if u in new_urls_set]
            if filtered_urls:
                filtered_groups[key] = filtered_urls
        groups = filtered_groups

        # Mark skipped URLs as completed in the queue
        skipped_items: list[dict[str, Any]] = []
        for row in batch.get("urls", []):
            if not isinstance(row, dict):
                continue
            url_val = row.get("url")
            if not isinstance(url_val, str):
                continue
            # Check if this URL was skipped (not in new_urls_set)
            normalized_url = _to_greenhouse_api_url(url_val, row.get("sourceUrl"))
            if normalized_url not in new_urls_set:
                item: dict[str, Any] = {"url": url_val}
                row_id = row.get("_id") or row.get("id")
                if isinstance(row_id, str):
                    item["id"] = row_id
                skipped_items.append(item)
        if skipped_items:
            try:
                dbos_queue.complete_scrape_urls(
                    {"items": skipped_items, "status": "completed", "error": "already_exists_in_jobs"}
                )
            except Exception:
                pass

        logger.info(
            "SpiderCloud job batch dedup: total=%d new=%d skipped=%d",
            len(all_urls_to_check),
            len(new_urls_set),
            skipped_existing_count,
        )

    if not groups:
        response = {"provider": "spidercloud", "items": {"normalized": []}, "sourceUrl": source_url_hint}
        if persist_scrapes:
            response.update({"scrapeIds": [], "stored": 0, "invalid": 0, "failed": 0, "skippedExisting": skipped_existing_count})
        return response

    scraper = _make_spidercloud_scraper()

    async def _scrape_group(urls: list[str], source_url: str, pattern: str | None, site_id: str | None = None) -> list[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "urls": urls,
            "source_url": source_url or (urls[0] if urls else ""),
            "pattern": pattern,
        }
        posted_at_by_url = posted_at_groups.get((source_url, pattern))
        if posted_at_by_url:
            payload["posted_at_by_url"] = posted_at_by_url
        result = await scraper.scrape_greenhouse_jobs(payload) or {}

        # Unwrap and split into per-URL scrape payloads so they can be stored independently.
        base_payload: Dict[str, Any] | None = None
        if isinstance(result, dict):
            base_payload = (
                result.get("scrape") if isinstance(result.get("scrape"), dict) else result  # support direct payload
            )

        if not isinstance(base_payload, dict):
            return []

        base_payload.setdefault("provider", "spidercloud")
        base_payload.setdefault("workflowName", "SpidercloudJobDetails")
        # Add siteId for seen_job_urls recording during ingestJobsFromScrape
        if site_id:
            base_payload.setdefault("siteId", site_id)

        scrapes: list[Dict[str, Any]] = []
        items = base_payload.get("items") if isinstance(base_payload, dict) else {}
        normalized = items.get("normalized") if isinstance(items, dict) else []
        normalized_sample = items.get("normalizedSample") if isinstance(items, dict) else None
        raw_items = items.get("raw") if isinstance(items, dict) else []
        cost_milli_cents_total: float | None = None
        if isinstance(base_payload.get("costMilliCents"), (int, float)):
            cost_milli_cents_total = float(base_payload["costMilliCents"])
        elif isinstance(items, dict) and isinstance(items.get("costMilliCents"), (int, float)):
            cost_milli_cents_total = float(items["costMilliCents"])

        def _is_url_only(row: Any) -> bool:
            if not isinstance(row, dict):
                return True
            if any(
                row.get(key)
                for key in (
                    "title",
                    "job_title",
                    "jobTitle",
                    "company",
                    "description",
                    "job_description",
                )
            ):
                return False
            return bool(row.get("url") or row.get("job_url") or row.get("absolute_url"))

        if (
            isinstance(normalized, list)
            and normalized
            and isinstance(normalized_sample, list)
            and normalized_sample
        ):
            if all(_is_url_only(row) for row in normalized) and any(
                not _is_url_only(row) for row in normalized_sample
            ):
                normalized = normalized_sample
                if isinstance(items, dict):
                    items["normalized"] = normalized_sample

        url_count = len(urls) if urls else (len(normalized) if isinstance(normalized, list) else 0)
        per_url_cost = (
            int(cost_milli_cents_total / max(url_count, 1))
            if cost_milli_cents_total is not None and url_count
            else None
        )

        if isinstance(normalized, list) and normalized:
            for idx, row in enumerate(normalized):
                if not isinstance(row, dict):
                    continue
                marketing_url = _to_greenhouse_marketing_url(
                    row.get("url") or row.get("job_url") or row.get("absolute_url") or ""
                )
                if marketing_url and not row.get("apply_url"):
                    row["apply_url"] = marketing_url

                single_items: Dict[str, Any] = {"normalized": [row]}
                if isinstance(raw_items, list) and idx < len(raw_items):
                    single_items["raw"] = raw_items[idx]
                per_url_payload = dict(base_payload)
                per_url_payload["items"] = single_items
                # Track the specific URL we processed for easier diagnostics.
                per_url_payload["subUrls"] = [
                    row.get("url") or row.get("job_url") or row.get("absolute_url") or source_url
                ]
                if per_url_cost is not None:
                    per_url_payload["costMilliCents"] = per_url_cost
                    per_url_payload["items"]["costMilliCents"] = per_url_cost
                scrapes.append(_shrink_for_activity(per_url_payload))
        else:
            scrapes.append(_shrink_for_activity(base_payload))

        return scrapes

    scrapes: list[Dict[str, Any]] = []
    max_group_concurrency = max(1, int(runtime_config.spidercloud_job_details_concurrency))
    semaphore = asyncio.Semaphore(max_group_concurrency)

    async def _scrape_group_with_limit(
        urls: list[str],
        source_url: str,
        pattern: str | None,
        site_id: str | None = None,
    ) -> list[Dict[str, Any]]:
        async with semaphore:
            return await _scrape_group(urls, source_url, pattern, site_id)

    tasks: list[asyncio.Task[list[Dict[str, Any]]]] = []
    for (source_url, pattern), urls in groups.items():
        # Look up siteId for this group to pass to store_scrape for seen_job_urls recording
        group_site_id = site_id_by_group.get((source_url, pattern))
        tasks.append(asyncio.create_task(_scrape_group_with_limit(urls, source_url, pattern, group_site_id)))

    if tasks:
        results = await asyncio.gather(*tasks)
        for group_scrapes in results:
            scrapes.extend(group_scrapes)

    if not persist_scrapes:
        return {"scrapes": scrapes, "sourceUrl": source_url_hint}

    def _scrape_url(scrape: Dict[str, Any]) -> str | None:
        sub_urls = scrape.get("subUrls")
        if isinstance(sub_urls, list):
            for entry in sub_urls:
                if isinstance(entry, str) and entry.strip():
                    return entry
        source_url = scrape.get("sourceUrl")
        if isinstance(source_url, str) and source_url.strip():
            return source_url
        return None

    entry_by_url: dict[str, Dict[str, Any]] = {}
    raw_batch_urls = batch.get("urls") if isinstance(batch, dict) else None
    if isinstance(raw_batch_urls, list):
        for entry in raw_batch_urls:
            if not isinstance(entry, dict):
                continue
            url_val = entry.get("url")
            if isinstance(url_val, str) and url_val.strip():
                entry_by_url[url_val] = entry
                normalized_url = _to_greenhouse_api_url(url_val, entry.get("sourceUrl"))
                if normalized_url and normalized_url != url_val:
                    entry_by_url[normalized_url] = entry

    def _build_completion_item(url_val: str) -> Dict[str, Any]:
        item: Dict[str, Any] = {"url": url_val}
        entry = entry_by_url.get(url_val)
        if not entry:
            return item
        row_id = entry.get("_id")
        if isinstance(row_id, str):
            item["id"] = row_id
        source_val = entry.get("sourceUrl")
        if isinstance(source_val, str):
            item["sourceUrl"] = source_val
        provider_val = entry.get("provider")
        if isinstance(provider_val, str):
            item["provider"] = provider_val
        site_val = entry.get("siteId")
        if isinstance(site_val, str):
            item["siteId"] = site_val
        attempts_val = entry.get("attempts")
        if isinstance(attempts_val, (int, float)):
            item["attempts"] = int(attempts_val)
        return item

    def _complete_urls(urls: list[str], status: str, error: str | None = None) -> None:
        if not urls:
            return
        chunk_size = 100
        for idx in range(0, len(urls), chunk_size):
            chunk = urls[idx : idx + chunk_size]
            items = [_build_completion_item(url_val) for url_val in chunk if isinstance(url_val, str)]
            payload: Dict[str, Any] = {"items": items, "status": status}
            if error:
                payload["error"] = error
            try:
                dbos_queue.complete_scrape_urls(payload)
            except Exception:
                logger.warning("SpiderCloud URL completion failed status=%s size=%s", status, len(chunk))

    scrape_ids: list[str] = []
    completed_urls: list[str] = []
    invalid_urls: list[str] = []
    failed_urls: list[str] = []
    http_404_urls: set[str] = set()
    skip_completion_urls: set[str] = set()  # URLs that should not be marked as completed (e.g., timeouts)

    for scrape in scrapes:
        if not isinstance(scrape, dict):
            continue
        items_block = scrape.get("items")
        if not isinstance(items_block, dict):
            continue
        failed_items = items_block.get("failed")
        if not isinstance(failed_items, list):
            continue
        for entry in failed_items:
            if not isinstance(entry, dict):
                continue
            url_val = entry.get("url")
            reason = entry.get("reason")
            status = entry.get("status") or entry.get("httpStatus")
            # Check for skipCompletion flag (e.g., timeouts should not be marked as completed)
            if entry.get("skipCompletion"):
                if isinstance(url_val, str) and url_val.strip():
                    skip_completion_urls.add(url_val.strip())
                continue
            if isinstance(status, (int, float)) and int(status) == 404:
                if isinstance(url_val, str) and url_val.strip():
                    http_404_urls.add(url_val.strip())
                continue
            if isinstance(reason, str) and "404" in reason.lower():
                if isinstance(url_val, str) and url_val.strip():
                    http_404_urls.add(url_val.strip())

    if http_404_urls:
        _complete_urls(sorted(http_404_urls), "failed", error="http_404")

    max_store_concurrency = max(1, int(runtime_config.spidercloud_job_details_concurrency))
    semaphore = asyncio.Semaphore(max_store_concurrency)

    async def _store_scrape_with_limit(  # noqa: DBOS004 - nested in deprecated parent
        scrape: Dict[str, Any],
    ) -> tuple[str, str | None, str | None]:
        url_val = _scrape_url(scrape)
        if isinstance(url_val, str) and url_val in http_404_urls:
            return "skipped", url_val, None
        # Skip storing and completing URLs that timed out - they will be retried next schedule
        if isinstance(url_val, str) and url_val in skip_completion_urls:
            return "skipped", url_val, None
        async with semaphore:
            try:
                res_id = store_scrape(scrape)
                return "completed", url_val, res_id if isinstance(res_id, str) else None
            except ApplicationError as exc:
                if exc.type == "invalid_scrape":
                    return "invalid", url_val, None
                return "failed", url_val, None
            except Exception:
                return "failed", url_val, None

    store_tasks: list[asyncio.Task[tuple[str, str | None, str | None]]] = []
    for idx, scrape in enumerate(scrapes):
        if not isinstance(scrape, dict):
            continue
        store_tasks.append(asyncio.create_task(_store_scrape_with_limit(scrape)))

    results = await asyncio.gather(*store_tasks) if store_tasks else []
    for status, url_val, res_id in results:
        if status == "completed":
            if isinstance(res_id, str):
                scrape_ids.append(res_id)
            if isinstance(url_val, str):
                completed_urls.append(url_val)
        elif status == "invalid":
            if isinstance(url_val, str):
                invalid_urls.append(url_val)
        elif status == "failed":
            if isinstance(url_val, str):
                failed_urls.append(url_val)

    _complete_urls(completed_urls, "completed")
    _complete_urls(invalid_urls, "invalid", error="invalid_job_data")
    _complete_urls(failed_urls, "failed", error="store_scrape_failed")

    response = {
        "scrapeIds": scrape_ids,
        "stored": len(scrape_ids),
        "invalid": len(invalid_urls),
        "failed": len(failed_urls) + len(http_404_urls),
        "sourceUrl": source_url_hint,
    }
    return response

@activity.defn
async def process_spidercloud_listing_batch(  # noqa: DBOS004 - deprecated, use scrape_listing_batch workflow
    batch: Dict[str, Any],
) -> Dict[str, Any]:
    """Scrape listing URLs and enqueue extracted job/detail URLs without storing scrapes.

    .. deprecated::
        Use :func:`scrape_listing_batch` workflow from
        ``job_scrape_application.workflows.workflow`` instead.
    """

    def _build_completion_item(entry: Dict[str, Any]) -> Dict[str, Any]:
        item: Dict[str, Any] = {"url": entry.get("url")}
        row_id = entry.get("_id")
        if isinstance(row_id, str):
            item["id"] = row_id
        source_val = entry.get("sourceUrl")
        if isinstance(source_val, str):
            item["sourceUrl"] = source_val
        provider_val = entry.get("provider")
        if isinstance(provider_val, str):
            item["provider"] = provider_val
        site_val = entry.get("siteId")
        if isinstance(site_val, str):
            item["siteId"] = site_val
        attempts_val = entry.get("attempts")
        if isinstance(attempts_val, (int, float)):
            item["attempts"] = int(attempts_val)
        item["isListingUrl"] = True
        return item

    from ...services.convex_client import convex_query

    listing_entries: list[Dict[str, Any]] = []
    zero_url_error = "scrape.listing.zero_urls"
    entry_by_key: dict[tuple[str, str | None], Dict[str, Any]] = {}
    groups: dict[tuple[str, str | None], list[str]] = {}
    posted_at_groups: dict[tuple[str, str | None], Dict[str, int]] = {}
    source_url_hint = ""
    pagination_limit_cache: dict[str, Optional[int]] = {}
    attempt_entries: list[dict[str, Any]] = []

    for row in batch.get("urls", []):
        if not isinstance(row, dict):
            continue
        url_val = row.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            continue
        cleaned_url = strip_wrapping_url(url_val)
        if not cleaned_url:
            cleaned_url = url_val.strip()
        listing_entries.append(row)
        source_val_raw = row.get("sourceUrl")
        source_val: str = source_val_raw if isinstance(source_val_raw, str) else ""
        pattern_val_raw = row.get("pattern")
        pattern_val: str | None = pattern_val_raw if isinstance(pattern_val_raw, str) else None
        if source_val and not source_url_hint:
            source_url_hint = source_val
        key = (source_val, pattern_val)
        entry_by_key.setdefault(key, row)
        groups.setdefault(key, []).append(cleaned_url)
        provider_raw = row.get("provider")
        attempt_entries.append(
            {
                "url": cleaned_url,
                "sourceUrl": source_val,
                "provider": provider_raw if isinstance(provider_raw, str) else None,
                "attempts": row.get("attempts"),
            }
        )
        posted_at_val = row.get("postedAt")
        if isinstance(posted_at_val, (int, float)):
            mapping = posted_at_groups.setdefault(key, {})
            mapping[normalize_url(cleaned_url) or cleaned_url] = int(posted_at_val)

    _record_scrape_url_attempts(attempt_entries)

    if not groups:
        return {"queued": 0, "listingCompleted": 0, "sourceUrl": source_url_hint}

    scraper = _make_spidercloud_scraper()
    queued_total = 0

    def _resolve_pagination_limit(entry: Dict[str, Any]) -> Optional[int]:
        limit_val = entry.get("paginationLimit")
        if isinstance(limit_val, (int, float)):
            limit_int = int(limit_val)
            if limit_int <= 0:
                return 0
            return limit_int
        site_id = _convex_site_id(entry.get("siteId"))
        if not site_id:
            return DEFAULT_PAGINATION_LIMIT
        if site_id in pagination_limit_cache:
            return pagination_limit_cache[site_id]
        limit = resolve_pagination_limit_step(site_id)
        if limit is None:
            limit = DEFAULT_PAGINATION_LIMIT
        pagination_limit_cache[site_id] = limit
        return limit

    def _limit_listing_urls(
        urls: list[str],
        limit: int,
        source_url: str,
        handler: BaseSiteHandler | None,
    ) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            cleaned.append(url)
        normalized_source = source_url
        source_to_insert = source_url
        if handler:
            normalized = handler.filter_job_urls(cleaned)
            if normalized:
                cleaned = normalized
            if source_url:
                normalized_sources = handler.filter_job_urls([source_url])
                if normalized_sources:
                    normalized_source = normalized_sources[0]
                    source_to_insert = normalized_source
                try:
                    source_parts = urlparse(source_url)
                except Exception:
                    source_parts = None
                if source_parts and source_parts.fragment:
                    source_to_insert = source_url
        if source_to_insert and normalized_source and source_to_insert != normalized_source:
            cleaned = [url for url in cleaned if url != normalized_source]
        if limit > 0:
            def _page_param(url: str) -> Optional[int]:
                try:
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)
                except Exception:
                    return None
                values = params.get("page")
                if not values:
                    return None
                try:
                    page_val = int(values[0])
                except Exception:
                    return None
                return page_val if page_val > 0 else None
            filtered: list[str] = []
            for url in cleaned:
                page_val = _page_param(url)
                if page_val is None or page_val <= limit:
                    filtered.append(url)
            cleaned = filtered
        if (
            source_to_insert
            and handler
            and handler.is_listing_url(source_to_insert)
            and source_to_insert not in cleaned
        ):
            cleaned.insert(0, source_to_insert)
        if limit <= 0 or len(cleaned) <= limit:
            return cleaned
        indexed = list(enumerate(cleaned))
        def _page_key(item: tuple[int, str]) -> tuple[int, int]:
            idx, url = item
            try:
                parsed = urlparse(url)
                params = parse_qs(parsed.query)
            except Exception:
                params = {}
            page_val = None
            for key in ("from", "start", "offset", "page"):
                raw = params.get(key, [None])[0]
                if raw is None:
                    continue
                try:
                    page_val = int(raw)
                except Exception:
                    page_val = None
                if page_val is not None:
                    break
            return ((page_val if page_val is not None else 0), idx)
        ordered = [url for _, url in sorted(indexed, key=_page_key)]
        return ordered[:limit]

    def _enqueue_from_scrape(
        scrape_payload: Dict[str, Any],
        entry: Dict[str, Any],
        requested_urls: list[str],
    ) -> tuple[int, bool]:
        extracted_urls = _extract_job_urls_from_scrape(scrape_payload)
        source_url_raw = entry.get("sourceUrl")
        source_url = source_url_raw if isinstance(source_url_raw, str) else ""
        handler = get_site_handler(source_url) if source_url else None

        # Extract posted_at_by_url from items block if available
        posted_at_by_url: Dict[str, int] = {}
        items_block_for_posted = scrape_payload.get("items") if isinstance(scrape_payload, dict) else None
        if isinstance(items_block_for_posted, dict):
            raw_posted = items_block_for_posted.get("posted_at_by_url")
            if isinstance(raw_posted, dict):
                for key, value in raw_posted.items():
                    if not isinstance(key, str):
                        continue
                    if not isinstance(value, (int, float)):
                        continue
                    normalized_key = normalize_url(key) or key
                    posted_at_by_url[normalized_key] = int(value)

            # If no posted_at_by_url in items, try to extract from raw JSON payload using handler
            if not posted_at_by_url and handler:
                get_posted_at_fn = getattr(handler, "get_posted_at_by_url", None)
                if callable(get_posted_at_fn):
                    raw_block = items_block_for_posted.get("raw")
                    json_payload = None

                    def _extract_json_from_content(content_text: str) -> Any:
                        """Extract JSON payload from SpiderCloud response content."""
                        if not isinstance(content_text, str) or not content_text.strip():
                            return None
                        text = content_text.strip()

                        # Handle HTML-wrapped JSON (raw content from SpiderCloud)
                        # Format: <html><meta ...><pre>{json}</pre></html>
                        if text.startswith("<html"):
                            pre_match = re.search(r'<pre[^>]*>(.*?)</pre>', text, re.DOTALL)
                            if pre_match:
                                text = pre_match.group(1).strip()

                        # Remove markdown escapes (\_  -> _)
                        text = text.replace("\\_", "_")

                        # Remove markdown code fence if present
                        if text.startswith("```"):
                            lines = text.split("\n")
                            # Skip opening fence and closing fence
                            inner_lines = []
                            in_fence = False
                            for line in lines:
                                if line.strip().startswith("```") and not in_fence:
                                    in_fence = True
                                    continue
                                if line.strip() == "```" and in_fence:
                                    break
                                if in_fence:
                                    inner_lines.append(line)
                            text = "\n".join(inner_lines).strip()
                        if not text:
                            return None
                        try:
                            return orjson.loads(text)
                        except orjson.JSONDecodeError:
                            return None

                    # raw_block is a list of {url, events, markdown} dicts
                    if isinstance(raw_block, list):
                        for raw_item in raw_block:
                            if not isinstance(raw_item, dict):
                                continue
                            # First try: Look at events[*].content for raw SpiderCloud response
                            events = raw_item.get("events")
                            if isinstance(events, list):
                                for event in events:
                                    if not isinstance(event, dict):
                                        continue
                                    content = event.get("content")
                                    if not isinstance(content, dict):
                                        continue
                                    # Try raw_html first (for API responses), then commonmark
                                    for content_key in ("raw_html", "raw", "commonmark"):
                                        raw_content = content.get(content_key)
                                        if isinstance(raw_content, str):
                                            json_payload = _extract_json_from_content(raw_content)
                                            if json_payload:
                                                break
                                    if json_payload:
                                        break
                            if json_payload:
                                break
                            # Second try: Look at markdown field directly
                            markdown = raw_item.get("markdown")
                            if isinstance(markdown, str):
                                json_payload = _extract_json_from_content(markdown)
                                if json_payload:
                                    break
                    if json_payload:
                        try:
                            handler_posted_ats = get_posted_at_fn(json_payload)
                            if isinstance(handler_posted_ats, dict):
                                for key, value in handler_posted_ats.items():
                                    if isinstance(key, str) and isinstance(value, (int, float)):
                                        normalized_key = normalize_url(key) or key
                                        posted_at_by_url[normalized_key] = int(value)
                        except Exception:
                            pass

        def _detail_url_from_scrape() -> str | None:
            items_block = scrape_payload.get("items") if isinstance(scrape_payload, dict) else None
            if not isinstance(items_block, dict):
                return None
            normalized = items_block.get("normalized")
            if not isinstance(normalized, list):
                return None
            saw_description = False
            for row in normalized:
                if not isinstance(row, dict):
                    continue
                description = row.get("description") or row.get("job_description")
                if not isinstance(description, str) or not description.strip():
                    continue
                saw_description = True
                for key in ("url", "job_url", "absolute_url", "apply_url"):
                    value = row.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
            if saw_description and source_url:
                return source_url
            return None

        detail_url_override = _detail_url_from_scrape() if not extracted_urls else None

        def _select_base_listing_url() -> str | None:
            for candidate in requested_urls:
                if not isinstance(candidate, str) or not candidate.strip():
                    continue
                if handler:
                    if not handler.is_listing_url(candidate):
                        continue
                elif not _is_probable_listing_url(candidate):
                    continue
                if _is_base_listing_page(candidate):
                    return candidate
            return None

        base_listing_url = _select_base_listing_url()
        base_page_context = _build_listing_zero_url_context(scrape_payload, base_listing_url)
        base_link_count = base_page_context.get("linkCount") if base_page_context else None

        def _sample_urls(values: Iterable[str], limit: int = 10) -> list[str]:
            return [val for idx, val in enumerate(values) if idx < limit]

        def _sample_skip_reasons(
            values: Iterable[dict[str, str]],
            limit: int = 20,
        ) -> list[dict[str, str]]:
            return [val for idx, val in enumerate(values) if idx < limit]

        def _has_timeout_failures() -> bool:
            """Check if scrape failed due to timeout - already logged separately."""
            items_block = scrape_payload.get("items") if isinstance(scrape_payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            # Check if any failures are due to timeout/cancelled
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        def _should_warn_zero_urls() -> bool:
            if not base_listing_url:
                return False
            if handler:
                if not handler.is_listing_url(base_listing_url):
                    return False
            elif not _is_probable_listing_url(base_listing_url):
                return False
            # Don't emit zero_urls if scrape failed due to timeout - that error is already logged
            if _has_timeout_failures():
                return False
            return _is_base_listing_page(base_listing_url)

        should_warn_zero_urls = _should_warn_zero_urls()

        def _emit_listing_event(
            event: str,
            level: str,
            reason: str,
            details: Dict[str, Any] | None = None,
        ) -> None:
            if not should_warn_zero_urls:
                return
            # Extract SpiderCloud request params from providerRequest
            provider_request = scrape_payload.get("providerRequest") if isinstance(scrape_payload, dict) else None
            request_params = None
            if isinstance(provider_request, dict):
                request_params = provider_request.get("params")
            payload = {
                "event": event,
                "level": level,
                "siteUrl": base_listing_url or source_url,
                "data": _strip_none_values(
                    {
                        "provider": entry.get("provider") or scrape_payload.get("provider") or "spidercloud",
                        "sourceUrl": source_url,
                        "listingUrl": base_listing_url,
                        "pattern": entry.get("pattern"),
                        "siteId": entry.get("siteId"),
                        "reason": reason,
                        "extractedCount": len(extracted_urls),
                        "requestedUrlCount": len(requested_urls),
                        "requestedUrlSample": _sample_urls(requested_urls),
                        "requestParams": request_params,
                        "details": details,
                        "pageContext": base_page_context if not extracted_urls else None,
                    }
                ),
            }
            try:
                telemetry.emit_posthog_log(payload)
            except Exception:
                pass

        def _emit_zero_url_warning(reason: str, details: Dict[str, Any] | None = None) -> None:
            # ERROR level - no job URLs found on the page at all
            _emit_listing_event("scrape.listing.zero_urls", "error", reason, details)

        def _emit_skip_all_seen_urls(reason: str, details: Dict[str, Any] | None = None) -> None:
            # WARN level - all URLs already exist in Convex DB
            _emit_listing_event("scrape.listing.skip_all_seen_urls", "warn", reason, details)

        def _emit_skip_all_invalid_urls(reason: str, details: Dict[str, Any] | None = None) -> None:
            # WARN level - all URLs are invalid
            _emit_listing_event("scrape.listing.skip_all_invalid_urls", "warn", reason, details)

        def _emit_listing_url_counts(reason: str, details: Dict[str, Any]) -> None:
            payload = {
                "event": "scrape.listing.url_counts",
                "level": "info",
                "siteUrl": base_listing_url or source_url,
                "data": _strip_none_values(
                    {
                        "provider": entry.get("provider") or scrape_payload.get("provider") or "spidercloud",
                        "sourceUrl": source_url,
                        "listingUrl": base_listing_url,
                        "pattern": entry.get("pattern"),
                        "siteId": entry.get("siteId"),
                        "reason": reason,
                        "details": details,
                    }
                ),
            }
            try:
                telemetry.emit_posthog_log(payload)
            except Exception:
                pass

        def _extract_pagination_payload(value: Any) -> dict[str, Any] | None:
            if isinstance(value, dict):
                jobs_val = value.get("jobs")
                if isinstance(jobs_val, list):
                    return value
                positions_val = value.get("positions")
                if isinstance(positions_val, list):
                    return value
            for text in (t for t in gather_strings(value) if isinstance(t, str) and t.strip()):
                if "<pre" in text.lower():
                    payload = BaseSiteHandler._extract_json_payload_from_html(text)  # noqa: SLF001
                    if isinstance(payload, dict):
                        return payload
                try:
                    parsed = orjson.loads(text)
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    jobs_val = parsed.get("jobs")
                    if isinstance(jobs_val, list):
                        return parsed
                    positions_val = parsed.get("positions")
                    if isinstance(positions_val, list):
                        return parsed
            return None

        def _extract_pagination_urls() -> list[str]:
            if not handler or not source_url:
                return []
            items_block = scrape_payload.get("items") if isinstance(scrape_payload, dict) else None
            raw_val = items_block.get("raw") if isinstance(items_block, dict) else None
            pagination_payload = _extract_pagination_payload(raw_val)
            pagination_urls: list[str] = []
            if pagination_payload:
                pagination_urls = handler.get_pagination_urls_from_json(pagination_payload, source_url)
            if not pagination_urls:
                pagination_urls = handler.get_pagination_urls_from_listing(source_url)
            if not pagination_urls:
                return []
            normalized: list[str] = []
            seen: set[str] = set()
            for value in pagination_urls:
                if not isinstance(value, str):
                    continue
                cleaned = strip_wrapping_url(value) or value.strip()
                if not cleaned:
                    continue
                normalized_url = normalize_url(cleaned, base_url=source_url) or cleaned
                if not normalized_url or not handler.is_listing_url(normalized_url):
                    continue
                if normalized_url in seen:
                    continue
                seen.add(normalized_url)
                normalized.append(normalized_url)
            return BaseSiteHandler.drop_source_listing_url(normalized, source_url)

        if not extracted_urls and not detail_url_override:
            _emit_zero_url_warning("no_extracted_urls")
            _emit_listing_url_counts(
                "no_extracted_urls",
                {
                    "extractedCount": 0,
                    "requestedUrlCount": len(requested_urls),
                    "requestedUrlSample": _sample_urls(requested_urls),
                },
            )
            return 0, should_warn_zero_urls

        if isinstance(base_link_count, int) and base_link_count == 0:
            _emit_zero_url_warning(
                "base_page_no_urls",
                _strip_none_values(
                    {
                        "basePageContext": base_page_context or None,
                        "basePageLinkCount": base_link_count,
                    }
                ),
            )

        force_detail_urls = False
        converted_urls: list[str] = []
        if detail_url_override:
            urls = [detail_url_override]
            invalid_urls = []
            force_detail_urls = True
        else:
            urls = _filter_job_urls(
                extracted_urls,
                handler,
                _is_probable_listing_url,
                pattern=entry.get("pattern"),
                source_url=source_url,
            )
            converted_urls, invalid_urls = _classify_filtered_urls(
                extracted_urls, urls, handler, source_url
            )

        if not urls:
            invalid_details = _strip_none_values(
                {
                    "invalidCount": len(invalid_urls),
                    "invalidSample": _sample_urls(invalid_urls),
                    "extractedSample": _sample_urls(extracted_urls),
                }
            )
            _emit_skip_all_invalid_urls("filtered_invalid_urls", invalid_details)
            _emit_listing_url_counts(
                "filtered_invalid_urls",
                {
                    "extractedCount": len(extracted_urls),
                    "invalidCount": len(invalid_urls),
                    "invalidSample": invalid_details.get("invalidSample"),
                },
            )
            return 0, should_warn_zero_urls

        job_urls: list[str] = []
        listing_urls: list[str] = []
        for url in urls:
            if force_detail_urls:
                job_urls.append(url)
                continue
            if handler:
                is_listing = handler.is_listing_url(url)
            else:
                is_listing = _is_probable_listing_url(url)
            if is_listing:
                listing_urls.append(url)
            else:
                job_urls.append(url)

        listing_urls_extracted = list(listing_urls)
        listing_urls = _extract_pagination_urls()
        pagination_limit = _resolve_pagination_limit(entry)
        listing_urls_before_pagination = list(listing_urls)
        pagination_dropped: list[str] = []
        if pagination_limit and listing_urls:
            limited_listing_urls = _limit_listing_urls(
                listing_urls,
                pagination_limit,
                source_url,
                handler,
            )
            pagination_dropped = [
                url for url in listing_urls_before_pagination if url not in limited_listing_urls
            ]
            listing_urls = limited_listing_urls

        seen_listing: set[str] = set()
        listing_urls_before_seen = list(listing_urls)
        if listing_urls and source_url:
            try:
                seen_listing = set(
                    u
                    for u in fetch_seen_urls_for_site(
                        source_url,
                        entry.get("pattern"),
                        listing_urls,
                    )
                    if isinstance(u, str)
                )
            except Exception:
                seen_listing = set()
            if seen_listing and handler:
                seen_listing = {u for u in seen_listing if not handler.is_listing_url(u)}
            if seen_listing:
                listing_urls = [u for u in listing_urls if u not in seen_listing]
        listing_seen_dropped = [url for url in listing_urls_before_seen if url in seen_listing]

        job_urls_before_existing = list(job_urls)
        # Use filter_new_job_urls for efficiency - returns only non-existing URLs (less network transfer)
        filter_new_job_urls_fallback = False
        filter_new_job_urls_error: str | None = None
        if job_urls:
            try:
                new_job_urls = filter_new_job_urls(job_urls)
            except Exception as exc:
                filter_new_job_urls_fallback = True
                filter_new_job_urls_error = str(exc)[:200]
                new_job_urls = job_urls  # On error, proceed with all URLs
            new_job_urls_set = set(new_job_urls)
            job_urls = [u for u in job_urls if u in new_job_urls_set]
        else:
            new_job_urls_set = set()
        job_existing_dropped = [url for url in job_urls_before_existing if url not in new_job_urls_set]

        # Log if fallback was triggered - this indicates a Convex query failure
        if filter_new_job_urls_fallback:
            try:
                telemetry.emit_posthog_log({
                    "event": "scrape.listing.filter_new_job_urls_fallback",
                    "level": "warn",
                    "siteUrl": base_listing_url or source_url,
                    "data": _strip_none_values({
                        "sourceUrl": source_url,
                        "siteId": entry.get("siteId"),
                        "jobUrlCount": len(job_urls_before_existing),
                        "error": filter_new_job_urls_error,
                        "fallbackAction": "proceed_with_all_urls",
                    }),
                })
            except Exception:
                pass

        if not job_urls:
            skip_reasons: list[dict[str, str]] = []
            skip_reasons.extend({"url": url, "reason": "url_converted"} for url in converted_urls)
            skip_reasons.extend({"url": url, "reason": "invalid_url"} for url in invalid_urls)
            skip_reasons.extend(
                {"url": url, "reason": "listing_from_scrape_ignored"}
                for url in listing_urls_extracted
            )
            skip_reasons.extend(
                {"url": url, "reason": "listing_pagination_limit"} for url in pagination_dropped
            )
            skip_reasons.extend({"url": url, "reason": "listing_seen"} for url in listing_seen_dropped)
            skip_reasons.extend(
                {"url": url, "reason": "detail_existing_job"} for url in job_existing_dropped
            )
            zero_details = _strip_none_values(
                {
                    "invalidCount": len(invalid_urls),
                    "invalidSample": _sample_urls(invalid_urls),
                    "listingExtractedCount": len(listing_urls_extracted),
                    "listingExtractedSample": _sample_urls(listing_urls_extracted),
                    "listingCount": len(listing_urls_before_pagination),
                    "listingSample": _sample_urls(listing_urls_before_pagination),
                    "listingPaginationDroppedCount": len(pagination_dropped),
                    "listingPaginationDroppedSample": _sample_urls(pagination_dropped),
                    "listingSeenDroppedCount": len(listing_seen_dropped),
                    "listingSeenDroppedSample": _sample_urls(listing_seen_dropped),
                    "jobCount": len(job_urls_before_existing),
                    "jobSample": _sample_urls(job_urls_before_existing),
                    "jobNewCount": len(new_job_urls_set),
                    "jobExistingDroppedCount": len(job_existing_dropped),
                    "jobExistingDroppedSample": _sample_urls(job_existing_dropped),
                    "jobFilterFallback": filter_new_job_urls_fallback or None,
                    "skipReasonCount": len(skip_reasons),
                    "skipReasons": _sample_skip_reasons(skip_reasons),
                    "paginationLimit": pagination_limit,
                }
            )
            # Determine the appropriate event based on why URLs were filtered
            # If all job URLs were dropped because they already exist in DB, use WARN level
            all_seen = (
                len(job_urls_before_existing) > 0
                and len(job_existing_dropped) == len(job_urls_before_existing)
                and len(invalid_urls) == 0
            )
            if all_seen:
                _emit_skip_all_seen_urls("filtered_to_zero", zero_details)
            else:
                _emit_zero_url_warning("filtered_to_zero", zero_details)
            _emit_listing_url_counts(
                "filtered_to_zero",
                {
                    "extractedCount": len(extracted_urls),
                    "invalidCount": len(invalid_urls),
                    "listingCount": len(listing_urls_before_pagination),
                    "listingPaginationDroppedCount": len(pagination_dropped),
                    "listingSeenDroppedCount": len(listing_seen_dropped),
                    "jobCount": len(job_urls_before_existing),
                    "jobNewCount": len(new_job_urls_set),
                    "jobExistingDroppedCount": len(job_existing_dropped),
                    "jobFilterFallback": filter_new_job_urls_fallback or None,
                    "skipReasonCount": len(skip_reasons),
                    "skipReasons": zero_details.get("skipReasons"),
                },
            )
            # Only mark as failed if this is a listing page AND not all URLs were just skipped
            # If all URLs were skipped because they already exist, don't treat as error
            return 0, should_warn_zero_urls and not all_seen

        merged_urls = job_urls
        url_types = ["detail"] * len(merged_urls)
        delays_ms: list[int] | None = None

        # Build postedAts list from posted_at_by_url
        posted_ats_to_enqueue: list[int | None] | None = None
        if posted_at_by_url and merged_urls:
            posted_ats_to_enqueue = [posted_at_by_url.get(normalize_url(url) or url) for url in merged_urls]
            if not any(isinstance(val, (int, float)) for val in posted_ats_to_enqueue):
                posted_ats_to_enqueue = None

        skip_reasons: list[dict[str, str]] = []
        skip_reasons.extend({"url": url, "reason": "url_converted"} for url in converted_urls)
        skip_reasons.extend({"url": url, "reason": "invalid_url"} for url in invalid_urls)
        skip_reasons.extend(
            {"url": url, "reason": "listing_from_scrape_ignored"} for url in listing_urls_extracted
        )
        skip_reasons.extend(
            {"url": url, "reason": "listing_pagination_limit"} for url in pagination_dropped
        )
        skip_reasons.extend({"url": url, "reason": "listing_seen"} for url in listing_seen_dropped)
        skip_reasons.extend(
            {"url": url, "reason": "detail_existing_job"} for url in job_existing_dropped
        )

        enqueue_result = dbos_queue.enqueue_scrape_urls(
            _strip_none_values(
                {
                    "urls": merged_urls,
                    "sourceUrl": source_url or "",
                    "provider": entry.get("provider") or "spidercloud",
                    "siteId": entry.get("siteId"),
                    "pattern": entry.get("pattern"),
                    "delaysMs": delays_ms,
                    "urlTypes": url_types,
                    "postedAts": posted_ats_to_enqueue,
                }
            )
        )
        queued_accepted = None
        if isinstance(enqueue_result, dict):
            queued_val = enqueue_result.get("queued")
            if isinstance(queued_val, int):
                queued_accepted = queued_val

        _emit_listing_url_counts(
            "enqueued",
            _strip_none_values(
                {
                    "extractedCount": len(extracted_urls),
                    "invalidCount": len(invalid_urls),
                    "listingCount": len(listing_urls_before_pagination),
                    "listingPaginationDroppedCount": len(pagination_dropped),
                    "listingSeenDroppedCount": len(listing_seen_dropped),
                    "jobCount": len(job_urls_before_existing),
                    "jobNewCount": len(new_job_urls_set),
                    "jobExistingDroppedCount": len(job_existing_dropped),
                    "jobFilterFallback": filter_new_job_urls_fallback or None,
                    "queuedListingCount": 0,
                    "queuedDetailCount": len(job_urls),
                    "queuedTotal": len(job_urls),
                    "queueAccepted": queued_accepted,
                    "skipReasonCount": len(skip_reasons),
                    "skipReasons": _sample_skip_reasons(skip_reasons),
                }
            ),
        )
        return len(merged_urls), False

    max_group_concurrency = max(1, int(runtime_config.spidercloud_job_details_concurrency))
    semaphore = asyncio.Semaphore(max_group_concurrency)

    async def _process_group(  # noqa: DBOS004 - nested in deprecated parent
        source_url: str,
        pattern: str | None,
        urls: list[str],
    ) -> tuple[int, bool]:
        async with semaphore:
            payload: Dict[str, Any] = {
                "urls": urls,
                "source_url": source_url or (urls[0] if urls else ""),
                "pattern": pattern,
            }
            posted_at_by_url = posted_at_groups.get((source_url, pattern))
            if posted_at_by_url:
                payload["posted_at_by_url"] = posted_at_by_url
            result = await scraper.scrape_greenhouse_jobs(payload) or {}
        base_payload = None
        if isinstance(result, dict):
            base_payload = result.get("scrape") if isinstance(result.get("scrape"), dict) else result
        if not isinstance(base_payload, dict):
            return 0, False
        base_payload.setdefault("provider", "spidercloud")
        base_payload.setdefault("workflowName", "SpidercloudListing")
        entry = entry_by_key.get((source_url, pattern)) or {}
        return _enqueue_from_scrape(base_payload, entry, urls)

    zero_url_keys: set[tuple[str, str | None]] = set()
    tasks: list[tuple[tuple[str, str | None], asyncio.Task[tuple[int, bool]]]] = []
    for (source_url, pattern), urls in groups.items():
        tasks.append(
            ((source_url, pattern), asyncio.create_task(_process_group(source_url, pattern, urls)))
        )

    if tasks:
        results = await asyncio.gather(*(task for _, task in tasks))
        for (key, _), (queued_count, zero_urls) in zip(tasks, results):
            queued_total += queued_count
            if zero_urls:
                zero_url_keys.add(key)

    listing_completed = 0
    if listing_entries:
        completed_items = []
        failed_items = []
        for entry in listing_entries:
            url_val = entry.get("url")
            if not isinstance(url_val, str) or not url_val.strip():
                continue
            source_val_raw = entry.get("sourceUrl")
            source_val: str = source_val_raw if isinstance(source_val_raw, str) else ""
            pattern_val_raw = entry.get("pattern")
            pattern_val: str | None = pattern_val_raw if isinstance(pattern_val_raw, str) else None
            key = (source_val, pattern_val)
            item = _build_completion_item(entry)
            if key in zero_url_keys:
                failed_items.append(item)
            else:
                completed_items.append(item)
        if completed_items:
            dbos_queue.complete_scrape_urls({"items": completed_items, "status": "completed"})
            listing_completed = len(completed_items)
        if failed_items:
            dbos_queue.complete_scrape_urls(
                {"items": failed_items, "status": "failed", "error": zero_url_error}
            )

    return {"queued": queued_total, "listingCompleted": listing_completed, "sourceUrl": source_url_hint}

@activity.defn
async def scrape_greenhouse_jobs(
    payload: Dict[str, Any],
    workflow_context: Dict[str, Any] | None = None,
    persist_scrape: bool = False,
) -> Dict[str, Any]:
    """Scrape new Greenhouse job URLs with a single FetchFox request."""

    idempotency_key = payload.get("idempotency_key") or payload.get("webhook_id")
    if settings.spider_api_key and not idempotency_key:
        scraper = _make_spidercloud_scraper()
    elif settings.firecrawl_api_key:
        scraper = _make_firecrawl_scraper()
    else:
        scraper = _factories.build_fetchfox_scraper(
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
    scraper = _make_firecrawl_scraper()
    return await scraper.scrape_greenhouse_jobs(payload)

@activity.defn
def fetch_pending_firecrawl_webhooks(limit: int = 25, event: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return unprocessed Firecrawl webhook rows from Convex."""
    return fetch_pending_firecrawl_webhooks_step(limit=limit, event=event)


@activity.defn
def get_firecrawl_webhook_status(job_id: str) -> Dict[str, Any]:
    """Return the current Convex state for a Firecrawl job's webhook rows."""
    return get_firecrawl_webhook_status_step(job_id)


@activity.defn
def mark_firecrawl_webhook_processed(webhook_id: str, error: Optional[str] = None) -> None:
    """Mark a webhook row as processed and optionally attach an error."""
    mark_firecrawl_webhook_processed_step(webhook_id=webhook_id, error=error)

@activity.defn
async def collect_firecrawl_job_result(  # noqa: DBOS004 - deprecated, mixed async+convex
    event: FirecrawlWebhookEvent,
) -> Dict[str, Any]:
    """Fetch Firecrawl job status and build a scrape payload.

    .. deprecated::
        Use step functions from ``job_scrape_application.workflows.activities.step`` instead.
    """

    job_id = str(event.get("jobId") or event.get("id") or "")
    if not job_id:
        raise ApplicationError("Webhook payload missing jobId", non_retryable=True)

    metadata_raw = event.get("metadata")
    metadata: Dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
    payload_raw = event.get("payload")
    payload_dict: Dict[str, Any] = payload_raw if isinstance(payload_raw, dict) else {}
    data_block = payload_dict.get("data") or event.get("data")

    def _data_source_url() -> Optional[str]:
        if not isinstance(data_block, list):
            return None
        for item in data_block:
            if not isinstance(item, dict):
                continue
            meta_raw = item.get("metadata")
            meta = meta_raw if isinstance(meta_raw, dict) else {}
            for key in ("sourceURL", "sourceUrl", "url"):
                candidate = meta.get(key) or item.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate
        return None

    source_url = (
        event.get("sourceUrl")
        or metadata.get("siteUrl")
        or metadata.get("sourceUrl")
        or metadata.get("sourceURL")
        or metadata.get("url")
        or payload_dict.get("url")
        or _data_source_url()
    )
    pattern = metadata.get("pattern")
    site_id = metadata.get("siteId") or event.get("siteId")
    kind = metadata.get("kind") or (
        FirecrawlJobKind.GREENHOUSE_LISTING
        if metadata.get("siteType") == "greenhouse"
        else FirecrawlJobKind.SITE_CRAWL
    )
    raw_status_url = (
        event.get("statusUrl")
        or event.get("status_url")
        or metadata.get("statusUrl")
        or metadata.get("status_url")
    )
    status_link = _build_provider_status_url("firecrawl", job_id, status_url=raw_status_url, kind=kind)
    data_items = len(event.get("data", [])) if isinstance(event.get("data"), list) else 0
    metadata_keys = len(metadata)

    now = int(time.time() * 1000)

    def _coerce_int(val: Any) -> Optional[int]:
        if isinstance(val, (int, float)):
            return int(val)
        return None

    def _first_seen_ms() -> int:
        """Best-effort timestamp for when the job was initially queued/received."""

        candidates = [
            metadata.get("queuedAt"),
            metadata.get("createdAt"),
            metadata.get("startedAt"),
            event.get("receivedAt"),
            event.get("createdAt"),
        ]
        payload = event.get("payload")
        if isinstance(payload, dict):
            candidates.extend([payload.get("queuedAt"), payload.get("receivedAt"), payload.get("createdAt")])

        for val in candidates:
            coerced = _coerce_int(val)
            if coerced is not None:
                return coerced
        return now

    first_seen_ms = _first_seen_ms()
    age_ms = max(0, now - first_seen_ms)
    status_endpoint_default = f"https://api.firecrawl.dev/v2/batch/scrape/{job_id}"
    status_endpoint = status_link or status_endpoint_default
    use_mock_provider = _should_use_mock_firecrawl(source_url)
    if use_mock_provider:
        status_endpoint = status_endpoint.replace("https://api.firecrawl.dev", "mock://firecrawl")
    status_link = status_endpoint

    logger.info(
        "collect_firecrawl_job_result start job_id=%s kind=%s site_id=%s site_url=%s data_items=%s status_link=%s",
        job_id,
        kind,
        site_id,
        source_url,
        data_items,
        status_link,
    )
    try:
        telemetry.emit_posthog_log(
            _strip_none_values(
                {
                    "event": "firecrawl.webhook.status_fetch",
                    "jobId": job_id,
                    "kind": kind,
                    "siteId": site_id,
                    "siteUrl": source_url,
                    "pattern": pattern,
                    "ageMs": age_ms,
                    "statusUrl": status_endpoint or "n/a",
                    "dataItems": data_items,
                    "metadataKeys": metadata_keys,
                    "mockProvider": use_mock_provider,
                }
            )
        )
    except Exception:
        pass
    raw_url_candidates = (
        metadata.get("urls")
        or metadata.get("seedUrls")
        or metadata.get("urlsRequested")
    )
    url_candidates = _metadata_urls_to_list(raw_url_candidates)
    request_payload: Dict[str, Any] = {
        "jobId": job_id,
        "kind": kind,
        "siteId": site_id,
        "siteUrl": source_url,
        "pattern": pattern,
    }
    if url_candidates:
        request_payload["urls"] = url_candidates
    if event.get("webhookId"):
        request_payload["webhookId"] = event.get("webhookId")

    request_provider = "firecrawl_mock" if use_mock_provider else "firecrawl"
    request_snapshot = _build_request_snapshot(
        request_payload,
        provider=request_provider,
        method="GET",
        url=status_endpoint,
    )

    if use_mock_provider:
        return _mock_firecrawl_status_response(
            event=event,
            job_id=job_id,
            kind=kind,
            site_id=site_id,
            source_url=source_url,
            pattern=pattern,
            status_endpoint=status_endpoint,
            request_snapshot=request_snapshot,
            first_seen_ms=first_seen_ms,
        )

    firecrawl_api_key = settings.firecrawl_api_key
    if not firecrawl_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY env var is required for Firecrawl",
            non_retryable=True,
        )

    pagination = PaginationConfig(auto_paginate=True, max_wait_time=30, max_results=5000)

    def _record_scrape_error(error: str) -> None:
        error_payload: ScrapeErrorInput = {
            "sourceUrl": source_url,
            "error": error,
            "metadata": metadata,
            "payload": event,
            "createdAt": int(time.time() * 1000),
        }
        if job_id is not None:
            error_payload["jobId"] = job_id
        if site_id is not None:
            error_payload["siteId"] = site_id
        event_name = event.get("event") or event.get("type")
        if event_name is not None:
            error_payload["event"] = event_name
        status_value = event.get("status")
        if status_value is not None:
            error_payload["status"] = status_value

        try:
            if _should_mock_convex_webhooks():
                logger.info(
                    "collect_firecrawl_job_result skip error log (mock convex) job_id=%s error=%s",
                    job_id,
                    error,
                )
                return
            _log_scrape_error(error_payload)
        except Exception:
            # Non-fatal best-effort logging; keep workflow progress
            pass

    if age_ms >= FIRECRAWL_STATUS_EXPIRATION_MS:
        msg = (
            "Firecrawl job expired (>24h); skipping status lookup "
            f"(job_id={job_id}, site_id={site_id}, age_ms={age_ms})"
        )
        logger.warning("collect_firecrawl_job_result expired job: %s", msg)
        _record_scrape_error(msg)
        return {
            "kind": kind,
            "siteId": site_id,
            "siteUrl": source_url,
            "status": "cancelled_expired",
            "jobsScraped": 0,
            "error": msg,
            "scrape": None,
        }

    if age_ms >= FIRECRAWL_STATUS_WARN_MS:
        logger.info(
            "collect_firecrawl_job_result nearing expiration job_id=%s age_ms=%s",
            job_id,
            age_ms,
        )

    def _get_status() -> Any:
        client = Firecrawl(api_key=firecrawl_api_key)
        return client.get_batch_scrape_status(job_id, pagination_config=pagination)

    try:
        status = await asyncio.to_thread(_get_status)
    except Exception as exc:  # noqa: BLE001
        error_msg = str(exc)
        logger.warning(
            "collect_firecrawl_job_result status fetch failed job_id=%s kind=%s error=%s",
            job_id,
            kind,
            error_msg,
            exc_info=True,
        )
        _record_scrape_error(error_msg)
        msg_lower = error_msg.lower()
        missing_method = "no attribute" in msg_lower or "has no attribute" in msg_lower
        if (("404" in msg_lower) or missing_method) and age_ms >= FIRECRAWL_STATUS_WARN_MS:
            msg = (
                "Firecrawl failed to complete within 24h; treating job as cancelled "
                f"(job_id={job_id}, site_id={site_id}, age_ms={age_ms})"
            )
            return {
                "kind": kind,
                "siteId": site_id,
                "siteUrl": source_url,
                "status": "cancelled_expired",
                "httpStatus": "404",
                "itemsCount": 0,
                "jobsScraped": 0,
                "error": msg,
                "scrape": None,
            }
        retryable = "429" in msg_lower or "timeout" in msg_lower or "too many requests" in msg_lower
        if "invalid job id" in msg_lower:
            return {
                "kind": kind,
                "siteId": site_id,
                "siteUrl": source_url,
                "status": "error",
                "httpStatus": "invalid_job",
                "itemsCount": 0,
                "jobsScraped": 0,
                "error": error_msg,
                "scrape": None,
            }
        raise ApplicationError(f"Failed to fetch Firecrawl status for job {job_id}: {exc}", non_retryable=not retryable) from exc

    status_value = getattr(status, "status", None)
    http_status = "ok"
    now = int(time.time() * 1000)

    if kind == FirecrawlJobKind.GREENHOUSE_LISTING:
        json_payload = _extract_first_json_doc(status)
        raw_text = None

        # When using rawHtml format, status may hold raw_html instead of json
        if json_payload is None:
            raw_text = _extract_first_text_doc(status)
            try:
                if raw_text:
                    json_payload = orjson.loads(raw_text)
            except Exception:
                json_payload = None

        if raw_text is None and json_payload is not None:
            raw_text = orjson.dumps(json_payload).decode("utf-8")

        if json_payload is None:
            # Attempt direct fetch of the board JSON as a fallback
            try:
                fallback_site: Site = {
                    "_id": str(site_id or "unknown"),
                    "url": source_url or "",
                    "type": metadata.get("siteType"),
                    "pattern": metadata.get("pattern"),
                    "name": metadata.get("siteName"),
                }
                fallback = await fetch_greenhouse_listing_firecrawl(fallback_site)
                raw_text = fallback.get("raw") if isinstance(fallback, dict) else None
                json_payload = raw_text
            except Exception:
                # No structured payload returned; treat as empty result but still mark processed
                return {
                    "kind": FirecrawlJobKind.GREENHOUSE_LISTING,
                    "siteId": site_id,
                    "siteUrl": source_url,
                    "status": status_value,
                    "httpStatus": http_status,
                    "itemsCount": 0,
                    "job_urls": [],
                    "raw": raw_text or "{}",
                }

        response_block = {
            "status": status_value,
            "raw": raw_text or json_payload,
        }
        async_response_block = {
            "jobId": job_id,
            "status": status_value,
            "event": event.get("event") or event.get("type"),
            "receivedAt": event.get("receivedAt"),
            "payload": event,
            "metadata": metadata,
        }

        try:
            board: GreenhouseBoardResponse = load_greenhouse_board(raw_text or json_payload)
            job_urls = extract_greenhouse_job_urls(board, required_keywords=())
            posted_at_by_url: Dict[str, int] = {}

            def _pick_job_timestamp(job: Any) -> Any | None:
                for value in (getattr(job, "updated_at", None), getattr(job, "first_published", None)):
                    if isinstance(value, str):
                        cleaned = value.strip()
                        if cleaned:
                            return cleaned
                    elif isinstance(value, (int, float)):
                        return value
                extra = getattr(job, "model_extra", None) or {}
                if isinstance(extra, dict):
                    for key in (
                        "updated_at",
                        "updatedAt",
                        "first_published",
                        "firstPublished",
                        "created_at",
                        "createdAt",
                    ):
                        val = extra.get(key)
                        if isinstance(val, str):
                            cleaned = val.strip()
                            if cleaned:
                                return cleaned
                        elif isinstance(val, (int, float)):
                            return val
                return None

            if board.jobs:
                for job in board.jobs:
                    raw_date = _pick_job_timestamp(job)
                    if raw_date is None:
                        continue
                    posted_at = parse_posted_at(raw_date)
                    absolute_url = getattr(job, "absolute_url", None)
                    if isinstance(absolute_url, str) and absolute_url.strip():
                        normalized = normalize_url(absolute_url) or absolute_url
                        posted_at_by_url[normalized] = posted_at
        except Exception as exc:  # noqa: BLE001
            payload = {
                "event": "scrape.greenhouse_listing.webhook_parse_failed",
                "level": "error",
                "siteUrl": source_url or "",
                "data": {
                    "provider": "firecrawl",
                    "siteId": site_id,
                    "rawLength": len(raw_text) if isinstance(raw_text, str) else 0,
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
                        "event": "scrape.greenhouse_listing.webhook_parse_failed",
                        "siteUrl": source_url,
                        "siteId": site_id,
                        "provider": "firecrawl",
                        "jobId": job_id,
                    },
                )
            except Exception:
                pass
            raise ApplicationError(f"Unable to parse Greenhouse board payload (webhook): {exc}", non_retryable=True) from exc

        logger.info(
            "collect_firecrawl_job_result greenhouse job_id=%s status=%s urls=%d status_url=%s",
            job_id,
            status_value,
            len(job_urls),
            status_link,
        )
        try:
            telemetry.emit_posthog_log(
                _strip_none_values(
                    {
                        "event": "firecrawl.webhook.status",
                        "jobId": job_id,
                        "kind": FirecrawlJobKind.GREENHOUSE_LISTING,
                        "status": status_value,
                        "urls": len(job_urls),
                        "httpStatus": http_status,
                        "statusUrl": status_link or "n/a",
                    }
                )
            )
        except Exception:
            pass
        return {
            "kind": FirecrawlJobKind.GREENHOUSE_LISTING,
            "siteId": site_id,
            "siteUrl": source_url,
            "status": status_value,
            "httpStatus": http_status,
            "request": request_snapshot,
            "response": response_block,
            "asyncResponse": async_response_block,
            "itemsCount": len(job_urls),
            "jobsScraped": len(job_urls),
            "job_urls": job_urls,
            "posted_at_by_url": posted_at_by_url if posted_at_by_url else None,
            "raw": raw_text,
        }

    raw_payload = (
        status.model_dump(mode="json", exclude_none=True)
        if hasattr(status, "model_dump")
        else status
    )
    normalized_items = normalize_firecrawl_items(raw_payload)
    try:
        telemetry.emit_posthog_log(
            _strip_none_values(
                {
                    "event": "firecrawl.webhook.status",
                    "jobId": job_id,
                    "kind": kind,
                    "status": status_value,
                    "items": len(normalized_items),
                    "httpStatus": http_status,
                    "statusUrl": status_link or "n/a",
                }
            )
        )
    except Exception:
        pass

    scrape_payload = {
        "sourceUrl": source_url or "",
        "pattern": pattern,
        "startedAt": event.get("receivedAt") or metadata.get("startedAt") or now,
        "completedAt": now,
        "request": request_snapshot,
        "items": {
            "normalized": normalized_items,
            "raw": raw_payload,
            "provider": "firecrawl",
            "seedUrls": url_candidates or None,
            "request": request_snapshot,
        },
        "provider": "firecrawl",
        "workflowName": "ProcessWebhookScrape",
    }

    return {
        "kind": kind,
        "siteId": site_id,
        "siteUrl": source_url,
        "status": status_value,
        "httpStatus": http_status,
        "request": request_snapshot,
        "scrape": scrape_payload,
        "jobsScraped": len(normalized_items),
        "itemsCount": len(normalized_items),
    }


@activity.defn
def store_scrape(scrape: Dict[str, Any]) -> str:  # noqa: DBOS001
    try:
        def _log_workflow_event(
            event: str,
            message: str | None = None,
            data: Dict[str, Any] | None = None,
            *,
            level: str = "info",
        ) -> None:
            site_url = scrape.get("sourceUrl")
            if not isinstance(site_url, str):
                site_url = ""
            workflow_id = scrape.get("workflowId") or scrape.get("workflow_id")
            payload = _strip_none_values(
                {
                    "event": event,
                    "message": message,
                    "data": data,
                    "createdAt": int(time.time() * 1000),
                    "workflowName": scrape.get("workflowName"),
                    "workflowId": workflow_id or "unknown",
                    "runId": scrape.get("runId") or scrape.get("run_id"),
                    "siteUrl": site_url or "",
                    "level": level,
                }
            )
            payload["message"] = _build_log_message(payload)
            try:
                telemetry.emit_posthog_log(payload)
            except Exception:
                # best-effort; ignore logging errors
                pass
    
        def _apply_job_detail_heuristics_to_jobs(
            jobs: List[Dict[str, Any]],
            heuristic_time_ms: int,
            context: Dict[str, Any] | None = None,
        ) -> List[Dict[str, Any]]:
            """Enrich job rows with heuristic parsing before ingestion."""
            context_payload = _strip_none_values(context or {})
            configs_cache: Dict[str, List[Dict[str, Any]]] = {}
            enriched: List[Dict[str, Any]] = []
            for job in jobs:
                domain = _domain_from_url(job.get("url") or "")
                configs = configs_cache.get(domain)
                if configs is None:
                    try:
                        configs = list_job_detail_configs_step(domain)
                    except asyncio.CancelledError:
                        # Best-effort heuristics; ignore cancellation from auxiliary Convex calls.
                        try:
                            telemetry.emit_posthog_log(
                                _strip_none_values(
                                    {
                                        "event": "heuristic.list_configs_cancelled",
                                        "level": "warning",
                                        "domain": domain,
                                        "url": job.get("url"),
                                        **context_payload,
                                    }
                                )
                            )
                        except Exception:
                            pass
                        configs = []
                    except Exception:
                        configs = []
                    configs_cache[domain] = configs
                patch, records = _build_job_detail_heuristic_patch(job, configs or [], heuristic_time_ms)
                enriched.append({**job, **patch})
                for rec in records:
                    try:
                        record_job_detail_heuristic_step(rec)
                    except asyncio.CancelledError:
                        # Best-effort; do not fail ingestion on cancelled heuristic logging.
                        try:
                            telemetry.emit_posthog_log(
                                _strip_none_values(
                                    {
                                        "event": "heuristic.record_cancelled",
                                        "level": "warning",
                                        "url": job.get("url")
                                        or (rec.get("jobUrl") if isinstance(rec, dict) else None),
                                        **context_payload,
                                    }
                                )
                            )
                        except Exception:
                            pass
                        continue
                    except Exception:
                        # best-effort; do not block ingestion
                        continue
            return enriched

        payload = trim_scrape_for_convex(
            scrape,
            max_description=2000,
            max_title_chars=200,
            raw_preview_chars=0,
            request_max_chars=1000,
            collect_page_links=False,
        )
        SIZE_WARN_BYTES = 900_000

        def _estimate_payload_size(value: Any) -> int:
            try:
                return len(orjson.dumps(value))
            except Exception:
                try:
                    return len(str(value))
                except Exception:
                    return 0

        def _log_payload_size(label: str, data: Dict[str, Any]) -> None:
            total = _estimate_payload_size(data)
            if total < SIZE_WARN_BYTES:
                return
            size_entries: list[tuple[str, int]] = []
            for key in ("items", "request", "providerRequest", "response", "asyncResponse", "subUrls"):
                if key in data:
                    size_entries.append((key, _estimate_payload_size(data.get(key))))
            items_block = data.get("items")
            if isinstance(items_block, dict):
                for key in (
                    "normalized",
                    "normalizedSample",
                    "job_urls",
                    "seedUrls",
                    "page_links",
                    "raw",
                    "rawPreview",
                ):
                    if key in items_block:
                        size_entries.append((f"items.{key}", _estimate_payload_size(items_block.get(key))))
            top = sorted(size_entries, key=lambda entry: entry[1], reverse=True)[:6]
            top_display = ", ".join(f"{name}={size}" for name, size in top if size)
            logger.warning(
                "Scrape payload %s is large (%s bytes). Top fields: %s",
                label,
                total,
                top_display or "n/a",
            )

        _log_payload_size("trimmed", payload)
        raw_items_block = scrape.get("items") if isinstance(scrape, dict) else None
        now = int(time.time() * 1000)
        normalized_count = 0
        normalized_items = None
        if isinstance(raw_items_block, dict):
            normalized_items = raw_items_block.get("normalized")
        if isinstance(normalized_items, list):
            normalized_count = len(normalized_items)
        elif isinstance(payload.get("items"), dict):
            normalized_items = payload["items"].get("normalized")
            if isinstance(normalized_items, list):
                normalized_count = len(normalized_items)
        ignored_count = 0
        ignored_items = None
        if isinstance(raw_items_block, dict):
            ignored_items = raw_items_block.get("ignored")
        if isinstance(ignored_items, list):
            ignored_count = len(ignored_items)
        if not ignored_count and isinstance(raw_items_block, dict):
            ignored_meta = raw_items_block.get("ignoredCount")
            if isinstance(ignored_meta, int):
                ignored_count = ignored_meta
        failed_count = 0
        failed_reasons: list[str] = []
        failed_items = None
        if isinstance(raw_items_block, dict):
            failed_items = raw_items_block.get("failed")
        if isinstance(failed_items, list):
            failed_count = len(failed_items)
            for entry in failed_items:
                if isinstance(entry, dict):
                    reason = entry.get("reason")
                    if isinstance(reason, str) and reason:
                        failed_reasons.append(reason)
        if not failed_count and isinstance(raw_items_block, dict):
            failed_meta = raw_items_block.get("failedCount")
            if isinstance(failed_meta, int):
                failed_count = failed_meta
    
        scraped_with = None
        if isinstance(payload.get("items"), dict):
            scraped_with = payload["items"].get("provider")
        scraped_with = scraped_with or payload.get("provider")
        workflow_name = payload.get("workflowName")
        cost_milli_cents = payload.get("costMilliCents")
        if cost_milli_cents is None and isinstance(payload.get("items"), dict):
            maybe_cost = payload["items"].get("costMilliCents")
            if isinstance(maybe_cost, (int, float)):
                cost_milli_cents = int(maybe_cost)
        # Support costCents fallback
        if cost_milli_cents is None and payload.get("costCents") is not None:
            try:
                cost_milli_cents = int(float(payload["costCents"]) * 1000)
            except Exception:
                cost_milli_cents = None
        response_preview = None
        async_response_preview = None
        items_provider = None
        if isinstance(payload.get("items"), dict):
            items_provider = payload["items"].get("provider") or payload["items"].get("crawlProvider")
        provider_for_log = scraped_with or payload.get("provider") or items_provider
        invalid_reason = None
        failure_reason = None
        if workflow_name == "SpidercloudJobDetails" and normalized_count == 0 and ignored_count == 0:
            if failed_count:
                if failed_reasons and all(reason == "captcha_failed" for reason in failed_reasons):
                    failure_reason = "captcha_failed"
                else:
                    failure_reason = "scrape_failed"
            else:
                invalid_reason = "no_normalized_jobs"
    
        _log_workflow_event(
            "scrape.received",
            message=(
                f"Scrape payload received for {payload.get('sourceUrl') or 'unknown site'} "
                f"via {provider_for_log or 'unknown provider'}"
            ),
            data={
                "workflowId": payload.get("workflowId"),
                "provider": provider_for_log,
                "normalizedCount": normalized_count,
                "ignoredCount": ignored_count or None,
                "siteId": payload.get("siteId"),
            },
        )
        if failed_count:
            _log_workflow_event(
                "scrape.failed_urls",
                message="Scrape payload contained failed URLs",
                data={
                    "workflowId": payload.get("workflowId"),
                    "provider": provider_for_log,
                    "failedCount": failed_count,
                    "failedSample": failed_items[:10] if isinstance(failed_items, list) else None,
                    "siteId": payload.get("siteId"),
                },
                level="warning",
            )
    
        def _extract_source_url_from_raw(raw_value: Any) -> str:
            if isinstance(raw_value, dict):
                raw_url = raw_value.get("url")
                if isinstance(raw_url, str) and raw_url.strip():
                    return raw_url
            if isinstance(raw_value, list):
                for entry in raw_value:
                    nested = entry if isinstance(entry, list) else [entry]
                    for item in nested:
                        if isinstance(item, dict):
                            raw_url = item.get("url")
                            if isinstance(raw_url, str) and raw_url.strip():
                                return raw_url
            return ""

        def _resolve_source_url(data: Dict[str, Any]) -> str:
            """Best-effort source URL extraction that tolerates missing fields."""

            for key in ("sourceUrl", "sourceURL", "source_url", "siteUrl", "url"):
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    return val

            request_block = data.get("request")
            if isinstance(request_block, dict):
                req_url = request_block.get("url")
                if isinstance(req_url, str) and req_url.strip():
                    return req_url

            provider_request = data.get("providerRequest")
            if isinstance(provider_request, dict):
                req_url = provider_request.get("url")
                if isinstance(req_url, str) and req_url.strip():
                    return req_url

            items_block = data.get("items")
            if isinstance(items_block, dict):
                raw_val = items_block.get("raw")
                raw_url = _extract_source_url_from_raw(raw_val)
                if raw_url:
                    return raw_url

            return ""

    
        def _base_payload(data: Dict[str, Any]) -> Dict[str, Any]:
            source_url = _resolve_source_url(data)
            body = {
                "sourceUrl": source_url,
                "startedAt": data.get("startedAt", now),
                "completedAt": data.get("completedAt", now),
                "items": data.get("items"),
            }
            if data.get("siteId") is not None:
                body["siteId"] = data.get("siteId")
            provider_value = scraped_with
            if provider_value is None:
                provider_value = data.get("provider")
            if provider_value is None and isinstance(data.get("items"), dict):
                provider_value = data["items"].get("provider")
            if provider_value is not None:
                body["provider"] = str(provider_value)
            workflow_value = data.get("workflowName")
            if workflow_value is None:
                workflow_value = workflow_name
            if workflow_value is not None:
                body["workflowName"] = str(workflow_value)
            pattern = data.get("pattern")
            if pattern is not None:
                body["pattern"] = pattern
            if data.get("request") is not None:
                body["request"] = data.get("request")
            if data.get("providerRequest") is not None:
                body["providerRequest"] = data.get("providerRequest")
            if cost_milli_cents is not None:
                body["costMilliCents"] = cost_milli_cents
            if response_preview is not None:
                body["response"] = response_preview
            if async_response_preview is not None:
                body["asyncResponse"] = async_response_preview
            if data.get("asyncState") is not None:
                body["asyncState"] = data.get("asyncState")
            if data.get("batchId") is not None:
                body["batchId"] = data.get("batchId")
            if data.get("workflowId") is not None:
                body["workflowId"] = data.get("workflowId")
            if data.get("workflowType") is not None:
                body["workflowType"] = data.get("workflowType")
            if data.get("jobBoardJobId") is not None:
                body["jobBoardJobId"] = data.get("jobBoardJobId")
            if data.get("subUrls") is not None:
                body["subUrls"] = data.get("subUrls")
            return body
    
        def _build_invalid_context() -> Dict[str, Any]:
            items_block = scrape.get("items") if isinstance(scrape, dict) else {}
            raw_block = items_block.get("raw") if isinstance(items_block, dict) else None
            raw_items: List[Any] = []
            if isinstance(raw_block, list):
                raw_items = raw_block
            elif isinstance(raw_block, dict):
                raw_items = [raw_block]
    
            markdown_samples: List[Any] = []
            html_samples: List[Any] = []
            event_samples: List[Any] = []
            link_samples: List[Any] = []
            markdown_lengths: List[int] = []
            event_counts: List[int] = []
            link_counts: List[int] = []
    
            def _add_markdown(value: Any) -> None:
                if isinstance(value, str) and value.strip():
                    markdown_samples.append(_shrink_payload(value, 12000))
    
            def _add_html(value: Any) -> None:
                if isinstance(value, str) and value.strip():
                    html_samples.append(_shrink_payload(value, 12000))
    
            for raw in raw_items[:2]:
                if not isinstance(raw, dict):
                    continue
                markdown_val = raw.get("markdown") or raw.get("commonmark") or raw.get("content")
                _add_markdown(markdown_val)
                if isinstance(markdown_val, str):
                    markdown_lengths.append(len(markdown_val))
                _add_html(raw.get("raw_html") or raw.get("html"))
                events_val = raw.get("events")
                if events_val is not None:
                    event_samples.append(_shrink_payload(events_val, 6000))
                    if isinstance(events_val, list):
                        event_counts.append(len(events_val))
                job_urls = raw.get("job_urls") or raw.get("links")
                if isinstance(job_urls, list) and job_urls:
                    link_samples.append(job_urls[:50])
                    link_counts.append(len(job_urls))
    
            if isinstance(items_block, dict):
                markdown_val = items_block.get("markdown") or items_block.get("commonmark") or items_block.get("content")
                _add_markdown(markdown_val)
                if isinstance(markdown_val, str):
                    markdown_lengths.append(len(markdown_val))
                _add_html(items_block.get("raw_html") or items_block.get("html"))
                if isinstance(items_block.get("job_urls"), list):
                    job_urls = items_block.get("job_urls")
                    link_samples.append(job_urls[:50])
                    link_counts.append(len(job_urls))
                if isinstance(items_block.get("seedUrls"), list):
                    seed_urls = items_block.get("seedUrls")
                    link_samples.append(seed_urls[:50])
                    link_counts.append(len(seed_urls))
    
            response_preview = _shrink_payload(scrape.get("response"), 12000)
            async_response_preview = _shrink_payload(scrape.get("asyncResponse"), 12000)
            provider_request_preview = _shrink_payload(scrape.get("providerRequest"), 6000)
            request_preview = _shrink_payload(scrape.get("request"), 6000)
    
            sub_urls = scrape.get("subUrls")
            sub_url_sample = sub_urls[:20] if isinstance(sub_urls, list) else None
            failed_entries: list[Any] = []
            failed_count_local = 0
            if isinstance(items_block, dict):
                failed_items_local = items_block.get("failed")
                if isinstance(failed_items_local, list):
                    failed_entries = failed_items_local[:10]
                    failed_count_local = len(failed_items_local)
    
            return _strip_none_values(
                {
                    "reason": invalid_reason or failure_reason,
                    "normalizedCount": normalized_count,
                    "ignoredCount": ignored_count,
                    "failedCount": failed_count_local,
                    "provider": provider_for_log,
                    "workflowName": workflow_name,
                    "siteId": payload.get("siteId"),
                    "sourceUrl": payload.get("sourceUrl"),
                    "pattern": payload.get("pattern"),
                    "subUrlsSample": sub_url_sample,
                    "failedSamples": failed_entries or None,
                    "rawItemsCount": len(raw_items),
                    "markdownLengths": markdown_lengths or None,
                    "eventCounts": event_counts or None,
                    "linkCounts": [int(count) for count in link_counts] if len(link_counts) else None,
                    "requestedFormat": items_block.get("requestedFormat") if isinstance(items_block, dict) else None,
                    "markdownSamples": markdown_samples or None,
                    "htmlSamples": html_samples or None,
                    "eventSamples": event_samples or None,
                    "linkSamples": link_samples or None,
                    "response": response_preview,
                    "asyncResponse": async_response_preview,
                    "providerRequest": provider_request_preview,
                    "request": request_preview,
                }
            )
    
        scrape_id: str | None = None
        try:
            scrape_id = insert_scrape_record_step(_base_payload(payload))
            _log_workflow_event(
                "scrape.persisted",
                message=(
                    f"Persisted scrape with {normalized_count} normalized jobs "
                    f"({provider_for_log or 'unknown provider'})"
                ),
                data={
                    "scrapeId": scrape_id,
                    "workflowId": payload.get("workflowId"),
                    "normalizedCount": normalized_count,
                    "provider": provider_for_log,
                    "siteId": payload.get("siteId"),
                },
            )
        except Exception as exc:
            logger.warning("insertScrapeRecord failed; retrying with trimmed payload: %s", exc, exc_info=exc)
            try:
                telemetry.emit_posthog_exception(
                    exc,
                    properties={
                        "event": "scrape.persist_failed",
                        "siteUrl": payload.get("sourceUrl") or "",
                        "provider": provider_for_log,
                        "workflowName": workflow_name,
                    },
                )
            except Exception:
                pass
            # Fallback: aggressively trim and retry once so we still record the run
            fallback = trim_scrape_for_convex(
                scrape,
                max_items=100,
                max_description=400,
                raw_preview_chars=0,
                request_max_chars=500,
                collect_page_links=False,
            )
            if isinstance(fallback.get("items"), dict):
                fallback["items"]["truncated"] = True
            try:
                scrape_id = insert_scrape_record_step(_base_payload(fallback))
                _log_workflow_event(
                    "scrape.persisted.fallback",
                    message=f"Persisted fallback scrape after initial failure ({provider_for_log or 'unknown provider'})",
                    data={
                        "scrapeId": scrape_id,
                        "workflowId": payload.get("workflowId"),
                        "normalizedCount": normalized_count,
                        "provider": provider_for_log,
                        "siteId": payload.get("siteId"),
                    },
                )
            except Exception as fallback_exc:
                logger.error(
                    "Failed to persist scrape after fallback: %s",
                    fallback_exc,
                    exc_info=fallback_exc,
                )
                try:
                    telemetry.emit_posthog_exception(
                        fallback_exc,
                        properties={
                            "event": "scrape.persist_failed.final",
                            "siteUrl": payload.get("sourceUrl") or "",
                            "provider": provider_for_log,
                            "workflowName": workflow_name,
                        },
                    )
                except Exception:
                    pass
                error_id = f"store-error:{int(time.time() * 1000)}"
                raise ApplicationError(
                    f"Failed to persist scrape after fallback ({error_id})",
                    type="store_scrape_failed",
                ) from fallback_exc
    
        # Best-effort job ingestion (mimics router.ts behavior)
        try:
            # Ingest jobs from the original (untrimmed) scrape items so long descriptions are preserved.
            # Still cap the number of jobs we attempt to ingest to avoid unbounded payloads.
            MAX_JOBS_TO_INGEST = 400
            items_for_jobs = scrape.get("items") if isinstance(scrape, dict) else None
            if isinstance(items_for_jobs, dict):
                normalized = items_for_jobs.get("normalized")
                if isinstance(normalized, list):
                    items_for_jobs = {**items_for_jobs, "normalized": normalized[:MAX_JOBS_TO_INGEST]}
            else:
                items_for_jobs = payload.get("items")
    
            jobs = _jobs_from_scrape_items(
                items_for_jobs,
                default_posted_at=now,
                scraped_at=payload.get("completedAt", now),
                scraped_with=scraped_with,
                workflow_name=workflow_name,
                scraped_cost_milli_cents=(
                    int(cost_milli_cents / max(len(payload.get("items", {}).get("normalized") or []) or 1, 1))
                    if isinstance(cost_milli_cents, (int, float))
                    else None
                ),
            )
            if jobs:
                try:
                    jobs = _apply_job_detail_heuristics_to_jobs(
                        jobs,
                        now,
                        {
                            "workflowId": payload.get("workflowId"),
                            "workflowName": workflow_name,
                            "runId": payload.get("runId") or payload.get("run_id"),
                            "siteUrl": payload.get("sourceUrl"),
                        },
                    )
                except asyncio.CancelledError as exc:
                    try:
                        telemetry.emit_posthog_exception(
                            exc,
                            properties={
                                "event": "ingest.job_heuristics_cancelled",
                                "siteUrl": payload.get("sourceUrl") or "",
                                "provider": provider_for_log,
                                "workflowName": workflow_name,
                                "workflowId": payload.get("workflowId"),
                                "runId": payload.get("runId"),
                                "normalizedCount": normalized_count,
                            },
                        )
                    except Exception:
                        pass
                    raise
                except Exception as exc:
                    try:
                        telemetry.emit_posthog_exception(
                            exc,
                            properties={
                                "event": "ingest.job_heuristics_failed",
                                "siteUrl": payload.get("sourceUrl") or "",
                                "provider": provider_for_log,
                                "workflowName": workflow_name,
                                "workflowId": payload.get("workflowId"),
                                "runId": payload.get("runId"),
                                "normalizedCount": normalized_count,
                            },
                        )
                    except Exception:
                        pass
                    # Heuristics are best-effort; continue with raw jobs if parsing fails.
                    pass
                INGEST_CHUNK_SIZE = 100
                site_id = payload.get("siteId")
                for start in range(0, len(jobs), INGEST_CHUNK_SIZE):
                    chunk = jobs[start : start + INGEST_CHUNK_SIZE]
                    # Log each job URL being posted to Convex
                    job_urls = [job.get("url") for job in chunk if isinstance(job, dict) and job.get("url")]
                    logger.info("Posting %d job detail(s) to Convex: %s", len(chunk), ", ".join(job_urls[:5]) + ("..." if len(job_urls) > 5 else ""))

                    # Truncate descriptions for DB row (full descriptions go to file storage separately)
                    truncated_chunk = [
                        {**job, "description": build_description_preview(job.get("description", ""))}
                        if isinstance(job, dict) else job
                        for job in chunk
                    ]
                    ingest_jobs_from_scrape_step(jobs=truncated_chunk, site_id=site_id)
                _log_workflow_event(
                    "ingest.jobs",
                    message=(
                        f"Ingested {len(jobs)} jobs into Convex "
                        f"from {payload.get('sourceUrl') or 'unknown site'}"
                    ),
                    data={
                        "count": len(jobs),
                        "workflowId": payload.get("workflowId"),
                        "siteId": payload.get("siteId"),
                        "provider": provider_for_log,
                    },
                )
                try:
                    _store_job_descriptions_via_http(
                        jobs,
                        payload.get("sourceUrl") if isinstance(payload.get("sourceUrl"), str) else None,
                        provider_for_log,
                        workflow_name,
                        _log_workflow_event,
                    )
                except Exception as exc:
                    logger.warning(
                        "Description storage upload failed for %s: %s",
                        payload.get("sourceUrl") or "unknown site",
                        exc,
                    )
        except asyncio.TimeoutError:
            try:
                telemetry.emit_posthog_log(
                    _strip_none_values(
                        {
                            "event": "ingest.jobs_timeout",
                            "level": "warning",
                            "siteUrl": payload.get("sourceUrl") or "",
                            "provider": provider_for_log,
                            "workflowName": workflow_name,
                            "normalizedCount": normalized_count,
                        }
                    )
                )
            except Exception:
                pass
            logger.warning(
                "ingestJobsFromScrape timed out for %s",
                payload.get("sourceUrl") or "unknown site",
            )
            # Non-fatal: ingestion timeouts shouldn't block scrape recording
            pass
        except asyncio.CancelledError as exc:
            try:
                telemetry.emit_posthog_exception(
                    exc,
                    properties={
                        "event": "ingest.jobs_cancelled",
                        "siteUrl": payload.get("sourceUrl") or "",
                        "provider": provider_for_log,
                        "workflowName": workflow_name,
                        "normalizedCount": normalized_count,
                        **_activity_cancellation_payload(),
                    },
                )
            except Exception:
                pass
            is_cancelled = getattr(activity, "is_cancelled", None)
            if callable(is_cancelled) and is_cancelled():
                raise
            # Non-fatal: ingestion failures shouldn't block scrape recording
            pass
        except Exception as exc:
            try:
                telemetry.emit_posthog_exception(
                    exc,
                    properties={
                        "event": "ingest.jobs_failed",
                        "siteUrl": payload.get("sourceUrl") or "",
                        "provider": provider_for_log,
                        "workflowName": workflow_name,
                        "normalizedCount": normalized_count,
                    },
                )
            except Exception:
                pass
            # Non-fatal: ingestion failures shouldn't block scrape recording
            pass
    
        # Record ignored entries (e.g., filtered by keyword) so future crawls can skip quickly.
        try:
            def _is_http_404_entry(entry: Dict[str, Any]) -> bool:
                status = entry.get("status") or entry.get("httpStatus")
                if isinstance(status, (int, float)) and int(status) == 404:
                    return True
                reason = entry.get("reason")
                if isinstance(reason, str) and "404" in reason.lower():
                    return True
                error_type = entry.get("errorType")
                return isinstance(error_type, str) and "404" in error_type.lower()

            ignored_entries = []
            ignored_recorded = 0
            if isinstance(raw_items_block, dict):
                ignored_entries = raw_items_block.get("ignored") or []
            if isinstance(ignored_entries, list):
                for entry in ignored_entries:
                    if not isinstance(entry, dict):
                        continue
                    if _is_http_404_entry(entry):
                        continue
                    url_val = entry.get("url")
                    if not isinstance(url_val, str) or not url_val.strip():
                        continue
                    title_val = entry.get("title")
                    desc_val = entry.get("description")
                    if not isinstance(title_val, str) or not title_val.strip():
                        title_val = "Unknown"
                    if isinstance(desc_val, str) and len(desc_val) > 4000:
                        desc_val = desc_val[:4000]
                    insert_ignored_job_step(
                        url=url_val.strip(),
                        source_url=payload.get("sourceUrl") or payload.get("pattern") or "",
                        reason=entry.get("reason") or "filtered",
                        provider=scraped_with or payload.get("provider"),
                        workflow_name=payload.get("workflowName"),
                        details=_shrink_payload(entry, 4000),
                        title=title_val,
                        description=desc_val,
                    )
                    ignored_recorded += 1
            if ignored_recorded:
                _log_workflow_event(
                    "scrape.ignored_jobs",
                    message=f"Recorded {ignored_recorded} ignored jobs for {payload.get('sourceUrl') or 'unknown'}",
                    data={
                        "count": ignored_recorded,
                        "workflowId": payload.get("workflowId"),
                        "siteId": payload.get("siteId"),
                        "provider": provider_for_log,
                    },
                )
        except Exception as exc:
            try:
                telemetry.emit_posthog_exception(
                    exc,
                    properties={
                        "event": "ignored_jobs.record_failed",
                        "siteUrl": payload.get("sourceUrl") or "",
                        "provider": provider_for_log,
                        "workflowName": workflow_name,
                        "ignoredCount": ignored_count,
                    },
                )
            except Exception:
                pass
            # Best-effort; ignore failures
            pass

        urls: list[str] = []
        job_urls: list[str] = []
        listing_urls: list[str] = []
        had_listing_urls = False
        extracted_job_urls: list[str] = []
        existing_job_set_ready = False
    
        # Best-effort enqueue of job URLs discovered in scrape payloads (e.g., Greenhouse listings).
        try:
            urls_from_raw = _extract_job_urls_from_scrape(scrape)
            _log_workflow_event(
                "scrape.url_extraction.raw",
                message="Attempted URL extraction from raw scrape payload",
                data={"urls": len(urls_from_raw or []), "sourceUrl": payload.get("sourceUrl")},
            )
    
            urls_from_trimmed = _extract_job_urls_from_scrape(payload) if not urls_from_raw else []
            if not urls_from_raw:
                _log_workflow_event(
                    "scrape.url_extraction.trimmed",
                    message="Attempted URL extraction from trimmed payload",
                    data={"urls": len(urls_from_trimmed or []), "sourceUrl": payload.get("sourceUrl")},
                )
    
            urls = urls_from_raw or urls_from_trimmed or []
            if not urls and isinstance(raw_items_block, dict):
                raw_payload = raw_items_block.get("raw")
                if isinstance(raw_payload, dict):
                    raw_job_urls = raw_payload.get("job_urls") or raw_payload.get("jobUrls")
                    if isinstance(raw_job_urls, list):
                        urls = [
                            url
                            for url in raw_job_urls
                            if isinstance(url, str) and url.strip()
                        ]
            source_url = payload.get("sourceUrl")
            if not isinstance(source_url, str) or not source_url:
                source_url = scrape.get("sourceUrl")
            if not isinstance(source_url, str) or not source_url:
                source_url = _resolve_source_url(scrape)
            handler = get_site_handler(source_url) if isinstance(source_url, str) and source_url else None
            pattern = payload.get("pattern")
            if not isinstance(pattern, str) or not pattern.strip():
                pattern = None

            if urls:
                had_listing_urls = any(
                    handler.is_listing_url(url) if handler else _is_probable_listing_url(url)
                    for url in urls
                    if isinstance(url, str)
                )
                if not had_listing_urls and isinstance(source_url, str) and source_url:
                    if handler and handler.is_listing_url(source_url):
                        had_listing_urls = True
                    elif not handler and _is_probable_listing_url(source_url):
                        had_listing_urls = True
                urls = _filter_job_urls(
                    urls,
                    handler,
                    _is_probable_listing_url,
                    pattern=pattern,
                    source_url=source_url,
                )
                if not urls and (urls_from_raw or urls_from_trimmed):
                    fallback_urls = [
                        url
                        for url in (urls_from_raw or urls_from_trimmed)
                        if isinstance(url, str) and url.strip()
                    ]
                    urls = _filter_job_urls(
                        fallback_urls,
                        handler,
                        _is_probable_listing_url,
                        pattern=pattern,
                        source_url=source_url,
                    )
                if not urls and isinstance(raw_items_block, dict):
                    raw_payload = raw_items_block.get("raw")
                    if isinstance(raw_payload, dict):
                        raw_job_urls = raw_payload.get("job_urls") or raw_payload.get("jobUrls")
                        if isinstance(raw_job_urls, list):
                            urls = _filter_job_urls(
                                raw_job_urls,
                                handler,
                                _is_probable_listing_url,
                                pattern=pattern,
                                source_url=source_url,
                            )
                    if not urls:
                        raw_links = extract_links_from_payload(
                            raw_payload,
                            collect_all=True,
                            scan_strings=True,
                        )
                        if raw_links:
                            normalized_links = [
                                normalized
                                for link in raw_links
                                if (normalized := normalize_url(link, base_url=source_url))
                            ]
                        else:
                            normalized_links = []
                        if not normalized_links and isinstance(raw_payload, (dict, list, str)):
                            for text in gather_strings(raw_payload):
                                if not isinstance(text, str) or "http" not in text:
                                    continue
                                for match in re.findall(URL_PATTERN, text):
                                    lower = match.lower()
                                    if not any(token in lower for token in ("/job", "/jobs", "/career")):
                                        continue
                                    normalized = normalize_url(match, base_url=source_url)
                                    if normalized:
                                        normalized_links.append(normalized)
                        if normalized_links:
                            urls = _filter_job_urls(
                                normalized_links,
                                handler,
                                _is_probable_listing_url,
                                pattern=pattern,
                            )
            delays_ms: list[int] | None = None
            url_types: list[str] | None = None
            if urls:
                url_types = []
                for url in urls:
                    is_listing = False
                    if handler:
                        is_listing = handler.is_listing_url(url)
                    else:
                        is_listing = _is_probable_listing_url(url)
                    if is_listing:
                        listing_urls.append(url)
                        url_types.append("listing")
                    else:
                        job_urls.append(url)
                        url_types.append("detail")
                extracted_job_urls = list(dict.fromkeys(job_urls))
                if listing_urls:
                    listing_urls = []
                    had_listing_urls = False
                    urls = list(job_urls)
                    if url_types is not None:
                        url_types = ["detail"] * len(job_urls)
                if listing_urls and isinstance(source_url, str) and source_url:
                    seen_listing: set[str] = set()
                    try:
                        seen_listing = set(
                            u
                            for u in fetch_seen_urls_for_site(
                                source_url,
                                payload.get("pattern"),
                            )
                            if isinstance(u, str)
                        )
                    except Exception:
                        seen_listing = set()
                    if seen_listing:
                        listing_candidates = set(listing_urls)
                        seen_listing = seen_listing.intersection(listing_candidates)
                    if seen_listing:
                        urls_before_seen = urls
                        url_types_before_seen = url_types
                        urls = [u for u in urls_before_seen if u not in seen_listing]
                        if url_types_before_seen:
                            filtered_pairs = [
                                (u, t)
                                for u, t in zip(urls_before_seen, url_types_before_seen)
                                if u not in seen_listing
                            ]
                            if filtered_pairs:
                                urls, url_types = zip(*filtered_pairs)
                                urls = list(urls)
                                url_types = list(url_types)
                            else:
                                urls = []
                                url_types = []
                        listing_urls = [
                            u
                            for u in urls
                            if (handler and handler.is_listing_url(u))
                            or (not handler and _is_probable_listing_url(u))
                        ]
                        job_urls = [u for u in urls if u not in listing_urls]
                        if url_types is None:
                            url_types = [
                                "listing" if u in listing_urls else "detail" for u in urls
                            ]
                if listing_urls:
                    delay_map: Dict[str, int] = {}
                    delay_idx = 1
                    for url in urls:
                        if handler and handler.is_listing_url(url):
                            delay_map[url] = delay_idx * PAGINATION_ENQUEUE_STAGGER_MS
                            delay_idx += 1
                    if delay_map:
                        delays_ms = [delay_map.get(url, 0) for url in urls]
                        if not any(delays_ms):
                            delays_ms = None
                _log_workflow_event(
                    "scrape.url_split",
                    message="Split scrape URLs into job and listing groups",
                    data={
                        "sourceUrl": source_url or "",
                        "jobUrlsCount": len(job_urls),
                        "listingUrlsCount": len(listing_urls),
                        "jobUrlsSample": job_urls[:3],
                        "listingUrlsSample": listing_urls[:3],
                    },
                )
            if not job_urls and normalized_count == 0:
                company_name = None
                for key in ("company", "companyName"):
                    val = payload.get(key)
                    if isinstance(val, str) and val.strip():
                        company_name = val.strip()
                        break
                if not company_name and isinstance(source_url, str) and source_url:
                    derived_company = derive_company_from_url(source_url)
                    if derived_company:
                        company_name = derived_company
                already_saved_urls: list[str] = []
                for items_block in (raw_items_block, payload.get("items")):
                    if not isinstance(items_block, dict):
                        continue
                    for key in (
                        "existing",
                        "alreadySaved",
                        "already_saved",
                        "alreadySavedUrls",
                        "already_saved_urls",
                    ):
                        candidates = items_block.get(key)
                        if not isinstance(candidates, list):
                            continue
                        for candidate in candidates:
                            if isinstance(candidate, str) and candidate.strip():
                                already_saved_urls.append(candidate.strip())
                        break
                already_saved_count = len(already_saved_urls)
                already_saved_note = (
                    f" (alreadySavedUrlCount={already_saved_count}; "
                    "job detail URLs already saved in jobs table)"
                    if already_saved_count
                    else ""
                )

                skip_all_message = (
                    f"All job URLs skipped; already stored in Convex{already_saved_note}"
                    if already_saved_count
                    else f"No job URLs extracted from job site scrape{already_saved_note}"
                )
                skip_all_event = "scrape.job_urls.skipped" if already_saved_count else "scrape.job_urls.none"
                skip_all_level = "debug" if already_saved_count else "error"

                _log_workflow_event(
                    skip_all_event,
                    message=skip_all_message,
                    data=_strip_none_values(
                        {
                            "companyName": company_name or "Unknown",
                            "details": _strip_none_values(
                                {
                                    "sourceUrl": source_url or "",
                                    "provider": provider_for_log,
                                    "workflowName": workflow_name,
                                    "pattern": payload.get("pattern"),
                                    "siteId": payload.get("siteId"),
                                    "normalizedCount": normalized_count,
                                    "ignoredCount": ignored_count,
                                    "failedCount": failed_count,
                                    "totalUrlsCount": len(urls),
                                    "jobUrlsCount": len(job_urls),
                                    "listingUrlsCount": len(listing_urls),
                                    "alreadySavedUrlCount": already_saved_count or None,
                                    "alreadySavedUrlSample": (
                                        already_saved_urls[:5] if already_saved_urls else None
                                    ),
                                    "urlsSample": urls[:5] if urls else None,
                                    "listingUrlsSample": listing_urls[:5] if listing_urls else None,
                                }
                            ),
                        }
                    ),
                    level=skip_all_level,
                )
            _log_workflow_event(
                "scrape.url_extraction.summary",
                message="Scrape URL extraction summary",
                data={
                    "sourceUrl": source_url or "",
                    "urlCount": len(urls),
                    "urlSample": urls[:5] if urls else None,
                },
            )
            # Use filter_new_job_urls for efficiency - returns only non-existing URLs (less network transfer)
            skip_existing_job_filter = bool(handler and handler.name == "ashby") or had_listing_urls
            new_job_urls_set: set[str] = set()
            if job_urls and not skip_existing_job_filter:
                try:
                    new_job_urls = filter_new_job_urls(job_urls)
                    new_job_urls_set = set(new_job_urls)
                except Exception:
                    new_job_urls_set = set(job_urls)  # On error, proceed with all URLs
            else:
                new_job_urls_set = set(job_urls) if job_urls else set()
            existing_job_set_ready = True
            if job_urls and len(new_job_urls_set) < len(job_urls):
                job_urls = [u for u in job_urls if u in new_job_urls_set]
                urls = [u for u in urls if u in new_job_urls_set]

            if not job_urls and isinstance(raw_items_block, dict):
                raw_payload = raw_items_block.get("raw")
                if isinstance(raw_payload, dict):
                    raw_job_urls = raw_payload.get("job_urls") or raw_payload.get("jobUrls")
                    if isinstance(raw_job_urls, list):
                        filtered_job_urls = _filter_job_urls(
                            raw_job_urls,
                            handler,
                            _is_probable_listing_url,
                            pattern=pattern,
                        )
                        for url in filtered_job_urls:
                            if handler and handler.is_listing_url(url):
                                continue
                            if not handler and _is_probable_listing_url(url):
                                continue
                            job_urls.append(url)
            if job_urls and not listing_urls:
                urls = job_urls
                url_types = ["detail"] * len(job_urls)
            if urls:
                site_id = _convex_site_id(payload.get("siteId"))
                dbos_queue.enqueue_scrape_urls(
                    _strip_none_values(
                        {
                            "urls": urls,
                            "sourceUrl": payload.get("sourceUrl") or "",
                            "provider": scraped_with or payload.get("provider") or "",
                            "siteId": site_id,
                            "pattern": payload.get("pattern"),
                            "delaysMs": delays_ms,
                            "urlTypes": url_types,
                        }
                    )
                )
                _log_workflow_event(
                    "scrape.url_enqueue",
                    message="Enqueued URLs from scrape payload",
                    data={"urls": len(urls), "sourceUrl": payload.get("sourceUrl")},
                )
            else:
                _log_workflow_event(
                    "scrape.url_extraction.none",
                    message="No URLs extracted from scrape payload",
                    data={"sourceUrl": payload.get("sourceUrl")},
                )
        except Exception as exc:
            _log_workflow_event(
                "scrape.url_extraction.error",
                message="Failed to enqueue URLs from scrape payload",
                data={"error": str(exc), "sourceUrl": payload.get("sourceUrl")},
            )
            # Non-fatal

        if invalid_reason == "no_normalized_jobs" and extracted_job_urls:
            unique_job_urls = list(dict.fromkeys(extracted_job_urls))
            # Use filter_new_job_urls for efficiency - returns only non-existing URLs (less network transfer)
            new_urls_set = new_job_urls_set if existing_job_set_ready else None
            if new_urls_set is None:
                try:
                    new_urls = filter_new_job_urls(unique_job_urls)
                    new_urls_set = set(new_urls)
                except Exception:
                    new_urls_set = set(unique_job_urls)  # On error, assume all are new
            # If no new URLs, all jobs already exist
            if len(new_urls_set) == 0:
                invalid_reason = None
                _log_workflow_event(
                    "scrape.jobs_skipped",
                    message="All jobs skipped; URLs already stored in Convex",
                    data={
                        "sourceUrl": payload.get("sourceUrl") or "",
                        "jobUrlsCount": len(unique_job_urls),
                        "jobUrlsSample": unique_job_urls[:5],
                        "provider": provider_for_log,
                        "workflowName": workflow_name,
                    },
                    level="debug",
                )

        if invalid_reason or failure_reason:
            try:
                telemetry.emit_posthog_exception(
                    ValueError(f"Scrape payload invalid: {invalid_reason or failure_reason}"),
                    properties={
                        "event": "scrape.invalid_payload" if invalid_reason else "scrape.failed_payload",
                        "siteUrl": payload.get("sourceUrl") or "",
                        "provider": provider_for_log,
                        "workflowName": workflow_name,
                        "normalizedCount": normalized_count,
                        "ignoredCount": ignored_count,
                        "failedCount": failed_count,
                        "reason": invalid_reason or failure_reason,
                    },
                )
            except Exception:
                pass
    
        if invalid_reason:
            _log_workflow_event(
                "scrape.invalid",
                message=f"Invalid scrape: {invalid_reason}",
                data=_build_invalid_context(),
                level="error",
            )
            raise ApplicationError(
                f"Invalid scrape: {invalid_reason}",
                non_retryable=True,
                type="invalid_scrape",
            )
    
        if failure_reason:
            _log_workflow_event(
                "scrape.failed",
                message=f"Scrape failed: {failure_reason}",
                data=_build_invalid_context(),
                level="error",
            )
            raise ApplicationError(
                f"Scrape failed: {failure_reason}",
                type=failure_reason,
            )
    
        return str(scrape_id)


    except asyncio.CancelledError as exc:
        try:
            props = {}
            if isinstance(scrape, dict):
                props = {
                    "event": "scrape.store_scrape_cancelled",
                    "siteUrl": scrape.get("sourceUrl") or "",
                    "workflowName": scrape.get("workflowName"),
                    "workflowId": scrape.get("workflowId") or scrape.get("workflow_id"),
                    "runId": scrape.get("runId") or scrape.get("run_id"),
                    "provider": scrape.get("provider"),
                    **_activity_cancellation_payload(),
                }
            telemetry.emit_posthog_exception(exc, properties=_strip_none_values(props))
        except Exception:
            pass
        raise

def _extract_job_urls_from_scrape(scrape: Dict[str, Any]) -> list[str]:
    """Heuristic extraction of job URLs (Greenhouse or plain HTML) from a scrape payload."""

    md_link_re = re.compile(MARKDOWN_LINK_PATTERN)
    greenhouse_re = re.compile(GREENHOUSE_URL_PATTERN, re.IGNORECASE)
    confluent_job_re = re.compile(CONFLUENT_JOB_PATH_PATTERN, re.IGNORECASE)
    confluent_page_re = re.compile(r"/jobs/?\?page=\d+", re.IGNORECASE)
    location_line_re = re.compile(LOCATION_LINE_PATTERN, re.IGNORECASE)
    apply_text_re = re.compile(APPLY_WORD_PATTERN, re.IGNORECASE)
    dash_separators: Tuple[str, ...] = (" - ", " | ", " — ", " – ")

    class _AnchorParser(HTMLParser):  # noqa: N801
        def __init__(self):
            super().__init__()
            self.links: list[tuple[str, str]] = []
            self._current_href: str | None = None
            self._text_parts: list[str] = []

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag.lower() != "a":
                return
            href = None
            for key, val in attrs:
                if key.lower() == "href":
                    href = val
                    break
            if href:
                self._current_href = href
                self._text_parts = []

        def handle_data(self, data: str) -> None:
            if self._current_href is not None:
                self._text_parts.append(data)

        def handle_endtag(self, tag: str) -> None:
            if tag.lower() != "a" or self._current_href is None:
                return
            text = "".join(self._text_parts).strip()
            self.links.append((self._current_href, text))
            self._current_href = None
            self._text_parts = []

    def _split_title_and_location(text: str) -> tuple[Optional[str], Optional[str]]:
        if not text:
            return None, None
        val = text.strip()
        paren_match = re.match(TITLE_LOCATION_PAREN_PATTERN, val)
        if paren_match:
            return paren_match.group(1).strip() or None, paren_match.group(2).strip() or None
        in_bar_match = re.match(TITLE_IN_BAR_PATTERN, val, flags=re.IGNORECASE)
        if in_bar_match:
            title = in_bar_match.group("title").strip() or None
            location = in_bar_match.group("location").strip() or None
            if location and ("," in location or "remote" in location.lower()):
                return title, location
            return title, None
        for sep in dash_separators:
            if sep in val:
                left, right = val.rsplit(sep, 1)
                return (left.strip() or None, right.strip() or None)
        return val, None

    def _line_has_job_link(line: str) -> bool:
        for match in md_link_re.finditer(line):
            title_text = match.group(1).strip()
            if not title_text:
                continue
            title, _ = _split_title_and_location(title_text)
            if title_matches_required_keywords(title or title_text):
                return True
        return False

    def _extract_location_from_context(lines: list[str], anchor_idx: int) -> Optional[str]:
        max_offset = 5

        for offset in range(1, max_offset + 1):
            idx = anchor_idx + offset
            if idx >= len(lines):
                break
            if _line_has_job_link(lines[idx]):
                break
            match = location_line_re.search(lines[idx])
            if match:
                return match.group("location").strip()

        for offset in range(1, max_offset + 1):
            idx = anchor_idx - offset
            if idx < 0:
                break
            if _line_has_job_link(lines[idx]):
                break
            match = location_line_re.search(lines[idx])
            if match:
                return match.group("location").strip()

        return None

    def _looks_like_job_detail_url(url: str) -> bool:  # noqa: F811
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        query = (parsed.query or "").lower()
        if "gh_jid=" in query:
            return True
        host = (parsed.hostname or "").lower()
        path = parsed.path
        lower = (path or "").lower()
        if host.endswith("confluent.io"):
            return "/jobs/job/" in lower
        if host.endswith("ashbyhq.com"):
            segments = [seg for seg in lower.split("/") if seg]
            return len(segments) >= 2
        if not any(token in lower for token in ("/job", "/jobs", "/career", "/careers", "/position", "/positions")):
            return False
        segments = [seg for seg in lower.split("/") if seg]
        for idx, seg in enumerate(segments):
            if seg in {"job", "jobs", "career", "careers", "position", "positions"}:
                return idx + 1 < len(segments)
        return False

    def _looks_like_job_or_listing_url(url: str) -> bool:
        if _looks_like_job_detail_url(url):
            return True
        if handler and handler.is_listing_url(url):
            return True
        return _is_probable_listing_url(url)

    def _is_ashby_url(url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith("ashbyhq.com")

    def _looks_like_location_filter_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return False
        segments = [seg for seg in (parsed.path or "").split("/") if seg]
        for idx, seg in enumerate(segments[:-1]):
            if seg not in {"job", "jobs"}:
                continue
            slug = segments[idx + 1].lower()
            if slug.startswith(("united_states", "united-states")) and not re.search(DIGIT_PATTERN, slug):
                return True
        return False

    def _looks_like_confluent_listing_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return False
        segments = [seg for seg in (parsed.path or "").split("/") if seg]
        if not segments or segments[0] != "jobs":
            return False
        if len(segments) == 1:
            return True
        slug = segments[1].lower()
        if slug == "job":
            return False
        return not re.search(DIGIT_PATTERN, slug)

    def _confluent_page_value(url: str) -> Optional[int]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return None
        params = parse_qs(parsed.query)
        values = params.get("page")
        if not values:
            return None
        try:
            page_val = int(values[0])
        except Exception:
            return None
        return page_val if page_val > 0 else None

    def _is_confluent_pagination_url(url: str) -> bool:
        page_val = _confluent_page_value(url)
        if page_val is None or page_val < 2:
            return False
        current_page = _confluent_page_value(source_url) if isinstance(source_url, str) else None
        if current_page is not None and page_val == current_page:
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return False
        path = (parsed.path or "").lower()
        if not path.startswith("/jobs"):
            return False
        return "/jobs/job/" not in path

    def _canonicalize_confluent_pagination_url(url: str) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return url
        path = parsed.path or ""
        if path == "/jobs":
            path = "/jobs/"
        return parsed._replace(path=path).geturl()

    _NON_JOB_PATH_SEGMENTS = {
        "acceptable-use",
        "cookie",
        "cookie-policy",
        "cookies",
        "legal",
        "notice",
        "notices",
        "policy",
        "privacy",
        "privacy-policy",
        "terms",
        "terms-of-service",
        "tos",
    }

    def _looks_like_non_job_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
        if not path:
            return False
        if host.endswith(".convex.site") and path.startswith("/share/job"):
            return True
        if host.endswith("linkedin.com") and path.startswith("/company"):
            return True
        if host.endswith("confluent.io"):
            if path in {"/", ""}:
                return True
            if path.startswith("/early-talent"):
                return True
        if host.endswith("confluent.io") and path.rstrip("/") == "/careers":
            return True
        segments = [seg for seg in path.split("/") if seg]
        for seg in segments:
            if seg in _NON_JOB_PATH_SEGMENTS:
                return True
            if seg.startswith(("privacy", "terms", "tos", "cookie", "legal", "notice")):
                return True
        return False

    def _looks_like_apply_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if host.endswith("ashbyhq.com"):
            return False
        segments = [seg for seg in (parsed.path or "").split("/") if seg]
        return any(seg in {"apply", "application", "hvhapply"} for seg in segments)

    def _should_ignore_url(url: str) -> bool:
        return (
            _looks_like_location_filter_url(url)
            or (_looks_like_confluent_listing_url(url) and not _is_confluent_pagination_url(url))
            or _looks_like_non_job_url(url)
            or _looks_like_apply_url(url)
            or _looks_like_auth_url(url)
        )

    def _looks_like_apply_link(title_text: str | None, url: str) -> bool:
        if title_text and apply_text_re.search(title_text):
            return True
        lower = url.lower()
        return any(token in lower for token in ("/apply", "/login", "/register", "/signup"))

    def _extract_markdown_links_with_context(
        text: str,
    ) -> list[tuple[str, Optional[str], Optional[str], str, Optional[str]]]:
        links: list[tuple[str, Optional[str], Optional[str], str, Optional[str]]] = []
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            if "[" not in line or "](" not in line:
                continue
            for match in md_link_re.finditer(line):
                title_text = match.group(1).strip()
                url = match.group(2).strip()
                start = max(0, idx - 4)
                end = min(len(lines), idx + 5)
                context_lines: list[str] = []
                for j in range(start, end):
                    raw = lines[j]
                    if not raw.strip():
                        continue
                    if j != idx and md_link_re.search(raw):
                        continue
                    context_lines.append(raw.strip())
                context_text = " ".join(context_lines)
                title, loc = _split_title_and_location(title_text)
                context_location = _extract_location_from_context(lines, idx)
                links.append((url, title or title_text, loc, context_text, context_location))
        return links

    def _strip_code_fences(value: str) -> str:
        stripped = value.strip()
        if stripped.startswith("```"):
            stripped = re.sub(CODE_FENCE_START_PATTERN, "", stripped)
            stripped = re.sub(CODE_FENCE_END_PATTERN, "", stripped)
            return stripped.strip()
        fence_match = re.search(
            CODE_FENCE_CONTENT_PATTERN,
            value,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if fence_match:
            return fence_match.group("content").strip()
        return value

    def _clean_invalid_json_escapes(value: str) -> str:
        return re.sub(INVALID_JSON_ESCAPE_PATTERN, "", value)

    def _parse_raw_json_value(value: Any) -> Any | None:
        if isinstance(value, (dict, list)):
            return value
        if not isinstance(value, str) or not value.strip():
            return None
        cleaned = _clean_invalid_json_escapes(_strip_code_fences(value))
        try:
            return orjson.loads(cleaned)
        except Exception:
            parsed_items: list[Any] = []
            for line in cleaned.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed_line = orjson.loads(line)
                except Exception:
                    continue
                parsed_items.append(parsed_line)
            if parsed_items:
                return parsed_items
            return None

    def _collect_parsed_raw_values(value: Any) -> list[Any]:
        parsed_values: list[Any] = []

        def _add(parsed: Any | None) -> None:
            if parsed is None:
                return
            if isinstance(parsed, list):
                for entry in parsed:
                    _add(entry)
                return
            parsed_values.append(parsed)

        if isinstance(value, list):
            for entry in value:
                _add(_parse_raw_json_value(entry))
        else:
            _add(_parse_raw_json_value(value))
        return parsed_values

    url_re = re.compile(URL_PATTERN)

    def _extract_from_text(text: str) -> list[tuple[str, Optional[str], Optional[str]]]:
        links: list[tuple[str, Optional[str], Optional[str]]] = []
        markdown_urls: set[str] = set()
        if md_link_re.search(text):
            for match in md_link_re.finditer(text):
                raw_url = match.group(2).strip()
                if not raw_url:
                    continue
                cleaned_url = strip_wrapping_url(raw_url).rstrip(").,]")
                if cleaned_url:
                    markdown_urls.add(cleaned_url)

        parser = _AnchorParser()
        try:
            parser.feed(text)
        except Exception:
            # best-effort; ignore parsing failures
            parser.close()
        for href, anchor_text in parser.links:
            title, loc = _split_title_and_location(anchor_text)
            links.append((href.strip(), title, loc))

        for match in greenhouse_re.findall(text):
            if "jobs" not in match:
                continue
            links.append((match.strip(), None, None))

        if is_confluent:
            for match in confluent_job_re.findall(text):
                links.append((match.strip(), None, None))
            for match in confluent_page_re.findall(text):
                links.append((match.strip(), None, None))

        for match in url_re.findall(text):
            lower = match.lower()
            if "/job" not in lower and "/jobs/" not in lower and "/position" not in lower:
                continue
            cleaned = match.strip()
            cleaned = strip_wrapping_url(cleaned).rstrip(").,]")
            if cleaned in markdown_urls:
                continue
            links.append((cleaned, None, None))

        relative_re = re.compile(r"/(?:careers?/job|jobs)/(?!search)([^\"'<>\s]+)", re.IGNORECASE)
        for match in relative_re.finditer(text):
            # Skip if this match is inside a full URL (preceded by ://)
            # e.g., don't extract /jobs/123 from https://example.com/jobs/123
            start_pos = match.start()
            prefix = text[max(0, start_pos - 100) : start_pos]
            if "://" in prefix and not any(c in prefix[prefix.rfind("://"):] for c in " \n\t"):
                continue
            relative_url = match.group(0).strip()
            if relative_url in markdown_urls:
                continue
            links.append((relative_url, None, None))

        return links

    def _extract_ashby_job_urls(text: str) -> list[str]:
        if "window.__appData" not in text:
            return []

        def _find_slug(raw_text: str, payload: Dict[str, Any]) -> Optional[str]:
            org = payload.get("organization") if isinstance(payload, dict) else None
            if isinstance(org, dict):
                slug_val = org.get("hostedJobsPageSlug")
                if isinstance(slug_val, str) and slug_val.strip():
                    return slug_val.strip()
            match = re.search(ASHBY_JOB_SLUG_PATTERN, raw_text, re.IGNORECASE)
            return match.group(1).strip() if match else None

        def _load_app_data(raw_text: str) -> Optional[Dict[str, Any]]:
            marker = "window.__appData"
            start = raw_text.find(marker)
            if start == -1:
                return None
            brace_start = raw_text.find("{", start)
            if brace_start == -1:
                return None
            try:
                stack: list[str] = []
                in_string = False
                escape = False
                end = None
                for idx in range(brace_start, len(raw_text)):
                    ch = raw_text[idx]
                    if in_string:
                        if escape:
                            escape = False
                            continue
                        if ch == "\\":
                            escape = True
                            continue
                        if ch == '"':
                            in_string = False
                        continue
                    if ch == '"':
                        in_string = True
                        continue
                    if ch in "{[":
                        stack.append(ch)
                    elif ch in "}]":
                        if not stack:
                            break
                        open_ch = stack.pop()
                        if (open_ch == "{" and ch != "}") or (open_ch == "[" and ch != "]"):
                            break
                        if not stack:
                            end = idx + 1
                            break
                if end is None:
                    return None
                parsed = orjson.loads(raw_text[brace_start:end])
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None

        payload = _load_app_data(text)
        if not payload:
            return []

        slug = _find_slug(text, payload)
        if not slug:
            return []
        slug = slug.strip().lower()

        job_ids: set[str] = set()

        def _walk(node: Any) -> None:
            if isinstance(node, dict):
                job_id = None
                for key in ("jobPostingId", "id", "jobId"):
                    candidate = node.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        job_id = candidate.strip()
                        break
                title = node.get("title")
                is_listed = node.get("isListed")
                if (
                    isinstance(job_id, str)
                    and isinstance(title, str)
                    and title.strip()
                    and (is_listed is None or is_listed is True)
                ):
                    if title_matches_required_keywords(title):
                        job_ids.add(job_id.strip())
                for child in node.values():
                    _walk(child)
            elif isinstance(node, list):
                for child in node:
                    _walk(child)

        _walk(payload)

        return [f"https://jobs.ashbyhq.com/{slug}/{job_id}" for job_id in sorted(job_ids)]

    def _extract_source_url_from_raw(raw_value: Any) -> str:
        if isinstance(raw_value, dict):
            raw_url = raw_value.get("url")
            if isinstance(raw_url, str) and raw_url.strip():
                return raw_url
        if isinstance(raw_value, list):
            for entry in raw_value:
                nested = entry if isinstance(entry, list) else [entry]
                for item in nested:
                    if isinstance(item, dict):
                        raw_url = item.get("url")
                        if isinstance(raw_url, str) and raw_url.strip():
                            return raw_url
        return ""

    candidates: list[str] = []
    link_urls: list[str] = []
    pagination_urls: list[str] = []
    items = scrape.get("items") if isinstance(scrape, dict) else {}
    source_url = scrape.get("sourceUrl") if isinstance(scrape, dict) else ""
    if (not isinstance(source_url, str) or not source_url) and isinstance(items, dict):
        source_url = _extract_source_url_from_raw(items.get("raw"))
    source_host = urlparse(source_url).hostname if source_url else None
    is_confluent = bool(source_host and source_host.endswith("confluent.io"))
    handler = get_site_handler(source_url) if source_url else None
    has_raw_html = False

    def _dedupe_raw_urls(values: Iterable[Any]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not isinstance(value, str):
                continue
            normalized = normalize_url(value, base_url=source_url)
            cleaned = normalized or strip_wrapping_url(value)
            if not cleaned or cleaned in seen:
                continue
            # Filter out non-job URLs (social media, convex share links, privacy pages, etc.)
            if is_invalid_job_url(cleaned):
                continue
            seen.add(cleaned)
            deduped.append(cleaned)
        return deduped

    def _dedupe_ashby_urls(values: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        for value in values:
            cleaned = _strip_ashby_application_url(value)
            seen.add(cleaned)
        return sorted(seen)

    def _normalize_job_url(value: str, *, base_url: str | None = None) -> str | None:
        normalized = normalize_url(value, base_url=base_url)
        if not normalized:
            return None
        if base_url:
            try:
                normalized_parsed = urlparse(normalized)
                base_parsed = urlparse(base_url)
            except Exception:
                normalized_parsed = None
                base_parsed = None
            if (
                normalized_parsed
                and base_parsed
                and not normalized_parsed.query
                and base_parsed.query
                and normalized_parsed.scheme == base_parsed.scheme
                and normalized_parsed.netloc == base_parsed.netloc
                and (normalized_parsed.path or "").rstrip("/")
                == (base_parsed.path or "").rstrip("/")
            ):
                preferred = normalize_url(base_url, base_url=base_url)
                if preferred:
                    normalized = preferred
        return _strip_ashby_application_url(normalized)

    def _normalize_job_url_list(values: Iterable[str], *, base_url: str | None = None) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not isinstance(value, str):
                continue
            normalized_url = _normalize_job_url(value, base_url=base_url)
            if not normalized_url or normalized_url in seen:
                continue
            seen.add(normalized_url)
            normalized.append(normalized_url)
        return normalized

    def _merge_pagination_urls(values: list[str]) -> list[str]:
        if not pagination_urls:
            return values
        merged = list(values)
        normalized = _normalize_job_url_list(pagination_urls, base_url=source_url)
        normalized = [url for url in normalized if not _should_ignore_url(url)]
        normalized_source = _normalize_job_url(source_url, base_url=source_url) if source_url else None
        for url in normalized:
            if normalized_source and url == normalized_source:
                continue
            if _is_confluent_pagination_url(url):
                url = _canonicalize_confluent_pagination_url(url)
            if url not in merged:
                merged.append(url)
        return merged

    def _normalize_handler_links(links: list[str]) -> list[str]:
        if not links:
            return []
        normalized = _normalize_job_url_list(links, base_url=source_url)
        if handler:
            normalized = handler.filter_job_urls(normalized)
        normalized = _normalize_job_url_list(normalized, base_url=source_url)
        normalized = [url for url in normalized if not _should_ignore_url(url)]
        normalized = _merge_pagination_urls(normalized)
        normalized = BaseSiteHandler.drop_source_listing_url(normalized, source_url)
        return normalized

    def _normalize_direct_url(value: str) -> str | None:
        normalized = normalize_url(value, base_url=source_url)
        if normalized:
            return _strip_ashby_application_url(normalized)
        cleaned = strip_wrapping_url(value.strip())
        if not cleaned:
            return None
        cleaned = cleaned.replace("\\", "/")
        lower = cleaned.lower()
        if lower.startswith(("mailto:", "tel:", "javascript:", "#")):
            return None
        if cleaned.startswith(("http://", "https://")):
            return _strip_ashby_application_url(cleaned)
        if cleaned.startswith("//"):
            if not source_url:
                return None
            scheme = urlparse(source_url).scheme or "https"
            return _strip_ashby_application_url(f"{scheme}:{cleaned}")
        if source_url:
            return _strip_ashby_application_url(urljoin(source_url, cleaned))
        return None

    is_fetchfox_crawl = False
    if isinstance(items, dict):
        crawl_provider = items.get("crawlProvider")
        if isinstance(crawl_provider, str) and crawl_provider.lower().startswith("fetchfox"):
            is_fetchfox_crawl = True
    if not is_fetchfox_crawl and isinstance(scrape, dict):
        provider_val = scrape.get("provider")
        if isinstance(provider_val, str) and provider_val.lower() == "fetchfox-crawl":
            is_fetchfox_crawl = True

    if is_fetchfox_crawl and isinstance(items, dict):
        job_urls_val = items.get("job_urls")
        if not isinstance(job_urls_val, list):
            job_urls_val = items.get("jobUrls")
        if isinstance(job_urls_val, list):
            return _dedupe_raw_urls(job_urls_val)
        raw_urls_val = items.get("rawUrls")
        if isinstance(raw_urls_val, list):
            return _dedupe_raw_urls(raw_urls_val)
        urls_val = items.get("urls")
        if isinstance(urls_val, list):
            return _dedupe_raw_urls(urls_val)

    if isinstance(items, dict):
        job_urls_val = items.get("job_urls")
        if not isinstance(job_urls_val, list):
            job_urls_val = items.get("jobUrls")
        if isinstance(job_urls_val, list) and job_urls_val:
            deduped = _dedupe_raw_urls(job_urls_val)
            if handler:
                filtered = handler.filter_job_urls(deduped)
                if filtered:
                    return filtered
            return deduped

    def _collect_html_candidates(value: Any) -> list[str]:
        candidates: list[str] = []

        def _walk(node: Any) -> None:
            if isinstance(node, dict):
                for key in ("raw_html", "html"):
                    val = node.get(key)
                    if isinstance(val, str) and val.strip():
                        candidates.append(val)
                for child in node.values():
                    _walk(child)
            elif isinstance(node, list):
                for child in node:
                    _walk(child)
            elif isinstance(node, str):
                lower = node.lower()
                if "<" in node and ">" in node and not (
                    "<http://" in lower or "<https://" in lower or "<mailto:" in lower
                ):
                    candidates.append(node)

        _walk(value)
        return candidates

    def _extract_handler_links(values: Iterable[str], *, allow_markdown: bool = True) -> list[str]:
        if not handler or getattr(handler, "name", "") == "ashby":
            return []
        html_candidates: list[str] = []
        markdown_candidates: list[str] = []
        for text in values:
            if not isinstance(text, str) or not text.strip():
                continue
            lower = text.lower()
            looks_like_html = "<" in text and ">" in text and not (
                "<http://" in lower or "<https://" in lower or "<mailto:" in lower
            )
            if looks_like_html:
                html_candidates.append(text)
            markdown_candidates.append(text)

        links: list[str] = []
        seen_links: set[str] = set()

        for text in html_candidates:
            for link in handler.get_links_from_raw_html(text):
                if link and link not in seen_links:
                    seen_links.add(link)
                    links.append(link)

        if allow_markdown:
            for text in markdown_candidates:
                for link in handler.get_links_from_markdown(text):
                    if link and link not in seen_links:
                        seen_links.add(link)
                        links.append(link)
        return links

    def _extract_pagination_payload(value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict):
            jobs_val = value.get("jobs")
            if isinstance(jobs_val, list):
                return value
            positions_val = value.get("positions")
            if isinstance(positions_val, list):
                return value
        for text in (t for t in gather_strings(value) if isinstance(t, str) and t.strip()):
            if "<pre" in text.lower():
                payload = BaseSiteHandler._extract_json_payload_from_html(text)  # noqa: SLF001
                if isinstance(payload, dict):
                    return payload
            try:
                parsed = orjson.loads(text)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                jobs_val = parsed.get("jobs")
                if isinstance(jobs_val, list):
                    return parsed
                positions_val = parsed.get("positions")
                if isinstance(positions_val, list):
                    return parsed
        return None

    if isinstance(items, dict):
        link_urls = []
        raw_job_urls = items.get("job_urls") or items.get("jobUrls")
        if isinstance(raw_job_urls, list):
            link_urls.extend([link for link in raw_job_urls if isinstance(link, str) and link.strip()])
        raw_links = items.get("links") or items.get("page_links")
        if isinstance(raw_links, list):
            link_urls.extend([link for link in raw_links if isinstance(link, str) and link.strip()])

        raw_val = items.get("raw")
        parsed_raw_values = _collect_parsed_raw_values(raw_val)
        raw_html_candidates = _collect_html_candidates(raw_val)
        has_raw_html = bool(raw_html_candidates)
        if isinstance(raw_val, dict):
            raw_job_urls = raw_val.get("job_urls") or raw_val.get("jobUrls")
            if isinstance(raw_job_urls, list):
                link_urls.extend(
                    [link for link in raw_job_urls if isinstance(link, str) and link.strip()]
                )
        if parsed_raw_values:
            for parsed_value in parsed_raw_values:
                if not isinstance(parsed_value, dict):
                    continue
                raw_job_urls = parsed_value.get("job_urls") or parsed_value.get("jobUrls")
                if isinstance(raw_job_urls, list):
                    link_urls.extend(
                        [link for link in raw_job_urls if isinstance(link, str) and link.strip()]
                    )
                raw_links_val = parsed_value.get("links") or parsed_value.get("page_links")
                if isinstance(raw_links_val, list):
                    link_urls.extend(
                        [link for link in raw_links_val if isinstance(link, str) and link.strip()]
                    )
        raw_links = extract_links_from_payload(raw_val)
        if not raw_links:
            raw_links = extract_links_from_payload(
                raw_val,
                collect_all=True,
                scan_strings=not has_raw_html,
            )
        if parsed_raw_values:
            parsed_links = extract_links_from_payload(
                parsed_raw_values,
                collect_all=True,
                scan_strings=False,
            )
            if parsed_links:
                raw_links.extend(parsed_links)
        if raw_links:
            link_urls.extend(raw_links)
        if not link_urls and isinstance(raw_val, (dict, list, str)):
            relative_re = re.compile(r"/(?:careers?/job|jobs)/(?!search)([^\"'<>\s]+)", re.IGNORECASE)
            for text in gather_strings(raw_val):
                if not isinstance(text, str):
                    continue
                for match in relative_re.finditer(text):
                    # Skip if this match is inside a full URL (preceded by ://)
                    # e.g., don't extract /jobs/123 from https://example.com/jobs/123
                    start_pos = match.start()
                    prefix = text[max(0, start_pos - 100) : start_pos]
                    if "://" in prefix and not any(c in prefix[prefix.rfind("://"):] for c in " \n\t"):
                        continue
                    normalized = normalize_url(match.group(0), base_url=source_url)
                    if normalized:
                        link_urls.append(normalized)
        if handler:
            json_payload: dict[str, Any] | None = None
            if isinstance(raw_val, dict):
                json_payload = raw_val
            elif isinstance(raw_val, str) and raw_val.strip():
                json_payload = BaseSiteHandler._extract_json_payload_from_html(raw_val)  # noqa: SLF001
                if json_payload is None:
                    try:
                        parsed = orjson.loads(raw_val)
                    except Exception:
                        parsed = None
                    if isinstance(parsed, dict):
                        json_payload = parsed
            elif isinstance(raw_val, list):
                for entry in raw_val:
                    nested_items = entry if isinstance(entry, list) else [entry]
                    for item in nested_items:
                        if isinstance(item, dict):
                            content = item.get("content")
                            if isinstance(content, dict):
                                for key in ("raw", "raw_html", "html", "text", "body"):
                                    text = content.get(key)
                                    if isinstance(text, str) and text.strip():
                                        json_payload = BaseSiteHandler._extract_json_payload_from_html(text)  # noqa: SLF001
                                        if json_payload is not None:
                                            break
                                # Also check commonmark field (used by Kula and other APIs that return JSON in markdown)
                                if json_payload is None:
                                    commonmark_text = content.get("commonmark")
                                    if isinstance(commonmark_text, str) and commonmark_text.strip():
                                        # commonmark often contains JSON wrapped in code fences
                                        json_payload = _parse_raw_json_value(commonmark_text)
                            if json_payload is not None:
                                break
                        if isinstance(item, str) and item.strip():
                            json_payload = BaseSiteHandler._extract_json_payload_from_html(item)  # noqa: SLF001
                            if json_payload is None:
                                parsed_item = _parse_raw_json_value(item)
                                if isinstance(parsed_item, dict):
                                    json_payload = parsed_item
                        if json_payload is not None:
                            break
                    if json_payload is not None:
                        break
            if not json_payload and parsed_raw_values:
                for parsed_value in parsed_raw_values:
                    if isinstance(parsed_value, dict):
                        json_payload = parsed_value
                        break
            if json_payload:
                handler_urls = handler.get_links_from_json(json_payload)
                if handler_urls:
                    link_urls.extend(handler_urls)
        if link_urls:
            link_urls = _normalize_job_url_list(link_urls, base_url=source_url)
            if handler:
                link_urls = handler.filter_job_urls(link_urls)
            link_urls = _normalize_job_url_list(link_urls, base_url=source_url)
            link_urls = [url for url in link_urls if not _should_ignore_url(url)]
            link_urls = [url for url in link_urls if _looks_like_job_or_listing_url(url)]
            if (handler and handler.name == "ashby") or (
                source_host and source_host.endswith("ashbyhq.com")
            ):
                link_urls = _dedupe_ashby_urls(link_urls)
        link_urls = BaseSiteHandler.drop_source_listing_url(link_urls, source_url)
        if handler:
            pagination_payload = _extract_pagination_payload(raw_val)
            if pagination_payload:
                pagination_urls = handler.get_pagination_urls_from_json(pagination_payload, source_url)
            if not pagination_urls:
                pagination_urls = handler.get_pagination_urls_from_listing(source_url)
        if is_confluent and raw_val:
            for text in gather_strings(raw_val):
                if not isinstance(text, str):
                    continue
                pagination_urls.extend(confluent_page_re.findall(text))
        parseable_content = False
        if raw_val:
            for text in gather_strings(raw_val):
                if not isinstance(text, str):
                    continue
                if "<a" in text or "](" in text:
                    parseable_content = True
                    break
        json_urls = extract_job_urls_from_json_payload(raw_val)
        if json_urls:
            json_urls = _normalize_job_url_list(json_urls, base_url=source_url)
            if handler:
                json_urls = handler.filter_job_urls(json_urls)
            merged = json_urls + link_urls
            merged = _normalize_job_url_list(merged, base_url=source_url)
            merged = [url for url in merged if not _should_ignore_url(url)]
            merged = _merge_pagination_urls(merged)
            merged = BaseSiteHandler.drop_source_listing_url(merged, source_url)
            if (handler and getattr(handler, "name", "") == "ashby") or (
                source_host and source_host.endswith("ashbyhq.com")
            ):
                merged = _dedupe_ashby_urls(merged)
            return merged
        handler_links = _extract_handler_links(
            raw_html_candidates,
            allow_markdown=not has_raw_html,
        )
        if not handler_links and not has_raw_html:
            handler_links = _extract_handler_links(
                gather_strings(raw_val),
                allow_markdown=True,
            )
        if handler_links:
            merged = handler_links + link_urls
            if handler:
                merged = handler.filter_job_urls(merged)
            merged = _normalize_job_url_list(merged, base_url=source_url)
            merged = [url for url in merged if not _should_ignore_url(url)]
            merged = _merge_pagination_urls(merged)
            merged = BaseSiteHandler.drop_source_listing_url(merged, source_url)
            return merged
    if link_urls and not parseable_content:
            merged = _merge_pagination_urls(link_urls)
            merged = BaseSiteHandler.drop_source_listing_url(merged, source_url)
            return merged

    def _handler_looks_like_job_detail_url(url: str, handler: BaseSiteHandler | None) -> bool:
        if handler and hasattr(handler, "_looks_like_job_detail_url"):
            return handler._looks_like_job_detail_url(url)
        return _looks_like_job_detail_url(url)

    if isinstance(items, dict):
        raw_val = items.get("raw")
        candidates.extend(gather_strings(raw_val))
        if "raw" in items and not raw_val and isinstance(items.get("normalized"), list):
            for job in items["normalized"]:
                candidates.extend(gather_strings(job))
        if link_urls:
            candidates.extend(link_urls)
    candidates.extend(gather_strings(scrape.get("response")))
    handler_links = _extract_handler_links(candidates, allow_markdown=not has_raw_html)
    if handler_links:
        normalized_links = _normalize_handler_links(handler_links)
        if normalized_links:
            return normalized_links
        return _merge_pagination_urls(handler_links)

    urls: list[str] = []
    seen: set[str] = set()
    blocked: set[str] = set()
    # Direct URL arrays from crawl payloads (e.g., job_urls/rawUrls) should be enqueued even if we haven't parsed titles yet.
    if isinstance(items, dict):
        for key in ("job_urls", "rawUrls", "urls"):
            url_list = items.get(key)
            if isinstance(url_list, list):
                for url_val in url_list:
                    if isinstance(url_val, str) and url_val.strip():
                        normalized_url = _normalize_direct_url(url_val)
                        if not normalized_url:
                            continue
                        if _should_ignore_url(normalized_url):
                            continue
                        if normalized_url not in seen:
                            seen.add(normalized_url)
                            urls.append(normalized_url)

    enforce_title_keywords = bool(source_url)

    for text in list(candidates):
        if isinstance(text, str):
            try:
                parsed_json = orjson.loads(
                    _clean_invalid_json_escapes(_strip_code_fences(text))
                )
            except Exception:
                parsed_json = None
            if parsed_json is not None:
                handler_json_urls: list[str] = []
                if handler:
                    handler_json_urls = handler.get_links_from_json(parsed_json)
                    if handler_json_urls:
                        handler_json_urls = handler.filter_job_urls(handler_json_urls)
                        handler_json_urls = _normalize_job_url_list(
                            handler_json_urls,
                            base_url=source_url,
                        )
                        handler_json_urls = [
                            url for url in handler_json_urls if not _should_ignore_url(url)
                        ]
                        if handler_json_urls:
                            return handler_json_urls
                json_urls = extract_job_urls_from_json_payload(parsed_json)
                if json_urls:
                    if handler:
                        json_urls = handler.filter_job_urls(json_urls)
                    json_urls = _normalize_job_url_list(json_urls, base_url=source_url)
                    json_urls = [url for url in json_urls if not _should_ignore_url(url)]
                    if json_urls:
                        return json_urls
            ashby_urls = _extract_ashby_job_urls(text)
            if ashby_urls:
                for url in ashby_urls:
                    normalized_url = _normalize_job_url(url, base_url=source_url)
                    if not normalized_url:
                        continue
                    if _should_ignore_url(normalized_url):
                        continue
                    if normalized_url not in seen:
                        seen.add(normalized_url)
                        urls.append(normalized_url)
                if (handler and handler.name == "ashby") or (
                    source_host and source_host.endswith("ashbyhq.com")
                ):
                    trimmed: list[str] = []
                    seen_trimmed: set[str] = set()
                    for url in urls:
                        cleaned = _strip_ashby_application_url(url)
                        if cleaned not in seen_trimmed:
                            seen_trimmed.add(cleaned)
                            trimmed.append(cleaned)
                    return trimmed
                return urls
            try:
                parsed = orjson.loads(
                    _clean_invalid_json_escapes(_strip_code_fences(text))
                )
                candidates.extend(gather_strings(parsed))
            except Exception:
                pass
        if not isinstance(text, str):
            continue
        for url, title, location, context_text, context_location in _extract_markdown_links_with_context(text):
            normalized_url = _normalize_job_url(url, base_url=source_url)
            if not normalized_url:
                continue
            if _should_ignore_url(normalized_url):
                continue
            if _is_confluent_pagination_url(normalized_url):
                normalized_url = _canonicalize_confluent_pagination_url(normalized_url)
                if normalized_url not in seen:
                    seen.add(normalized_url)
                    urls.append(normalized_url)
                continue
            title_match = title_matches_required_keywords(title) if title else False
            context_match = False
            if enforce_title_keywords and not title_match and context_text:
                context_match = title_matches_required_keywords(context_text)
            title_is_apply = bool(title and apply_text_re.search(title))
            if enforce_title_keywords:
                if (
                    title
                    and not title_match
                    and not context_match
                    and not (title_is_apply and _looks_like_job_detail_url(normalized_url))
                ):
                    blocked.add(normalized_url)
                    continue
            if not title and not context_match and not _looks_like_job_detail_url(normalized_url):
                continue
            if _looks_like_apply_link(title, normalized_url) and not _looks_like_job_detail_url(normalized_url):
                continue
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            urls.append(normalized_url)

        for url, title, location in _extract_from_text(text):
            normalized_url = _normalize_job_url(url, base_url=source_url)
            if not normalized_url:
                continue
            if normalized_url in blocked:
                continue
            if _should_ignore_url(normalized_url):
                continue
            if _is_confluent_pagination_url(normalized_url):
                normalized_url = _canonicalize_confluent_pagination_url(normalized_url)
                if normalized_url not in seen:
                    seen.add(normalized_url)
                    urls.append(normalized_url)
                continue
            title_match = title_matches_required_keywords(title) if title else False
            title_is_apply = bool(title and apply_text_re.search(title))
            if enforce_title_keywords:
                if (
                    title
                    and not title_match
                    and not (title_is_apply and _looks_like_job_detail_url(normalized_url))
                ):
                    blocked.add(normalized_url)
                    continue
            if _looks_like_apply_link(title, normalized_url) and not _looks_like_job_detail_url(normalized_url):
                continue
            if not title and not _looks_like_job_detail_url(normalized_url):
                continue
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            urls.append(normalized_url)

    if link_urls:
        detail_link_urls = [
            url for url in link_urls if isinstance(url, str) and _looks_like_job_detail_url(url)
        ]
        if detail_link_urls and len(urls) < len(detail_link_urls):
            if not any(url in blocked for url in detail_link_urls):
                return detail_link_urls

    return urls

@activity.defn
def process_pending_job_details_batch(limit: int = 25) -> Dict[str, Any]:  # noqa: DBOS001
    """Parse pending job descriptions with heuristics and persist learned regex configs."""

    from ...services.convex_client import convex_mutation, convex_query

    pending = convex_query("router:listPendingJobDetails", {"limit": limit}) or []
    processed = 0
    updated: List[str] = []
    errors: List[Dict[str, Any]] = []
    total = len(pending)
    configs_by_domain: Dict[str, list] = {}
    logger.info("heuristic.batch start fetched=%s limit=%s", total, limit)

    def _attempt_mutation(op_name: str, payload: Dict[str, Any], row_id: Any) -> bool:  # noqa: DBOS001
        """Run a mutation and capture errors without aborting the batch."""

        try:
            convex_mutation(op_name, payload)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "heuristic.error job id=%s op=%s err=%s",
                row_id,
                op_name,
                _describe_exception(exc),
                exc_info=True,
            )
            errors.append(
                {
                    "id": row_id,
                    "op": op_name,
                    "requestId": _extract_request_id(exc),
                    "error": _describe_exception(exc),
                }
            )
            return False

    for idx, row in enumerate(pending):
        current_op = "row:init"
        try:
            job_id = row.get("jobId") or row.get("_id")
            title = (str(row.get("title") or row.get("jobTitle") or "")).strip() or "<untitled>"
            logger.info("heuristic.view job id=%s title=%s", job_id or "<missing>", title)
            url = row.get("url") or ""
            domain = _domain_from_url(url)

            current_op = "router:listJobDetailConfigs"
            if domain in configs_by_domain:
                configs = configs_by_domain[domain]
            else:
                configs = convex_query("router:listJobDetailConfigs", {"domain": domain}) or []
                configs_by_domain[domain] = configs
            now_ms = int(time.time() * 1000)
            patch, records = _build_job_detail_heuristic_patch(row, configs, now_ms)

            for rec in records:
                _attempt_mutation("router:recordJobDetailHeuristic", rec, job_id)

            if not job_id:
                continue

            if patch:
                current_op = "router:updateJobWithHeuristic"
                did_update = _attempt_mutation("router:updateJobWithHeuristic", {"id": job_id, **patch}, job_id)
                if did_update:
                    update_summary = {
                        key: value
                        for key, value in {
                            "location": patch.get("location"),
                            "totalCompensation": patch.get("totalCompensation"),
                            "currencyCode": patch.get("currencyCode"),
                            "remote": patch.get("remote"),
                            "compensationUnknown": patch.get("compensationUnknown"),
                            "compensationReason": patch.get("compensationReason"),
                        }.items()
                        if value is not None
                    }
                    logger.info(
                        "heuristic.updated job id=%s title=%s changes=%s",
                        job_id or "<missing>",
                        title,
                        update_summary or {"note": "heuristic bookkeeping only"},
                    )
                    updated.append(job_id)
                    processed += 1

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "heuristic.error job id=%s op=%s err=%s",
                row.get("_id"),
                current_op,
                _describe_exception(exc),
                exc_info=True,
            )
            errors.append(
                {
                    "id": row.get("_id"),
                    "op": current_op,
                    "requestId": _extract_request_id(exc),
                    "error": _describe_exception(exc),
                }
            )
            continue

    remaining_after: Optional[int] = None
    try:
        op = "router:countPendingJobDetails"
        remaining_resp = convex_query(op, {})
        remaining_after = _extract_pending_count(remaining_resp)
    except Exception as exc:  # noqa: BLE001
        logger.debug("heuristic.remaining_count_failed err=%s", exc)

    remaining_label = remaining_after if remaining_after is not None else "unknown"
    logger.info(
        "heuristic.batch processed=%s updated=%s remaining=%s",
        processed,
        len(updated),
        remaining_label,
    )

    return {
        "processed": processed,
        "updated": updated,
        "remaining": remaining_after,
        "fetched": total,
        "errors": errors,
    }

@activity.defn
async def batch_store_scrapes_background(  # noqa: DBOS004 - deprecated, use batch_store_scrapes_step
    scrapes: list[dict[str, Any]],
    url_completion_data: dict[str, Any]
) -> dict[str, Any]:
    """
    Store scrapes asynchronously without blocking workflow progression.

    .. deprecated::
        Use :func:`batch_store_scrapes_step` from
        ``job_scrape_application.workflows.activities.step`` instead.

    This activity enables pipeline parallelism by allowing storage operations
    to complete in the background while the workflow continues processing
    new batches.

    Args:
        scrapes: List of scrape payloads to store
        url_completion_data: Metadata for completing URLs in queue (contains urls list)

    Returns:
        Dictionary containing:
        - operationId: Unique ID for this storage operation
        - stored: Count of successfully stored scrapes
        - scrapeIds: List of scrape IDs from Convex
        - failed: Count of failed stores
        - invalid: Count of invalid scrapes
    """
    import uuid

    logger = activity.logger
    operation_id = str(uuid.uuid4())
    scrape_ids: list[str] = []
    failed_urls: list[str] = []
    invalid_urls: list[str] = []
    completed_urls: list[str] = []

    def _extract_url_from_scrape(scrape: dict[str, Any]) -> str | None:
        """Extract URL from scrape payload for tracking."""
        # Try subUrls first
        sub_urls = scrape.get("subUrls")
        if isinstance(sub_urls, list):
            for entry in sub_urls:
                if isinstance(entry, str) and entry.strip():
                    return entry.strip()

        # Try sourceUrl
        source_val = scrape.get("sourceUrl")
        if isinstance(source_val, str) and source_val.strip():
            return source_val.strip()

        return None

    def _complete_urls_batch(urls: list[str], status: str, error: str | None = None) -> None:
        """Helper to complete URLs in queue with given status."""
        if not urls:
            return

        payload: dict[str, Any] = {
            "items": [{"url": url} for url in urls if isinstance(url, str)],
            "status": status,
        }
        if error:
            payload["error"] = error

        try:
            dbos_queue.complete_scrape_urls(payload)
        except Exception as e:
            logger.warning(f"Failed to complete URLs in queue: {e}")

    # Store scrapes concurrently with configurable limit
    # This allows us to use more Convex capacity while respecting the 128 action limit
    max_concurrent_stores = max(1, min(
        runtime_config.spidercloud_job_details_concurrency,
        10  # Cap at 10 concurrent stores per activity
    ))
    semaphore = asyncio.Semaphore(max_concurrent_stores)

    async def _store_one_scrape(  # noqa: DBOS004 - nested in deprecated parent
        scrape: dict[str, Any],
    ) -> dict[str, Any]:
        """Store a single scrape with semaphore limiting."""
        async with semaphore:
            url = _extract_url_from_scrape(scrape)
            try:
                scrape_id = store_scrape(scrape)
                return {"status": "completed", "url": url, "scrape_id": scrape_id}
            except ApplicationError as exc:
                if exc.type == "invalid_scrape":
                    logger.info(f"Invalid scrape for URL {url}: {exc}")
                    return {"status": "invalid", "url": url, "error": str(exc)}
                else:
                    logger.warning(f"Failed to store scrape for URL {url}: {exc}")
                    return {"status": "failed", "url": url, "error": str(exc)}
            except Exception as e:
                logger.error(f"Unexpected error storing scrape for URL {url}: {e}")
                return {"status": "failed", "url": url, "error": str(e)}

    # Process all scrapes concurrently (with semaphore limiting concurrency)
    valid_scrapes = [s for s in scrapes if isinstance(s, dict)]
    if valid_scrapes:
        logger.info(f"Storing {len(valid_scrapes)} scrapes with max {max_concurrent_stores} concurrent")
        tasks = [_store_one_scrape(scrape) for scrape in valid_scrapes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        for result in results:
            if isinstance(result, dict):
                status = result.get("status")
                url = result.get("url")
                scrape_id = result.get("scrape_id")

                if status == "completed" and scrape_id:
                    scrape_ids.append(scrape_id)
                    if url:
                        completed_urls.append(url)
                elif status == "invalid" and url:
                    invalid_urls.append(url)
                elif status == "failed" and url:
                    failed_urls.append(url)
            elif isinstance(result, Exception):
                logger.error(f"Unexpected exception in storage: {result}")
                # Can't extract URL from exception, so just log it

    # Complete URLs in queue with appropriate status
    _complete_urls_batch(completed_urls, "completed")
    _complete_urls_batch(invalid_urls, "invalid", error="invalid_job_data")
    _complete_urls_batch(failed_urls, "failed", error="store_failed")

    result = {
        "operationId": operation_id,
        "stored": len(scrape_ids),
        "scrapeIds": scrape_ids,
        "failed": len(failed_urls),
        "invalid": len(invalid_urls)
    }

    logger.info(
        f"Background storage complete: operation={operation_id}, "
        f"stored={len(scrape_ids)}, failed={len(failed_urls)}, invalid={len(invalid_urls)}"
    )

    return result
