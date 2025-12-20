from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import re
from urllib.parse import urlparse
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Optional, Tuple

from firecrawl import Firecrawl
from firecrawl.v2.types import PaginationConfig
from fetchfox_sdk import FetchFox
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
    MAX_JOB_DESCRIPTION_CHARS,
    _jobs_from_scrape_items,
    _shrink_payload,
    build_firecrawl_schema,
    parse_markdown_hints,
    strip_known_nav_blocks,
    fetch_seen_urls_for_site,
    normalize_fetchfox_items,
    normalize_firecrawl_items,
    trim_scrape_for_convex,
)
from ..scrapers import BaseScraper, FetchfoxScraper, FirecrawlScraper, SpiderCloudScraper
from ..site_handlers import get_site_handler
from .constants import (
    FIRECRAWL_CACHE_MAX_AGE_MS,
    FIRECRAWL_STATUS_EXPIRATION_MS,
    FIRECRAWL_STATUS_WARN_MS,
    FirecrawlJobKind,
)
from .errors import ScrapeErrorInput, clean_scrape_error_payload, log_scrape_error as _log_scrape_error
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
_log_provider_dispatch = log_provider_dispatch
_log_sync_response = log_sync_response
_build_request_snapshot = build_request_snapshot
_build_provider_status_url = build_provider_status_url
_mask_secret = mask_secret
_sanitize_headers = sanitize_headers
_trim_scrape_for_convex = trim_scrape_for_convex
_clean_scrape_error_payload = clean_scrape_error_payload

__all__ = [
    "fetch_seen_urls_for_site",
    "normalize_fetchfox_items",
    "lease_scrape_url_batch",
    "process_pending_job_details_batch",
    "process_spidercloud_job_batch",
    "complete_scrape_urls",
]

MARKDOWN_LINK_PATTERN = r"(?<!!)\[([^\]]+)\]\((https?://[^\s)]+)\)"
GREENHOUSE_URL_PATTERN = r"https?://[\w.-]*greenhouse\.io/[^\s\"'>]+"
TITLE_LOCATION_PAREN_PATTERN = r"(.+?)[\[(]\s*(.+?)\s*[\)\]]$"
LOCATION_ANYWHERE_PATTERN = r"[A-Za-z].*,\s*[A-Za-z]"
LOCATION_SPLIT_PATTERN = r"[;|/]"
LOCATION_TOKEN_SPLIT_PATTERN = r"[,\s]+"
MULTI_SPACE_PATTERN = r"\s+"
COUNTRY_CODE_PATTERN = r"^[A-Z]{2}$"
REQUEST_ID_PATTERN = r"\[Request ID:\s*([^\]]+)\]"
NON_NUMERIC_DOT_PATTERN = r"[^0-9.]"
NON_NUMERIC_PATTERN = r"[^0-9]"

INR_CURRENCY_PATTERNS = [
    r"₹",
    r"\brupees?\b",
    r"\brupee\b",
    r"\bINR\b",
    r"\blakh\b",
    r"\blpa\b",
]
GBP_CURRENCY_PATTERNS = [r"£", r"\bGBP\b"]
EUR_CURRENCY_PATTERNS = [r"€", r"\bEUR\b"]
AUD_CURRENCY_PATTERNS = [r"\bAUD\b", r"\bA\\$"]
CAD_CURRENCY_PATTERNS = [r"\bCAD\b", r"\bC\\$"]

LOCATION_FULL_PATTERN = r"(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z][A-Za-z .'-]{3,})"
LOCATION_LABEL_PATTERN = r"location[:\-\s]+(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})"
LOCATION_CITY_STATE_PATTERN = r"(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})"
LOCATION_PAREN_PATTERN = r"\((?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\)"

COMP_USD_RANGE_PATTERN = (
    r"\$\s*(?P<low>\d{2,3}(?:[.,]\d{3})?)(?:\s*[-–]\s*\$?\s*(?P<high>\d{2,3}(?:[.,]\d{3})?))?"
)
COMP_INR_RANGE_PATTERN = (
    r"[₹]\s*(?P<low>\d{1,3}(?:[.,]\d{3})?)(?:\s*[-–]\s*[₹]?\s*(?P<high>\d{1,3}(?:[.,]\d{3})?))?"
)
COMP_K_PATTERN = r"(?P<value>\d{2,3})k"
COMP_LPA_PATTERN = r"(?P<value>\d{1,3})\s*(lpa|lakh)"

SCRAPE_URL_QUEUE_TTL_MS = 48 * 60 * 60 * 1000
SPIDERCLOUD_BATCH_SIZE = runtime_config.spidercloud_job_details_batch_size
SCRAPE_URL_QUEUE_LIST_LIMIT = 500

logger = logging.getLogger("temporal.worker.activities")
scheduling_logger = logging.getLogger("temporal.scheduler")


def _strip_none_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys whose values are None so Convex does not receive nulls."""

    return {k: v for k, v in payload.items() if v is not None}


def _convex_site_id(value: Any) -> Optional[str]:
    """Return a Convex document id if the value looks valid, else None."""

    candidate = value.get("_id") if isinstance(value, dict) else value
    if isinstance(candidate, str) and _looks_like_convex_id(candidate):
        return candidate
    return None


def _safe_activity_heartbeat(details: Dict[str, Any]) -> None:
    """Send a Temporal heartbeat when running in an activity context."""

    try:
        activity.heartbeat(details)
    except RuntimeError:
        # Not running inside a Temporal activity (e.g., unit tests); ignore.
        return


def _make_fetchfox_scraper() -> FetchfoxScraper:
    return _build_fetchfox_scraper(
        build_request_snapshot=_build_request_snapshot,
        log_provider_dispatch=_log_provider_dispatch,
        log_sync_response=_log_sync_response,
    )


def _make_firecrawl_scraper() -> FirecrawlScraper:
    return _build_firecrawl_scraper(
        start_firecrawl_webhook_scrape=start_firecrawl_webhook_scrape,
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


def _to_greenhouse_marketing_url(url: str) -> Optional[str]:
    """Convert Greenhouse API detail URL to the public marketing page."""

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    host = (parsed.hostname or "").lower()
    if "greenhouse.io" not in host:
        return None

    parts = [p for p in parsed.path.split("/") if p]
    # Expected API shape: /v1/boards/{slug}/jobs/{id}
    if len(parts) >= 5 and parts[0] == "v1" and parts[1] == "boards" and parts[3] == "jobs":
        slug = parts[2]
        job_id = parts[4]
        return f"https://boards.greenhouse.io/{slug}/jobs/{job_id}"

    return None


async def select_scraper_for_site(site: Site) -> tuple[BaseScraper, Optional[List[str]]]:
    """Return the scraper instance and any precomputed skip URLs for a site."""

    scraper, skip_urls = await _select_scraper_for_site(
        site,
        make_fetchfox=_make_fetchfox_scraper,
        make_firecrawl=_make_firecrawl_scraper,
        make_spidercloud=_make_spidercloud_scraper,
    )

    # Allow callers/tests to monkeypatch fetch_seen_urls_for_site and still forward skip URLs
    if isinstance(scraper, FirecrawlScraper) and not skip_urls:
        url = site.get("url")
        if url:
            skip_urls = await fetch_seen_urls_for_site(url, site.get("pattern"))

    return scraper, skip_urls


@activity.defn
async def fetch_sites() -> List[Site]:
    from ...services.convex_client import convex_query

    res = await convex_query("router:listSites", {"enabledOnly": True})
    if not isinstance(res, list):
        raise RuntimeError(f"Unexpected sites payload: {res!r}")
    return res  # type: ignore[return-value]


@activity.defn
async def lease_site(
    worker_id: str,
    lock_seconds: int = 300,
    site_type: Optional[str] = None,
    scrape_provider: Optional[str] = None,
) -> Optional[Site]:
    from ...services.convex_client import convex_mutation

    payload: Dict[str, Any] = {"workerId": worker_id, "lockSeconds": lock_seconds}
    if site_type:
        payload["siteType"] = site_type
    if scrape_provider:
        payload["scrapeProvider"] = scrape_provider

    res = await convex_mutation("router:leaseSite", payload)
    if res is None:
        return None
    if not isinstance(res, dict):
        raise RuntimeError(f"Unexpected lease payload: {res!r}")
    try:
        scheduling_logger.info(
            "lease_site leased site_id=%s url=%s provider=%s manual_trigger_at=%s last_run_at=%s completed=%s failed=%s lock_expires_at=%s locked_by=%s",
            res.get("_id"),
            res.get("url"),
            res.get("scrapeProvider"),
            res.get("manualTriggerAt"),
            res.get("lastRunAt"),
            res.get("completed"),
            res.get("failed"),
            res.get("lockExpiresAt"),
            res.get("lockedBy"),
        )
    except Exception:
        # logging should not break leasing
        pass
    return res  # type: ignore[return-value]


@activity.defn
async def _scrape_spidercloud_greenhouse(scraper: SpiderCloudScraper, site: Site, skip_urls: list[str]) -> Dict[str, Any]:
    """Fetch Greenhouse listing via SpiderCloud and scrape individual jobs."""

    from ...services.convex_client import convex_mutation, convex_query

    listing = await scraper.fetch_greenhouse_listing(site)
    job_urls = listing.get("job_urls") if isinstance(listing, dict) else []
    urls: list[str] = [u for u in job_urls if isinstance(u, str) and u]

    seen_for_site: list[str] = []
    try:
        source_url = site.get("url") or ""
        if source_url:
            seen_for_site = await fetch_seen_urls_for_site(source_url, site.get("pattern"))
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
    existing = await filter_existing_job_urls(pending_urls)
    existing_set = set(existing) | skip_set
    urls_to_scrape = [u for u in urls if u not in existing_set]
    logger.info(
        "SpiderCloud greenhouse urls total=%s pending=%s existing=%s to_scrape=%s",
        len(urls),
        len(pending_urls),
        len(existing_set),
        len(urls_to_scrape),
    )

    # Persist URLs so they can be retried later even if the worker dies mid-scrape.
    try:
        await convex_mutation(
            "router:enqueueScrapeUrls",
            _strip_none_values(
                {
                    "urls": urls_to_scrape,
                    "sourceUrl": site.get("url") or "",
                    "provider": scraper.provider,
                    "siteId": site_id,
                    "pattern": site.get("pattern"),
                }
            ),
        )
    except Exception:
        # best-effort; continue to scrape even if enqueue fails
        pass

    # Pull queued URLs for this site/provider (pending or processing)
    queued_urls: list[Dict[str, Any]] = []
    try:
        list_args = _strip_none_values(
            {"siteId": site_id, "provider": scraper.provider, "limit": SCRAPE_URL_QUEUE_LIST_LIMIT}
        )
        batch = await convex_query("router:listQueuedScrapeUrls", list_args) or []
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
            await convex_mutation(
                "router:completeScrapeUrls",
                {"urls": stale_urls, "status": "failed", "error": "stale (>48h)"},
            )
        except Exception:
            pass

    urls_to_scrape = [u for u in fresh_urls if u not in existing_set]

    # Listing flow now only enqueues; job detail scrape handled by separate workflow.
    return {
        "provider": scraper.provider,
        "sourceUrl": site.get("url"),
        "items": {
            "normalized": [],
            "provider": scraper.provider,
            "job_urls": urls,
            "existing": list(existing_set),
            "queued": True,
            "queuedCount": len(urls_to_scrape),
        },
        "skippedUrls": stale_urls,
    }


@activity.defn
async def scrape_site(site: Site) -> Dict[str, Any]:
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
    if isinstance(scraper, SpiderCloudScraper) and site_type == "greenhouse":
        return await _scrape_spidercloud_greenhouse(scraper, site, precomputed_skip or [])

    # Tests expect skip_urls to be forwarded for firecrawl so it can dedupe visited URLs
    return await scraper.scrape_site(site, skip_urls=precomputed_skip)


@activity.defn
async def start_firecrawl_webhook_scrape(site: Site) -> Dict[str, Any]:
    """Kick off a Firecrawl batch scrape with a Convex webhook callback."""

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
        raw_start = (
            job.model_dump(mode="json", exclude_none=True)
            if hasattr(job, "model_dump")
            else {
                "jobId": getattr(job, "jobId", None),
                "statusUrl": getattr(job, "statusUrl", None),
                "status": "queued",
                "kind": kind,
                "mock": True,
            }
        )
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
                await _log_scrape_error(error_payload)
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
            await _log_scrape_error(error_payload)
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
async def crawl_site_fetchfox(site: Site) -> Dict[str, Any]:
    """Use FetchFox crawl to queue job detail URLs for SpiderCloud extraction."""

    from ...services.convex_client import convex_mutation, convex_query

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
            skip_urls = await fetch_seen_urls_for_site(source_url, pattern)
    except Exception:
        skip_urls = []

    queued_urls: list[str] = []
    try:
        per_status_limit = 250
        for status_value in ("pending", "processing"):
            queued_rows = await convex_query(
                "router:listQueuedScrapeUrls",
                _strip_none_values(
                    {"siteId": site_id, "provider": "spidercloud", "status": status_value, "limit": per_status_limit}
                ),
            )
            if isinstance(queued_rows, list):
                for row in queued_rows:
                    if isinstance(row, dict):
                        url_val = row.get("url")
                        if isinstance(url_val, str) and url_val.strip():
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
        result_obj: Dict[str, Any] | Any = result if isinstance(result, dict) else json.loads(result)
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

    existing_jobs: list[str] = []
    try:
        existing_jobs = await filter_existing_job_urls(unique_urls)
    except Exception:
        existing_jobs = []

    skip_set.update(u for u in existing_jobs if isinstance(u, str))
    candidates = [u for u in unique_urls if u not in skip_set]
    enqueued: list[str] = []
    if candidates:
        try:
            res = await convex_mutation(
                "router:enqueueScrapeUrls",
                _strip_none_values(
                  {
                    "urls": candidates,
                    "sourceUrl": source_url,
                    "provider": "spidercloud",
                    "siteId": site_id,
                    "pattern": pattern,
                  }
                ),
            )
            if isinstance(res, dict):
                queued = res.get("queued")
                if isinstance(queued, list):
                    enqueued = [u for u in queued if isinstance(u, str)]
        except Exception:
            enqueued = []

    skipped_urls = [u for u in unique_urls if u in skip_set]

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

    return {
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


@activity.defn
async def scrape_site_fetchfox(site: Site) -> Dict[str, Any]:
    scraper = _build_fetchfox_scraper()
    return await scraper.scrape_site(site)


@activity.defn
async def scrape_site_firecrawl(site: Site, skip_urls: Optional[List[str]] = None) -> Dict[str, Any]:
    scraper = _make_firecrawl_scraper()
    return await scraper.scrape_site(site, skip_urls=skip_urls)


@activity.defn
async def fetch_greenhouse_listing(site: Site) -> Dict[str, Any]:
    scraper, _ = await select_scraper_for_site(site)
    return await scraper.fetch_greenhouse_listing(site)


@activity.defn
async def fetch_greenhouse_listing_firecrawl(site: Site) -> Dict[str, Any]:
    scraper = _make_firecrawl_scraper()
    return await scraper.fetch_greenhouse_listing(site)


@activity.defn
async def filter_existing_job_urls(urls: List[str]) -> List[str]:
    """Return the subset of URLs that already exist in Convex jobs table."""

    if not urls:
        return []
    from ...services.convex_client import convex_query

    try:
        data = await convex_query("router:findExistingJobUrls", {"urls": urls})
    except Exception:
        return []

    existing = data.get("existing", []) if isinstance(data, dict) else []
    if not isinstance(existing, list):
        return []

    return [u for u in existing if isinstance(u, str)]


@activity.defn
async def complete_scrape_urls(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Mark queued scrape URLs as completed/failed in Convex."""

    from ...services.convex_client import convex_mutation

    res = await convex_mutation("router:completeScrapeUrls", payload)
    return res if isinstance(res, dict) else {"updated": 0}


@activity.defn
async def lease_scrape_url_batch(provider: Optional[str] = None, limit: int = SPIDERCLOUD_BATCH_SIZE) -> Dict[str, Any]:
    """Lease a batch of queued job-detail URLs from Convex."""

    from ...services.convex_client import convex_mutation

    res = await convex_mutation(
        "router:leaseScrapeUrlBatch",
        _strip_none_values(
            {
                "provider": provider,
                "limit": limit,
                "maxPerMinuteDefault": SPIDERCLOUD_BATCH_SIZE,
                "processingExpiryMs": runtime_config.spidercloud_job_details_processing_expire_minutes * 60 * 1000,
            }
        ),
    )
    if not isinstance(res, dict):
        return {"urls": []}

    raw_urls = res.get("urls")
    if not isinstance(raw_urls, list) or not raw_urls:
        return {"urls": []}

    skipped: list[str] = []
    filtered: list[Dict[str, Any]] = []
    skip_cache: dict[tuple[str | None, str | None], set[str]] = {}

    for entry in raw_urls:
        if not isinstance(entry, dict):
            continue
        url_val = entry.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            continue
        source_val = entry.get("sourceUrl") if isinstance(entry.get("sourceUrl"), str) else None
        pattern_val = entry.get("pattern") if isinstance(entry.get("pattern"), str) else None
        cache_key = (source_val, pattern_val)
        if cache_key not in skip_cache:
            try:
                skip_list = await fetch_seen_urls_for_site(source_val or "", pattern_val)
            except Exception:
                skip_list = []
            skip_cache[cache_key] = set(u for u in skip_list if isinstance(u, str))
        if url_val in skip_cache[cache_key]:
            skipped.append(url_val)
            continue
        filtered.append(entry)

    if skipped:
        try:
            await convex_mutation(
                "router:completeScrapeUrls",
                {"urls": skipped, "status": "failed", "error": "skip_listed_url"},
            )
        except Exception as skip_err:
            logger.warning("Failed to mark skipped URLs as failed: %s", skip_err, exc_info=skip_err)

    return {"urls": filtered, "skippedUrls": skipped}


@activity.defn
async def process_spidercloud_job_batch(batch: Dict[str, Any]) -> Dict[str, Any]:
    """Process a batch of job URLs via SpiderCloud."""

    def _to_greenhouse_api_url(url: str) -> str:
        """
        Convert Greenhouse-hosted career URLs that contain gh_jid / board params
        into the canonical boards-api.greenhouse.io detail URL. This ensures the
        SpiderCloud scraper hits the JSON API instead of the marketing site.
        """
        handler = get_site_handler(url, "greenhouse")
        if not handler:
            return url
        api_url = handler.get_api_uri(url)
        return api_url or url

    def _shrink_for_activity(scrape: Dict[str, Any]) -> Dict[str, Any]:
        """
        Trim scrape payloads before returning them to the workflow to avoid blowing
        Temporal's activity result size limits. We keep enough data for downstream
        storage/ingestion while aggressively truncating large fields.
        """

        return trim_scrape_for_convex(
            scrape,
            max_items=50,  # we only ever keep one normalized row per scrape here
            max_description=MAX_JOB_DESCRIPTION_CHARS,
            raw_preview_chars=2000,
            request_max_chars=1500,
        )

    urls: list[str] = []
    source_url = ""
    pattern = None
    for row in batch.get("urls", []):
        if isinstance(row, dict):
            url_val = row.get("url")
            if isinstance(url_val, str) and url_val.strip():
                urls.append(_to_greenhouse_api_url(url_val))
            if not source_url and isinstance(row.get("sourceUrl"), str):
                source_url = row["sourceUrl"]
            if pattern is None and isinstance(row.get("pattern"), str):
                pattern = row["pattern"]

    if not urls:
        return {"provider": "spidercloud", "items": {"normalized": []}, "sourceUrl": source_url}

    scraper = _make_spidercloud_scraper()
    payload = {"urls": urls, "source_url": source_url, "pattern": pattern}
    result = await scraper.scrape_greenhouse_jobs(payload) or {}

    # Unwrap and split into per-URL scrape payloads so they can be stored independently.
    base_payload: Dict[str, Any] | None = None
    if isinstance(result, dict):
        base_payload = result.get("scrape") if isinstance(result.get("scrape"), dict) else result  # support direct payload

    if not isinstance(base_payload, dict):
        return {"scrapes": [], "sourceUrl": source_url}

    base_payload.setdefault("provider", "spidercloud")
    base_payload.setdefault("workflowName", "SpidercloudJobDetails")

    scrapes: list[Dict[str, Any]] = []
    items = base_payload.get("items") if isinstance(base_payload, dict) else {}
    normalized = items.get("normalized") if isinstance(items, dict) else []
    raw_items = items.get("raw") if isinstance(items, dict) else []
    cost_milli_cents_total: float | None = None
    if isinstance(base_payload.get("costMilliCents"), (int, float)):
        cost_milli_cents_total = float(base_payload["costMilliCents"])
    elif isinstance(items, dict) and isinstance(items.get("costMilliCents"), (int, float)):
        cost_milli_cents_total = float(items["costMilliCents"])

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
            per_url_payload["subUrls"] = [row.get("url") or row.get("job_url") or row.get("absolute_url") or source_url]
            if per_url_cost is not None:
                per_url_payload["costMilliCents"] = per_url_cost
                per_url_payload["items"]["costMilliCents"] = per_url_cost
            scrapes.append(_shrink_for_activity(per_url_payload))
    else:
        scrapes.append(_shrink_for_activity(base_payload))

    return {"scrapes": scrapes, "sourceUrl": source_url}


@activity.defn
async def scrape_greenhouse_jobs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Scrape new Greenhouse job URLs with a single FetchFox request."""

    idempotency_key = payload.get("idempotency_key") or payload.get("webhook_id")
    if settings.spider_api_key and not idempotency_key:
        scraper = _make_spidercloud_scraper()
    elif settings.firecrawl_api_key:
        scraper = _make_firecrawl_scraper()
    else:
        scraper = _build_fetchfox_scraper()
    return await scraper.scrape_greenhouse_jobs(payload)


@activity.defn
async def scrape_greenhouse_jobs_firecrawl(payload: Dict[str, Any]) -> Dict[str, Any]:
    scraper = _make_firecrawl_scraper()
    return await scraper.scrape_greenhouse_jobs(payload)


@activity.defn
async def fetch_pending_firecrawl_webhooks(limit: int = 25, event: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return unprocessed Firecrawl webhook rows from Convex."""

    from ...services.convex_client import convex_query

    args: Dict[str, Any] = {"limit": limit}
    if event:
        args["event"] = event
    res = await convex_query("router:listPendingFirecrawlWebhooks", args)
    if not isinstance(res, list):
        return []
    return res  # type: ignore[return-value]


@activity.defn
async def get_firecrawl_webhook_status(job_id: str) -> Dict[str, Any]:
    """Return the current Convex state for a Firecrawl job's webhook rows."""

    from ...services.convex_client import convex_query

    try:
        res = await convex_query("router:getFirecrawlWebhookStatus", {"jobId": job_id})
    except Exception:
        return {}
    return res if isinstance(res, dict) else {}


@activity.defn
async def mark_firecrawl_webhook_processed(webhook_id: str, error: Optional[str] = None) -> None:
    """Mark a webhook row as processed and optionally attach an error."""

    from ...services.convex_client import convex_mutation

    payload = {"id": webhook_id}
    if error is not None:
        payload["error"] = error

    await convex_mutation(
        "router:markFirecrawlWebhookProcessed",
        payload,
    )


@activity.defn
async def collect_firecrawl_job_result(event: FirecrawlWebhookEvent) -> Dict[str, Any]:
    """Fetch Firecrawl job status and build a scrape payload."""

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
            meta = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
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
    print(
        "\x1b[35m[WEBHOOK] fetch status "
        f"job_id={job_id} kind={kind} url={source_url} site_id={site_id} pattern={pattern} "
        f"age_ms={age_ms} status_url={status_endpoint or 'n/a'} "
        f"data_items={data_items} metadata_keys={metadata_keys} mock_provider={use_mock_provider}\x1b[0m"
    )
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

    async def _record_scrape_error(error: str) -> None:
        from ...services.convex_client import convex_mutation

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
            cleaned_payload = clean_scrape_error_payload(error_payload)
            if _should_mock_convex_webhooks():
                logger.info(
                    "collect_firecrawl_job_result skip error log (mock convex) job_id=%s error=%s",
                    job_id,
                    error,
                )
                return
            await convex_mutation("router:insertScrapeError", cleaned_payload)
        except Exception:
            # Non-fatal best-effort logging; keep workflow progress
            pass

    if age_ms >= FIRECRAWL_STATUS_EXPIRATION_MS:
        msg = (
            "Firecrawl job expired (>24h); skipping status lookup "
            f"(job_id={job_id}, site_id={site_id}, age_ms={age_ms})"
        )
        logger.warning("collect_firecrawl_job_result expired job: %s", msg)
        await _record_scrape_error(msg)
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
        await _record_scrape_error(error_msg)
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
                    json_payload = json.loads(raw_text)
            except Exception:
                json_payload = None

        if raw_text is None and json_payload is not None:
            raw_text = json.dumps(json_payload, ensure_ascii=False)

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
        except Exception as exc:  # noqa: BLE001
            raise ApplicationError(f"Unable to parse Greenhouse board payload (webhook): {exc}", non_retryable=True) from exc

        logger.info(
            "collect_firecrawl_job_result greenhouse job_id=%s status=%s urls=%d status_url=%s",
            job_id,
            status_value,
            len(job_urls),
            status_link,
        )
        print(
            "\x1b[35m[WEBHOOK] status "
            f"job_id={job_id} kind={FirecrawlJobKind.GREENHOUSE_LISTING} status={status_value} "
            f"urls={len(job_urls)} http={http_status} status_url={status_link or 'n/a'}\x1b[0m"
        )
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
            "raw": raw_text,
        }

    raw_payload = (
        status.model_dump(mode="json", exclude_none=True)
        if hasattr(status, "model_dump")
        else status
    )
    normalized_items = normalize_firecrawl_items(raw_payload)
    print(
        "\x1b[35m[WEBHOOK] status "
        f"job_id={job_id} kind={kind} status={status_value} items={len(normalized_items)} "
        f"http={http_status} status_url={status_link or 'n/a'}\x1b[0m"
    )

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
async def store_scrape(scrape: Dict[str, Any]) -> str:
    from ...services.convex_client import convex_mutation

    # Keep the activity alive during longer Convex/ingestion calls.
    try:
        activity.heartbeat({"stage": "start"})
    except Exception:
        pass

    async def _log_scratchpad(event: str, message: str | None = None, data: Dict[str, Any] | None = None):
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
                "level": "info",
            }
        )
        payload["message"] = _build_scratchpad_message(payload)
        try:
            telemetry.emit_posthog_log(payload)
        except Exception:
            # best-effort; ignore logging errors
            pass

    async def _apply_job_detail_heuristics_to_jobs(jobs: List[Dict[str, Any]], heuristic_time_ms: int) -> List[Dict[str, Any]]:
        """Enrich job rows with heuristic parsing before ingestion."""

        try:
            from ...services.convex_client import convex_query
        except Exception:
            convex_query = None  # type: ignore[assignment]

        configs_cache: Dict[str, List[Dict[str, Any]]] = {}
        enriched: List[Dict[str, Any]] = []
        for job in jobs:
            domain = _domain_from_url(job.get("url") or "")
            configs = configs_cache.get(domain)
            if configs is None:
                try:
                    fetched = await convex_query("router:listJobDetailConfigs", {"domain": domain}) if convex_query else []
                    configs = fetched if isinstance(fetched, list) else []
                except Exception:
                    configs = []
                configs_cache[domain] = configs
            patch, records = _build_job_detail_heuristic_patch(job, configs or [], heuristic_time_ms)
            enriched.append({**job, **patch})
            for rec in records:
                try:
                    await convex_mutation("router:recordJobDetailHeuristic", rec)
                except Exception:
                    # best-effort; do not block ingestion
                    continue
        return enriched

    payload = trim_scrape_for_convex(scrape)
    now = int(time.time() * 1000)
    normalized_count = 0
    if isinstance(payload.get("items"), dict):
        normalized_items = payload["items"].get("normalized")
        if isinstance(normalized_items, list):
            normalized_count = len(normalized_items)
    ignored_count = 0
    if isinstance(payload.get("items"), dict):
        ignored_items = payload["items"].get("ignored")
        if isinstance(ignored_items, list):
            ignored_count = len(ignored_items)

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
    response_preview = _shrink_payload(payload.get("response"), 4000)
    async_response_preview = _shrink_payload(payload.get("asyncResponse"), 4000)
    items_provider = None
    if isinstance(payload.get("items"), dict):
        items_provider = payload["items"].get("provider") or payload["items"].get("crawlProvider")
    provider_for_log = scraped_with or payload.get("provider") or items_provider

    await _log_scratchpad(
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

    # Capture richer FetchFox payload details into scratchpad so we can debug provider responses.
    try:
        if provider_for_log and str(provider_for_log).startswith("fetchfox"):
            items_block = scrape.get("items") if isinstance(scrape.get("items"), dict) else {}
            raw_block = items_block.get("raw") if isinstance(items_block, dict) else None
            normalized = items_block.get("normalized") if isinstance(items_block, dict) else None

            raw_urls: List[str] = []
            if isinstance(raw_block, dict):
                urls_field = raw_block.get("urls")
                if isinstance(urls_field, list):
                    raw_urls = [u for u in urls_field if isinstance(u, str)]
                items_field = raw_block.get("items")
                if not raw_urls and isinstance(items_field, list):
                    for entry in items_field:
                        if isinstance(entry, dict):
                            url_val = entry.get("url") or entry.get("link")
                            if isinstance(url_val, str):
                                raw_urls.append(url_val)
                data_field = raw_block.get("data")
                if not raw_urls and isinstance(data_field, list):
                    for entry in data_field:
                        if isinstance(entry, dict):
                            url_val = entry.get("url") or entry.get("link")
                            if isinstance(url_val, str):
                                raw_urls.append(url_val)

            await _log_scratchpad(
                "scrape.fetchfox.raw",
                message="Captured FetchFox raw payload",
                data={
                    "pattern": payload.get("pattern"),
                    "rawUrlCount": len(raw_urls) if raw_urls else None,
                    "rawUrlSample": raw_urls[:20] if raw_urls else None,
                    "normalizedCount": len(normalized) if isinstance(normalized, list) else None,
                    "rawPreview": _shrink_payload(raw_block, 20000),
                },
            )
    except Exception:
        # Best-effort; do not block on debug logging
        pass

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

    scrape_id: str | None = None
    try:
        scrape_id = await convex_mutation(
            "router:insertScrapeRecord",
            _base_payload(payload),
        )
        await _log_scratchpad(
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
        try:
            activity.heartbeat({"stage": "persisted", "scrapeId": scrape_id})
        except Exception:
            pass
    except Exception as exc:
        logger.warning("insertScrapeRecord failed; retrying with trimmed payload: %s", exc, exc_info=exc)
        # Fallback: aggressively trim and retry once so we still record the run
        fallback = trim_scrape_for_convex(
            scrape,
            max_items=100,
            max_description=400,
            raw_preview_chars=0,
        )
        if isinstance(fallback.get("items"), dict):
            fallback["items"]["truncated"] = True
        try:
            scrape_id = await convex_mutation(
                "router:insertScrapeRecord",
                _base_payload(fallback),
            )
            await _log_scratchpad(
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
            try:
                activity.heartbeat({"stage": "persisted_fallback", "scrapeId": scrape_id})
            except Exception:
                pass
        except Exception as fallback_exc:
            logger.error(
                "Failed to persist scrape after fallback: %s",
                fallback_exc,
                exc_info=fallback_exc,
            )
            return f"store-error:{int(time.time() * 1000)}"

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
                jobs = await _apply_job_detail_heuristics_to_jobs(jobs, now)
            except Exception:
                # Heuristics are best-effort; continue with raw jobs if parsing fails.
                pass
            ingest_payload: Dict[str, Any] = {"jobs": jobs}
            if payload.get("siteId") is not None:
                ingest_payload["siteId"] = payload.get("siteId")
            await convex_mutation("router:ingestJobsFromScrape", ingest_payload)
            await _log_scratchpad(
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
                activity.heartbeat({"stage": "ingested_jobs", "count": len(jobs)})
            except Exception:
                pass
    except Exception:
        # Non-fatal: ingestion failures shouldn't block scrape recording
        pass

    # Record ignored entries (e.g., filtered by keyword) so future crawls can skip quickly.
    try:
        ignored_entries = []
        ignored_recorded = 0
        if isinstance(payload.get("items"), dict):
            ignored_entries = payload["items"].get("ignored") or []
        if isinstance(ignored_entries, list):
            for entry in ignored_entries:
                if not isinstance(entry, dict):
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
                await convex_mutation(
                    "router:insertIgnoredJob",
                    {
                        "url": url_val.strip(),
                        "sourceUrl": payload.get("sourceUrl") or payload.get("pattern"),
                        "reason": entry.get("reason") or "filtered",
                        "provider": scraped_with or payload.get("provider"),
                        "workflowName": payload.get("workflowName"),
                        "details": _shrink_payload(entry, 4000),
                    "title": title_val,
                    "description": desc_val,
                },
            )
                ignored_recorded += 1
        if ignored_recorded:
            await _log_scratchpad(
                "scrape.ignored_jobs",
                message=f"Recorded {ignored_recorded} ignored jobs for {payload.get('sourceUrl') or 'unknown'}",
                data={
                    "count": ignored_recorded,
                    "workflowId": payload.get("workflowId"),
                    "siteId": payload.get("siteId"),
                    "provider": provider_for_log,
                },
            )
    except Exception:
        # Best-effort; ignore failures
        pass

    # Best-effort enqueue of job URLs discovered in scrape payloads (e.g., Greenhouse listings).
    try:
        urls_from_raw = _extract_job_urls_from_scrape(scrape)
        await _log_scratchpad(
            "scrape.url_extraction.raw",
            message="Attempted URL extraction from raw scrape payload",
            data={"urls": len(urls_from_raw or []), "sourceUrl": payload.get("sourceUrl")},
        )

        urls_from_trimmed = _extract_job_urls_from_scrape(payload) if not urls_from_raw else []
        if not urls_from_raw:
            await _log_scratchpad(
                "scrape.url_extraction.trimmed",
                message="Attempted URL extraction from trimmed payload",
                data={"urls": len(urls_from_trimmed or []), "sourceUrl": payload.get("sourceUrl")},
            )

        urls = urls_from_raw or urls_from_trimmed or []
        source_url = payload.get("sourceUrl")
        if urls:
            logger.info(
                "Scrape URL extraction source=%s count=%s",
                source_url,
                len(urls),
            )
            for idx, url in enumerate(urls, start=1):
                logger.info(
                    "Scrape URL %s/%s source=%s url=%s",
                    idx,
                    len(urls),
                    source_url,
                    url,
                )
        else:
            logger.info("Scrape URL extraction source=%s count=0", source_url)
        if urls:
            site_id = _convex_site_id(payload.get("siteId"))
            await convex_mutation(
                "router:enqueueScrapeUrls",
                {
                    "urls": urls,
                    "sourceUrl": payload.get("sourceUrl") or "",
                    "provider": scraped_with or payload.get("provider") or "",
                    "siteId": site_id,
                    "pattern": payload.get("pattern"),
                },
            )
            await _log_scratchpad(
                "scrape.url_enqueue",
                message="Enqueued URLs from scrape payload",
                data={"urls": len(urls), "sourceUrl": payload.get("sourceUrl")},
            )
        else:
            await _log_scratchpad(
                "scrape.url_extraction.none",
                message="No URLs extracted from scrape payload",
                data={"sourceUrl": payload.get("sourceUrl")},
            )
        try:
            activity.heartbeat({"stage": "urls_processed", "urls": len(urls or [])})
        except Exception:
            pass
    except Exception as exc:
        await _log_scratchpad(
            "scrape.url_extraction.error",
            message="Failed to enqueue URLs from scrape payload",
            data={"error": str(exc), "sourceUrl": payload.get("sourceUrl")},
        )
        # Non-fatal

    return str(scrape_id)


@activity.defn
async def complete_site(site_id: str) -> None:
    from ...services.convex_client import convex_mutation

    if not _looks_like_convex_id(site_id):
        # Skip best-effort if id is not a Convex document id
        return

    try:
        await convex_mutation("router:completeSite", {"id": site_id})
    except Exception as exc:  # noqa: BLE001
        # Swallow validator errors so workflows continue
        if "ArgumentValidationError" in str(exc) and ".id" in str(exc):
            return
        raise


@activity.defn
async def fail_site(payload: Dict[str, Any]) -> None:
    from ...services.convex_client import convex_mutation

    site_id = payload.get("id")
    if not isinstance(site_id, str) or not _looks_like_convex_id(site_id):
        return

    try:
        await convex_mutation("router:failSite", {"id": site_id, "error": payload.get("error")})
    except Exception as exc:  # noqa: BLE001
        if "ArgumentValidationError" in str(exc) and ".id" in str(exc):
            return
        raise


def _looks_like_convex_id(value: str) -> bool:
    return isinstance(value, str) and len(value) >= 26 and value.isalnum()


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


def _extract_job_urls_from_scrape(scrape: Dict[str, Any]) -> list[str]:
    """Heuristic extraction of job URLs (Greenhouse or plain HTML) from a scrape payload."""

    md_link_re = re.compile(MARKDOWN_LINK_PATTERN)
    greenhouse_re = re.compile(GREENHOUSE_URL_PATTERN, re.IGNORECASE)
    location_line_re = re.compile(r"^\s*location\b\s*[:\-–]?\s*(?P<location>.+)$", re.IGNORECASE)
    apply_text_re = re.compile(r"\bapply\b", re.IGNORECASE)
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
            if href and href.startswith("http"):
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

    def _gather_strings(value: Any) -> list[str]:
        results: list[str] = []
        if isinstance(value, str):
            results.append(value)
            return results
        if isinstance(value, dict):
            for v in value.values():
                results.extend(_gather_strings(v))
        elif isinstance(value, list):
            for item in value:
                results.extend(_gather_strings(item))
        return results

    def _split_title_and_location(text: str) -> tuple[Optional[str], Optional[str]]:
        if not text:
            return None, None
        val = text.strip()
        paren_match = re.match(TITLE_LOCATION_PAREN_PATTERN, val)
        if paren_match:
            return paren_match.group(1).strip() or None, paren_match.group(2).strip() or None
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

    def _looks_like_job_detail_url(url: str) -> bool:
        try:
            path = urlparse(url).path
        except Exception:
            return False
        lower = (path or "").lower()
        if not any(token in lower for token in ("/job", "/jobs", "/career", "/careers", "/position", "/positions")):
            return False
        segments = [seg for seg in lower.split("/") if seg]
        for idx, seg in enumerate(segments):
            if seg in {"job", "jobs", "career", "careers", "position", "positions"}:
                return idx + 1 < len(segments)
        return False

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
            if slug.startswith(("united_states", "united-states")) and not re.search(r"\d", slug):
                return True
        return False

    def _should_ignore_url(url: str) -> bool:
        return _looks_like_location_filter_url(url)

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

    def _extract_from_text(text: str) -> list[tuple[str, Optional[str], Optional[str]]]:
        links: list[tuple[str, Optional[str], Optional[str]]] = []

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

        return links

    candidates: list[str] = []
    items = scrape.get("items") if isinstance(scrape, dict) else {}
    if isinstance(items, dict):
        raw_val = items.get("raw")
        candidates.extend(_gather_strings(raw_val))
        if "raw" in items and not raw_val and isinstance(items.get("normalized"), list):
            for job in items["normalized"]:
                candidates.extend(_gather_strings(job))
    candidates.extend(_gather_strings(scrape.get("response")))

    urls: list[str] = []
    seen: set[str] = set()
    # Direct URL arrays from crawl payloads (e.g., job_urls/rawUrls) should be enqueued even if we haven't parsed titles yet.
    if isinstance(items, dict):
        for key in ("job_urls", "rawUrls", "urls"):
            url_list = items.get(key)
            if isinstance(url_list, list):
                for url_val in url_list:
                    if isinstance(url_val, str) and url_val.strip():
                        url = url_val.strip()
                        if _should_ignore_url(url):
                            continue
                        if url not in seen:
                            seen.add(url)
                            urls.append(url)

    for text in list(candidates):
        if isinstance(text, str):
            try:
                parsed = json.loads(text)
                candidates.extend(_gather_strings(parsed))
            except Exception:
                pass
        if not isinstance(text, str):
            continue
        for url, title, location, context_text, context_location in _extract_markdown_links_with_context(text):
            if not url or not url.startswith("http"):
                continue
            if _should_ignore_url(url):
                continue
            title_match = title_matches_required_keywords(title)
            context_match = False
            if not title_match and context_text:
                context_match = title_matches_required_keywords(context_text)
            if not title_match and not context_match:
                continue
            location_value = location or context_location
            if location_value and not location_matches_usa(location_value):
                continue
            if not title_match:
                if _looks_like_apply_link(title, url):
                    continue
                if not _looks_like_job_detail_url(url):
                    continue
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)

        for url, title, location in _extract_from_text(text):
            if not url or not url.startswith("http"):
                continue
            if _should_ignore_url(url):
                continue
            if not title_matches_required_keywords(title):
                continue
            if location and not location_matches_usa(location):
                continue
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)

    return urls


def _domain_from_url(url: str) -> str:
    try:
        return str(url or "").split("://", 1)[-1].split("/", 1)[0].lower()
    except Exception:
        return ""


def _build_ordered_regexes(configs: List[Dict[str, Any]], field: str, defaults: List[str]) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for cfg in configs:
        if (cfg.get("field") or "").lower() != field.lower():
            continue
        regex = cfg.get("regex")
        if not isinstance(regex, str) or not regex.strip():
            continue
        if regex in seen:
            continue
        seen.add(regex)
        ordered.append(regex)
    for regex in defaults:
        if regex not in seen:
            seen.add(regex)
            ordered.append(regex)
    return ordered


def _detect_currency_code(text: str) -> Optional[str]:
    """Lightweight currency detector to prioritize non-USD listings (e.g., INR, EUR, GBP)."""

    lowered = text.lower()
    currency_hints = [
        ("INR", INR_CURRENCY_PATTERNS),
        ("GBP", GBP_CURRENCY_PATTERNS),
        ("EUR", EUR_CURRENCY_PATTERNS),
        ("AUD", AUD_CURRENCY_PATTERNS),
        ("CAD", CAD_CURRENCY_PATTERNS),
    ]
    for code, patterns in currency_hints:
        for pat in patterns:
            try:
                if re.search(pat, text, flags=re.IGNORECASE):
                    return code
            except re.error:
                continue
    if "$" in text and "aud" not in lowered and "cad" not in lowered:
        return "USD"
    return None


def _looks_like_location_anywhere(value: Optional[str]) -> bool:
    """Allow non-US locations such as 'Bangalore, India' to pass heuristic parsing."""
    if not value:
        return False
    text = value.strip()
    if len(text) < 3 or len(text) > 80:
        return False
    return bool(re.search(LOCATION_ANYWHERE_PATTERN, text))


_CANADIAN_PROVINCE_CODES = {
    "AB",
    "BC",
    "MB",
    "NB",
    "NL",
    "NS",
    "NT",
    "NU",
    "ON",
    "PE",
    "QC",
    "SK",
    "YT",
}
_CANADIAN_PROVINCE_NAMES = {
    "alberta",
    "british columbia",
    "manitoba",
    "new brunswick",
    "newfoundland and labrador",
    "nova scotia",
    "northwest territories",
    "nunavut",
    "ontario",
    "prince edward island",
    "quebec",
    "saskatchewan",
    "yukon",
}
_UNKNOWN_LOCATION_TOKENS = {"unknown", "n/a", "na", "unspecified", "not available"}
_US_STATE_NAMES = {name.lower() for name in DEFAULT_US_STATE_NAMES}
_US_STATE_CODES = {code.upper() for code in DEFAULT_US_STATE_CODES}


def _normalize_locations(raw_locations: Iterable[str]) -> List[str]:
    """Split and dedupe multiple location hints (e.g., 'Madrid, Spain; Paris, France')."""

    seen: set[str] = set()
    cleaned: List[str] = []
    for raw in raw_locations:
        if not raw:
            continue
        for part in re.split(LOCATION_SPLIT_PATTERN, str(raw)):
            candidate = (part or "").strip(" ;|/\t")
            if not candidate:
                continue
            candidate = re.sub(MULTI_SPACE_PATTERN, " ", candidate)
            lowered = candidate.lower()
            if lowered in ("unknown", "n/a", "na"):
                continue
            if len(candidate) < 3 or len(candidate) > 100:
                continue
            if not _is_plausible_location(candidate):
                continue
            if candidate not in seen:
                seen.add(candidate)
                cleaned.append(candidate)

    return cleaned[:5]


def _is_plausible_location(value: str) -> bool:
    lowered = value.lower()
    if any(token in lowered for token in ("diversity", "equity", "inclusion", "benefits", "culture", "salary", "compensation", "pay", "package", "bonus", "range")):
        return False
    if "$" in value or "401k" in lowered or "401(k" in lowered:
        return False
    if "," in value:
        segments = [p.strip() for p in value.split(",") if p.strip()]
        if len(segments) > 3:
            return False
        if any(len(seg.split()) > 3 for seg in segments):
            return False
        if any("remote" in seg.lower() for seg in segments[1:]):
            return True
        return True
    if "remote" in lowered:
        return True
    return len(value.split()) <= 4


def _derive_location_states(locations: List[str]) -> List[str]:
    states: List[str] = []
    for loc in locations:
        parts = [p.strip() for p in str(loc).split(",") if p.strip()]
        if len(parts) >= 2:
            state_val = parts[-2] if len(parts) >= 3 else parts[-1]
            if state_val and state_val not in states:
                states.append(state_val)
    return states


def _derive_countries(locations: List[str]) -> List[str]:
    countries: List[str] = []
    for loc in locations:
        parts = [p.strip() for p in str(loc).split(",") if p.strip()]
        if not parts:
            continue
        country = parts[-1]
        lowered = country.lower()
        country_upper = country.upper()
        mapped: Optional[str] = None
        if "remote" in lowered:
            mapped = "United States"
        elif lowered in {"locations"}:
            continue
        elif lowered in _UNKNOWN_LOCATION_TOKENS:
            mapped = "United States"
        elif country_upper in _US_STATE_CODES:
            mapped = "United States"
        elif re.match(COUNTRY_CODE_PATTERN, country):
            if country_upper in _CANADIAN_PROVINCE_CODES:
                mapped = "Canada"
            else:
                continue
        elif lowered in _CANADIAN_PROVINCE_NAMES:
            mapped = "Canada"
        elif lowered in _US_STATE_NAMES:
            mapped = "United States"
        else:
            mapped = country
        if mapped and mapped not in countries:
            countries.append(mapped)
    return countries


def _build_location_search(locations: List[str]) -> str:
    tokens: set[str] = set()
    for loc in locations:
        for token in re.split(LOCATION_TOKEN_SPLIT_PATTERN, loc):
            cleaned = token.strip()
            if cleaned:
                tokens.add(cleaned)
    return " ".join(tokens)


HEURISTIC_VERSION = 4


def _describe_exception(exc: Exception) -> str:
    """Provide a compact string for unexpected errors."""

    parts: list[str] = [f"{type(exc).__name__}: {exc}"]
    resp = getattr(exc, "response", None)
    if resp is not None:
        status = getattr(resp, "status_code", None)
        request_id = None
        try:
            headers = getattr(resp, "headers", {}) or {}
            request_id = headers.get("x-request-id") or headers.get("request-id")
        except Exception:
            request_id = None
        parts.append(f"status={status}")
        if request_id:
            parts.append(f"request_id={request_id}")
    data = getattr(exc, "data", None)
    if data:
        parts.append(f"data={data}")
    return " ".join(str(p) for p in parts if p)


def _extract_request_id(exc: Exception) -> Optional[str]:
    """Best-effort extraction of Convex request id from exception or message."""

    msg = ""
    try:
        msg = str(exc)
    except Exception:
        msg = ""

    # Look for "[Request ID: xyz]" pattern commonly used by Convex errors.
    match = re.search(REQUEST_ID_PATTERN, msg)
    if match:
        return match.group(1).strip()

    # Some clients may attach response headers.
    resp = getattr(exc, "response", None)
    if resp is not None:
        try:
            headers = getattr(resp, "headers", {}) or {}
            candidate = headers.get("x-request-id") or headers.get("request-id")
            if candidate:
                return str(candidate)
        except Exception:
            return None

    return None


def _extract_pending_count(value: Any) -> Optional[int]:
    """Pull a numeric pending count from a Convex response or bare number."""

    if isinstance(value, dict):
        for key in ("pending", "remaining", "count", "total"):
            candidate = value.get(key)
            if isinstance(candidate, (int, float)):
                return int(candidate)
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _first_match(text: str, regexes: List[str]) -> tuple[Optional[str], Optional[str]]:
    for pattern in regexes:
        try:
            match = re.search(pattern, text, flags=re.MULTILINE | re.IGNORECASE)
        except re.error:
            continue
        if match:
            group_dict = match.groupdict() if match.groupdict() else {}
            # Prefer named groups if present.
            if "location" in group_dict:
                return pattern, group_dict.get("location")
            if "value" in group_dict:
                return pattern, group_dict.get("value")
            return pattern, match.group(0)
    return None, None


def _build_job_detail_heuristic_patch(
    row: Dict[str, Any],
    configs: List[Dict[str, Any]],
    now_ms: int,
) -> tuple[Dict[str, Any], List[Dict[str, str]]]:
    """Return heuristic patch + records for a job row without mutating Convex."""

    raw_description = row.get("description") or ""
    description = strip_known_nav_blocks(raw_description)
    url = row.get("url") or ""
    domain = _domain_from_url(url)
    attempts = int(row.get("heuristicAttempts") or 0)
    recorded_location = False
    recorded_comp = False
    records: List[Dict[str, str]] = []

    location_defaults = [
        LOCATION_FULL_PATTERN,
        LOCATION_LABEL_PATTERN,
        LOCATION_CITY_STATE_PATTERN,
        LOCATION_PAREN_PATTERN,
    ]
    comp_defaults = [
        COMP_USD_RANGE_PATTERN,
        COMP_INR_RANGE_PATTERN,
        COMP_K_PATTERN,
        COMP_LPA_PATTERN,
    ]

    location_regexes = _build_ordered_regexes(configs, "location", location_defaults)
    comp_regexes = _build_ordered_regexes(configs, "compensation", comp_defaults)

    hints = parse_markdown_hints(description)
    hinted_comp = hints.get("compensation")
    comp_range_hint = hints.get("compensation_range") or {}
    locations_hint = hints.get("locations") or []
    raw_company = row.get("company")
    company_name = raw_company if isinstance(raw_company, str) else str(raw_company or "")
    company_remote = is_remote_company(company_name)
    raw_location_value = (row.get("location") or "").strip()
    raw_location_lower = raw_location_value.lower()
    location_fallback = (
        hints.get("location")
        if (not raw_location_value or raw_location_lower in _UNKNOWN_LOCATION_TOKENS)
        else raw_location_value or hints.get("location")
    )
    is_remote = company_remote or hints.get("remote") is True or bool(row.get("remote"))
    if hints.get("remote") is False and not company_remote:
        is_remote = False
    if "remote" in raw_location_lower:
        is_remote = True
    location_unknown = raw_location_lower in _UNKNOWN_LOCATION_TOKENS or not raw_location_value
    locations = _normalize_locations(locations_hint or ([location_fallback] if location_fallback else []))
    comp_reason = row.get("compensationReason")
    total_comp = row.get("totalCompensation") or 0
    raw_comp_unknown = row.get("compensationUnknown")
    compensation_unknown = bool(raw_comp_unknown) if raw_comp_unknown is not None else None
    currency_code = row.get("currencyCode")
    currency_hint = _detect_currency_code(description)
    if currency_hint and currency_hint != currency_code:
        currency_code = currency_hint
    if (not total_comp or total_comp <= 0) and isinstance(hinted_comp, (int, float)):
        total_comp = int(hinted_comp)
        compensation_unknown = False
        comp_reason = "parsed from description"
    elif (not total_comp or total_comp <= 0) and isinstance(comp_range_hint, dict):
        low_hint = comp_range_hint.get("low")
        high_hint = comp_range_hint.get("high")
        range_values = [v for v in (low_hint, high_hint) if isinstance(v, (int, float)) and v >= 1000]
        if range_values:
            total_comp = int(sum(range_values) / len(range_values))
            compensation_unknown = False
            comp_reason = "parsed from description"
    elif total_comp and total_comp > 0 and compensation_unknown is None:
        compensation_unknown = False

    matched_locations: List[str] = []
    if description:
        used_pattern, found = _first_match(description, location_regexes)
        if found and (location_matches_usa(found) or _looks_like_location_anywhere(found)):
            found_locations = _normalize_locations([found])
            if found_locations:
                matched_locations = found_locations
                if used_pattern:
                    records.append(
                        {"domain": domain or "default", "field": "location", "regex": used_pattern}
                    )
                    recorded_location = True
    if matched_locations and not locations:
        locations = matched_locations
    if (not locations) and currency_hint and currency_hint != "USD":
        if currency_hint == "INR":
            locations = ["India"]
        elif currency_hint == "GBP":
            locations = ["United Kingdom"]
        elif currency_hint == "EUR":
            locations = ["Europe"]
    if not locations and is_remote:
        locations = ["Remote"]
    if locations:
        seen_cities: set[str] = set()
        deduped_locations: List[str] = []
        for loc in locations:
            city_part = loc.split(",")[0].strip().lower()
            if city_part in seen_cities:
                continue
            seen_cities.add(city_part)
            deduped_locations.append(loc)
        locations = deduped_locations

    countries = _derive_countries(locations)
    if not countries and (is_remote or location_unknown):
        countries = ["United States"]

    if (not total_comp or total_comp <= 0) and description:
        used_pattern, found_val = _first_match(description, comp_regexes)
        if found_val:
            cleaned = found_val.replace(",", "").lower()
            try:
                if "lpa" in cleaned or "lakh" in cleaned:
                    base_val = re.sub(NON_NUMERIC_DOT_PATTERN, "", cleaned)
                    comp_val = int(float(base_val) * 100_000) if base_val else None
                elif cleaned.endswith("k"):
                    comp_val = int(float(cleaned[:-1]) * 1000)
                else:
                    comp_val = int(float(re.sub(NON_NUMERIC_PATTERN, "", cleaned)))
            except Exception:
                comp_val = None
            if comp_val and comp_val > 0:
                total_comp = comp_val
                compensation_unknown = False
                comp_reason = "parsed with heuristic"
                if currency_hint and currency_hint != "USD":
                    currency_code = currency_hint
            if used_pattern:
                records.append(
                    {"domain": domain or "default", "field": "compensation", "regex": used_pattern}
                )
                recorded_comp = True

    if locations and not recorded_location:
        records.append({"domain": domain or "default", "field": "location", "regex": "hint:location"})
        recorded_location = True
    if total_comp and total_comp > 0 and not recorded_comp:
        records.append(
            {"domain": domain or "default", "field": "compensation", "regex": "hint:compensation"}
        )
        recorded_comp = True

    patch: Dict[str, Any] = {
        "heuristicAttempts": attempts + 1,
        "heuristicLastTried": now_ms,
        "heuristicVersion": HEURISTIC_VERSION,
    }
    if locations:
        patch["locations"] = locations
        patch["location"] = locations[0]
        patch["locationStates"] = _derive_location_states(locations)
        patch["locationSearch"] = _build_location_search(locations)
    if countries:
        patch["countries"] = countries
        patch["country"] = countries[0]
    if total_comp and total_comp > 0:
        patch["totalCompensation"] = int(total_comp)
    if comp_reason:
        patch["compensationReason"] = comp_reason
    if compensation_unknown is not None:
        patch["compensationUnknown"] = compensation_unknown
    if currency_code:
        patch["currencyCode"] = currency_code
    remote_hint = hints.get("remote")
    if company_remote:
        remote_hint = True
    if remote_hint is True and row.get("remote") is not True:
        patch["remote"] = True
    elif remote_hint is False and row.get("remote") is not False:
        patch["remote"] = False
    if description and description != raw_description:
        patch["description"] = description

    return patch, records


@activity.defn
async def process_pending_job_details_batch(limit: int = 25) -> Dict[str, Any]:
    """Parse pending job descriptions with heuristics and persist learned regex configs."""

    from ...services.convex_client import convex_mutation, convex_query

    pending = await convex_query("router:listPendingJobDetails", {"limit": limit}) or []
    processed = 0
    updated: List[str] = []
    errors: List[Dict[str, Any]] = []
    total = len(pending)
    logger.info("heuristic.batch start fetched=%s limit=%s", total, limit)

    async def _attempt_mutation(op_name: str, payload: Dict[str, Any], row_id: Any) -> bool:
        """Run a mutation and capture errors without aborting the batch."""

        try:
            await convex_mutation(op_name, payload)
            return True
        except asyncio.CancelledError:
            raise
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
            # Heartbeat regularly so the activity isn't cancelled while processing a large batch.
            _safe_activity_heartbeat({"processed": processed, "index": idx, "total": total})
            job_id = row.get("jobId") or row.get("_id")
            title = (str(row.get("title") or row.get("jobTitle") or "")).strip() or "<untitled>"
            logger.info("heuristic.view job id=%s title=%s", job_id or "<missing>", title)
            url = row.get("url") or ""
            domain = _domain_from_url(url)

            current_op = "router:listJobDetailConfigs"
            configs = await convex_query("router:listJobDetailConfigs", {"domain": domain}) or []
            now_ms = int(time.time() * 1000)
            patch, records = _build_job_detail_heuristic_patch(row, configs, now_ms)

            for rec in records:
                await _attempt_mutation("router:recordJobDetailHeuristic", rec, job_id)

            if not job_id:
                continue

            if patch:
                current_op = "router:updateJobWithHeuristic"
                did_update = await _attempt_mutation("router:updateJobWithHeuristic", {"id": job_id, **patch}, job_id)
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
        remaining_resp = await convex_query(op, {})
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


@activity.defn
async def record_workflow_run(run: Dict[str, Any]) -> None:
    from ...services.convex_client import convex_mutation

    payload = {k: v for k, v in run.items() if v is not None}
    try:
        await convex_mutation("temporal:recordWorkflowRun", payload)
    except asyncio.CancelledError:
        # Shutdown/interrupt paths shouldn't surface as activity failures
        return None
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to record workflow run: {e}") from e


def _coerce_workflow_id(entry: Dict[str, Any]) -> str:
    """Best-effort extraction of a workflow id for logging/filtering."""

    candidates = [
        entry.get("workflowId"),
        entry.get("workflow_id"),
        (entry.get("data") or {}).get("workflowId") if isinstance(entry.get("data"), dict) else None,
        (entry.get("data") or {}).get("workflow_id") if isinstance(entry.get("data"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return "unknown"


def _short_preview(value: Any) -> str:
    """Return a concise preview for message strings."""

    if value is None:
        return "none"
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, str):
        return value[:120]
    if isinstance(value, list):
        return f"len={len(value)}"
    if isinstance(value, dict):
        return ", ".join(
            f"{k}={_short_preview(v)}"
            for k, v in list(value.items())[:4]
            if v is not None
        )
    return str(value)[:120]


def _build_scratchpad_message(payload: Dict[str, Any]) -> str:
    """Compose a descriptive message that always includes workflow id."""

    event = payload.get("event")
    site_url = payload.get("siteUrl") or payload.get("sourceUrl")
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    workflow_id = _coerce_workflow_id(payload)

    base = payload.get("message")
    parts: list[str] = []
    if base:
        parts.append(str(base))
    elif event:
        parts.append(event.replace("_", " "))

    if site_url:
        parts.append(f"site={site_url}")

    interesting_keys = (
        "jobId",
        "jobsScraped",
        "jobUrls",
        "itemsCount",
        "normalizedCount",
        "urls",
        "count",
        "sitesProcessed",
        "stored",
        "failed",
        "remaining",
        "toScrape",
        "status",
        "provider",
        "pattern",
    )
    details: list[str] = []
    for key in interesting_keys:
        if key in data and data[key] is not None:
            details.append(f"{key}={_short_preview(data[key])}")

    if data.get("sample"):
        sample_title = None
        if isinstance(data["sample"], list):
            for entry in data["sample"]:
                if isinstance(entry, dict):
                    sample_title = entry.get("title") or entry.get("job_title")
                    if sample_title:
                        break
        if sample_title:
            details.append(f"sample_title={_short_preview(sample_title)}")

    if details:
        parts.append(", ".join(details))

    if workflow_id:
        parts.append(f"workflow_id={workflow_id}")

    if not parts:
        return f"{event or 'scratchpad'} | workflow_id={workflow_id}"

    return " | ".join(parts)


def _shrink_for_scratchpad(data: Any, max_len: int = 900) -> Any:
    """Keep scratchpad payloads small so log events stay lightweight."""

    if data is None:
        return None

    try:
        serialized = json.dumps(data, ensure_ascii=False)
    except Exception:
        serialized = str(data)

    if len(serialized) <= max_len:
        return data

    return f"{serialized[:max_len]}... (+{len(serialized) - max_len} chars)"


@activity.defn
async def record_scratchpad(entry: Dict[str, Any]) -> None:
    """Emit a lightweight scratchpad entry to OTLP/PostHog."""

    payload = _with_firecrawl_suffix({k: v for k, v in entry.items() if v is not None})
    if payload.get("siteUrl") is None:
        payload["siteUrl"] = ""
    if "data" in payload:
        payload["data"] = _shrink_for_scratchpad(payload.get("data"))
    workflow_id = _coerce_workflow_id(payload)
    payload["workflowId"] = workflow_id
    payload["message"] = _build_scratchpad_message(payload)

    try:
        telemetry.emit_posthog_log(payload)
    except asyncio.CancelledError:
        return None
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to record scratchpad entry: {e}") from e
