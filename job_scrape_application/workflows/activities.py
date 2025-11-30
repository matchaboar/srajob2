from __future__ import annotations

import asyncio
import json
import re
import time
import logging
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TypedDict, NotRequired, cast
from urllib.parse import urlparse

from firecrawl import Firecrawl
from firecrawl.v2.types import PaginationConfig, ScrapeOptions
from firecrawl.v2.utils.error_handler import PaymentRequiredError, RequestTimeoutError
from fetchfox_sdk import FetchFox
from pydantic import BaseModel, ConfigDict, Field
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .exceptions import (
    NonRetryableWorkflowError,
    PaymentRequiredWorkflowError,
    RateLimitWorkflowError,
    TimeoutWorkflowError,
)

from ..config import settings
from ..constants import title_matches_required_keywords
from ..components.models import (
    FetchFoxPriority,
    FetchFoxScrapeRequest,
    GreenhouseBoardResponse,
    MAX_FETCHFOX_VISITS,
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)


logger = logging.getLogger("temporal.worker.activities")

DEFAULT_TOTAL_COMPENSATION = 151000
MAX_DESCRIPTION_CHARS = 1200  # keep payloads small enough for Convex document limits
MAX_FIRECRAWL_VISITS = MAX_FETCHFOX_VISITS
FIRECRAWL_CACHE_MAX_AGE_MS = 600_000
FIRECRAWL_STATUS_EXPIRATION_MS = 24 * 60 * 60 * 1000
FIRECRAWL_STATUS_WARN_MS = 23 * 60 * 60 * 1000
HTTP_RETRY_BASE_SECONDS = 30
CONVEX_MUTATION_TIMEOUT_SECONDS = 3
JOB_TEMPLATE: Dict[str, str] = {
    # Keep simple strings so FetchFox LLM extractor can infer fields from detail pages
    "job_title": "str | None",
    "company": "str | None",
    "description": "str | None",
    "url": "str | None",
    "location": "str | None",
    "remote": "True | False | None",
    "level": (
        "junior | mid | senior | staff | lead | principal | director | manager | vp | cxo | intern | None"
    ),
    "salary": "str | number | None",
    "total_compensation": "number | None",
    "posted_at": "datetime | date | str | None",
}


def _convex_http_base() -> str:
    """Return Convex HTTP base with .convex.site domain for webhooks."""

    if settings.convex_http_url:
        base = settings.convex_http_url.rstrip("/")
    elif settings.convex_url:
        base = settings.convex_url.rstrip("/").replace(".convex.cloud", ".convex.site")
    else:
        raise ApplicationError(
            "CONVEX_HTTP_URL or CONVEX_URL env var is required for Convex HTTP routes",
            non_retryable=True,
        )

    if ".convex.site" not in base and ".convex.cloud" in base:
        base = base.replace(".convex.cloud", ".convex.site")

    return base


def _log_provider_dispatch(provider: str, url: str, **context: Any) -> None:
    """Emit a colored stdout log when dispatching a scrape request to a provider."""

    context_parts = [f"{k}={v}" for k, v in context.items() if v is not None]
    context_str = " ".join(context_parts)
    msg = f"[SCRAPE DISPATCH] provider={provider} url={url}"
    if context_str:
        msg = f"{msg} {context_str}"
    # Cyan text for visibility in the worker console.
    print(f"\x1b[36m{msg}\x1b[0m")
    logger.info(msg)


def _build_provider_status_url(
    provider: str, job_id: str | None, *, status_url: str | None = None, kind: str | None = None
) -> str | None:
    """Return a human-friendly status link for a provider if we can infer one."""

    def _parse_http_url(url: str) -> Optional[Any]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None
        return parsed

    status_url_str = status_url.strip() if isinstance(status_url, str) else None
    job_id_str = str(job_id) if job_id not in (None, "") else None

    if provider == "firecrawl":
        firecrawl_status = None
        if job_id_str:
            firecrawl_status = f"https://api.firecrawl.dev/v2/batch/scrape/{job_id_str}"

        if status_url_str:
            parsed = _parse_http_url(status_url_str)
            host = parsed.netloc.lower() if parsed else ""
            if parsed and "firecrawl" in host and (job_id_str is None or job_id_str in parsed.path):
                return status_url_str

        return firecrawl_status

    if status_url_str:
        return status_url_str
    return None


def _log_sync_response(
    provider: str,
    *,
    action: str,
    url: str | None = None,
    job_id: str | None = None,
    status_url: str | None = None,
    kind: str | None = None,
    summary: str | None = None,
    metadata: Dict[str, Any] | None = None,
) -> None:
    """Print a synchronous provider response to stdout for quick debugging."""

    link = _build_provider_status_url(provider, job_id, status_url=status_url, kind=kind)

    def _fmt(val: Any) -> str:
        return str(val)

    parts = [f"provider={provider}", f"action={action}"]
    if url:
        parts.append(f"url={url}")
    if kind:
        parts.append(f"kind={kind}")
    if job_id:
        parts.append(f"job_id={job_id}")
    if link:
        parts.append(f"status_url={link}")
    if summary:
        parts.append(summary)
    if metadata:
        meta_bits = [f"{k}={_fmt(v)}" for k, v in metadata.items() if v is not None]
        if meta_bits:
            parts.append(" ".join(meta_bits))

    msg = f"[SCRAPE RESPONSE] {' '.join(parts)}"
    print(f"\x1b[33m{msg}\x1b[0m")
    logger.info(msg)


def _mask_secret(secret: str | None) -> str | None:
    """Return a lightly redacted version of a secret for audit purposes."""

    if secret is None:
        return None
    secret_str = str(secret)
    if not secret_str:
        return None
    if len(secret_str) <= 4:
        return "*" * len(secret_str)
    return f"{secret_str[:4]}...{secret_str[-2:]}"


def _sanitize_headers(headers: Dict[str, Any] | None) -> Dict[str, Any] | None:
    """Mask sensitive header values while keeping the shape visible."""

    if not isinstance(headers, dict):
        return None

    sanitized: Dict[str, Any] = {}
    for key, value in headers.items():
        if value is None:
            continue
        if isinstance(value, str):
            masked = _mask_secret(value)
            sanitized[key] = masked if masked is not None else value
        else:
            sanitized[key] = value

    return sanitized if sanitized else None


def _build_request_snapshot(
    body: Any,
    *,
    provider: str | None = None,
    url: str | None = None,
    method: str | None = None,
    headers: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Construct a serializable view of the outbound provider request."""

    header_block: Dict[str, Any] = dict(headers or {})
    if provider == "firecrawl" and settings.firecrawl_api_key:
        header_block.setdefault("authorization", f"Bearer {_mask_secret(settings.firecrawl_api_key)}")
    if provider == "fetchfox" and settings.fetchfox_api_key:
        header_block.setdefault("x-api-key", _mask_secret(settings.fetchfox_api_key))

    sanitized_headers = _sanitize_headers(header_block) if header_block else None
    snapshot: Dict[str, Any] = {}

    if method:
        snapshot["method"] = method
    if url:
        snapshot["url"] = url
    if body is not None:
        snapshot["body"] = body
    if sanitized_headers:
        snapshot["headers"] = sanitized_headers

    return snapshot


class ScrapeErrorInputRequired(TypedDict):
    error: str


class ScrapeErrorInputOptional(TypedDict, total=False):
    jobId: str
    sourceUrl: str
    siteId: str
    event: str
    status: str
    metadata: Any
    payload: Any
    createdAt: int


class ScrapeErrorInput(ScrapeErrorInputRequired, ScrapeErrorInputOptional):
    pass


class ScrapeErrorPayloadRequired(TypedDict):
    error: str
    createdAt: int


class ScrapeErrorPayloadOptional(TypedDict, total=False):
    jobId: str
    sourceUrl: str
    siteId: str
    event: str
    status: str
    metadata: Any
    payload: Any


class ScrapeErrorPayload(ScrapeErrorPayloadRequired, ScrapeErrorPayloadOptional):
    pass


def _clean_scrape_error_payload(payload: ScrapeErrorInput) -> ScrapeErrorPayload:
    """Drop None values and ensure Convex payload strings never receive null."""

    created_at = payload.get("createdAt")
    cleaned: Dict[str, Any] = {
        "error": payload["error"],
        "createdAt": int(created_at if created_at is not None else int(time.time() * 1000)),
    }

    optional_fields = (
        "jobId",
        "sourceUrl",
        "siteId",
        "event",
        "status",
        "metadata",
        "payload",
    )
    for key in optional_fields:
        value = payload.get(key)
        if value is not None:
            cleaned[key] = value

    return cast(ScrapeErrorPayload, cleaned)


class FirecrawlJobSchema(BaseModel):
    job_title: Optional[str] = Field(default=None, alias="job_title")
    title: Optional[str] = None
    company: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    location: Optional[str] = None
    remote: Optional[bool] = None
    level: Optional[str] = None
    salary: Optional[str | float | int] = Field(default=None, alias="salary")
    total_compensation: Optional[str | float | int] = Field(
        default=None, alias="total_compensation"
    )
    posted_at: Optional[str] = Field(default=None, alias="posted_at")

    model_config = ConfigDict(populate_by_name=True, extra="allow")


def build_job_template() -> Dict[str, str]:
    """Return a fresh FetchFox template for job detail extraction."""

    return dict(JOB_TEMPLATE)


def build_firecrawl_schema() -> Dict[str, Any]:
    """JSON schema used to ask Firecrawl for structured job fields."""

    return FirecrawlJobSchema.model_json_schema()


class Site(TypedDict):
    """Site record stored in Convex.

    `_id` and `url` are always present, the rest are optional flags/metadata returned
    by the API. Using ``NotRequired`` keeps type safety while matching runtime
    payloads.
    """

    _id: str
    url: str
    name: NotRequired[Optional[str]]
    type: NotRequired[Optional[str]]
    scrapeProvider: NotRequired[Optional[str]]
    pattern: NotRequired[Optional[str]]
    enabled: NotRequired[bool]
    lastRunAt: NotRequired[Optional[int]]
    lockedBy: NotRequired[Optional[str]]
    lockExpiresAt: NotRequired[Optional[int]]
    completed: NotRequired[Optional[bool]]


class FirecrawlWebhookEvent(TypedDict, total=False):
    """Shape of Firecrawl webhook events delivered in the raw request body.

    Matches Firecrawl docs: success (bool), type (str), id (str), data (list),
    metadata (object), and optional error when success is false. Additional fields
    observed in practice (jobId, status, statusUrl) are allowed for compatibility.
    """

    success: bool
    type: str
    event: str
    id: str
    data: List[Any]
    metadata: Dict[str, Any]
    error: NotRequired[str]
    jobId: NotRequired[str]
    status: NotRequired[str]
    status_url: NotRequired[str]
    statusUrl: NotRequired[str]


async def fetch_seen_urls_for_site(source_url: str, pattern: Optional[str]) -> List[str]:
    """Return every URL we've already scraped for the site so scrapers can skip them."""

    from ..services.convex_client import convex_query

    payload: Dict[str, Any] = {"sourceUrl": source_url}
    # Convex optional validators reject explicit nulls; only send pattern when we have a string.
    if pattern is not None:
        payload["pattern"] = pattern

    try:
        res = await convex_query("router:listSeenJobUrlsForSite", payload)
    except Exception:
        return []

    urls = res.get("urls", []) if isinstance(res, dict) else []
    return [u for u in urls if isinstance(u, str)]


def extract_raw_body_from_fetchfox_result(result: Any) -> str:
    """Best-effort extraction of the primary body/html from a FetchFox scrape result."""

    if isinstance(result, str):
        return result

    if isinstance(result, dict):
        for key in ("raw_html", "html", "content", "body", "text"):
            val = result.get(key)
            if isinstance(val, str) and val.strip():
                return val

        nested_results = result.get("results")
        if isinstance(nested_results, dict):
            for key in ("raw_html", "html", "content", "body", "text"):
                val = nested_results.get(key)
                if isinstance(val, str) and val.strip():
                    return val

        nested_items = result.get("items")
        if isinstance(nested_items, list) and nested_items:
            first = nested_items[0]
            if isinstance(first, dict):
                for key in ("raw_html", "html", "content", "body", "text"):
                    val = first.get(key)
                    if isinstance(val, str) and val.strip():
                        return val

    try:
        return json.dumps(result, ensure_ascii=False)
    except Exception:
        return str(result)


@activity.defn
async def fetch_sites() -> List[Site]:
    from ..services.convex_client import convex_query

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
    from ..services.convex_client import convex_mutation

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
    return res  # type: ignore[return-value]


@activity.defn
async def scrape_site(site: Site) -> Dict[str, Any]:
    """Scrape a site, selecting provider based on per-site preference."""

    site_type = (site.get("type") or "general").lower()
    provider = (site.get("scrapeProvider") or "").lower()

    if site_type == "greenhouse" and not provider:
        provider = "firecrawl"
    if provider not in {"fetchfox", "firecrawl"}:
        provider = "fetchfox"

    if provider == "firecrawl":
        if settings.firecrawl_api_key:
            skip_urls = await fetch_seen_urls_for_site(site["url"], site.get("pattern"))
            return await scrape_site_firecrawl(site, skip_urls)
        if settings.fetchfox_api_key:
            return await scrape_site_fetchfox(site)
        raise ApplicationError(
            "FIRECRAWL_API_KEY or FETCHFOX_API_KEY env var is required",
            non_retryable=True,
        )

    if settings.fetchfox_api_key:
        return await scrape_site_fetchfox(site)
    if settings.firecrawl_api_key:
        skip_urls = await fetch_seen_urls_for_site(site["url"], site.get("pattern"))
        return await scrape_site_firecrawl(site, skip_urls)

    raise ApplicationError(
        "FIRECRAWL_API_KEY or FETCHFOX_API_KEY env var is required",
        non_retryable=True,
    )


def _build_firecrawl_webhook(site: Site, kind: str) -> Dict[str, Any]:
    """Construct webhook config with metadata for Firecrawl jobs."""

    # Greenhouse jobs only need final status updates; skip verbose page/start events.
    if kind == "greenhouse_listing":
        events = ["completed", "failed"]
    else:
        events = [
            "batch_scrape.started",
            "batch_scrape.page",
            "batch_scrape.completed",
            "batch_scrape.failed",
        ]

    metadata: Dict[str, Any] = {
        "siteId": site.get("_id"),
        "siteUrl": site.get("url"),
        "siteType": site.get("type") or "general",
        "pattern": site.get("pattern"),
        "kind": kind,
        "providerVersion": "v2",
    }

    # Firecrawl rejects nulls in webhook metadata; drop any None values.
    metadata = {k: v for k, v in metadata.items() if v is not None}

    return {
        "url": f"{_convex_http_base()}/api/firecrawl/webhook",
        "events": events,
        "metadata": metadata,
    }


def _stringify_firecrawl_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Convert webhook metadata values to strings for Firecrawl's API contract."""

    def _to_string(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    return {key: _to_string(val) for key, val in metadata.items() if val is not None}


def _should_use_mock_firecrawl(site_url: Optional[str]) -> bool:
    """Return True when the site URL should route to the mock Firecrawl client."""

    flag = os.getenv("FIRECRAWL_FORCE_MOCK")
    if flag is not None:
        return flag.lower() not in {"", "0", "false"}

    # Tests expect the real (monkeypatched) Firecrawl client even for example.com
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False

    if not site_url:
        return False
    try:
        hostname = urlparse(site_url).hostname
    except Exception:
        return False
    return bool(hostname and hostname.lower().endswith("example.com"))


def _should_mock_convex_webhooks() -> bool:
    """Return True when webhook bookkeeping should skip real Convex calls."""

    flag = os.getenv("MOCK_CONVEX_WEBHOOKS")
    if flag is not None:
        return flag.lower() not in {"", "0", "false"}

    # Default to mock when running tests to avoid hitting real Convex.
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


class _WebhookModel:
    """Minimal shim to satisfy Firecrawl client's model_dump expectation."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def model_dump(self, *_, exclude_none: bool = False, **__) -> Dict[str, Any]:  # type: ignore[override]
        if not exclude_none:
            return self._data

        def _strip_none(val: Any) -> Any:
            if isinstance(val, dict):
                return {k: _strip_none(v) for k, v in val.items() if v is not None}
            if isinstance(val, list):
                return [_strip_none(v) for v in val if v is not None]
            return val

        cleaned = _strip_none(self._data)
        return cleaned if isinstance(cleaned, dict) else self._data


async def _start_firecrawl_batch(
    start_fn: Callable[[Any], Any],
    webhook_model: Any,
    webhook_payload: Dict[str, Any],
) -> Any:
    """Invoke a Firecrawl start function, retrying once if webhook lacks model_dump."""

    try:
        return await asyncio.to_thread(start_fn, webhook_model)
    except AttributeError as exc:
        if "model_dump" not in str(exc):
            raise

        retry_webhook = _WebhookModel(webhook_payload)
        logger.warning("Retrying Firecrawl start with wrapped webhook after model_dump error")
        return await asyncio.to_thread(start_fn, retry_webhook)


def _metadata_urls_to_list(value: Any) -> List[str]:
    """Parse a Firecrawl metadata urls/seedUrls field into a list of strings."""

    if isinstance(value, list):
        return [url for url in value if isinstance(url, str) and url.strip()]

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [url for url in parsed if isinstance(url, str) and url.strip()]
        except Exception:
            pass
        return [value] if value.strip() else []

    return []


def _serialize_firecrawl_job(job: Any, site: Site, webhook: Dict[str, Any], kind: str) -> Dict[str, Any]:
    """Normalize a Firecrawl start response to a small payload."""

    job_id = None
    status_url = None
    if hasattr(job, "id"):
        job_id = getattr(job, "id")
    if hasattr(job, "jobId"):
        job_id = getattr(job, "jobId")
    if hasattr(job, "status_url"):
        status_url = getattr(job, "status_url")
    if hasattr(job, "statusUrl"):
        status_url = getattr(job, "statusUrl")

    if isinstance(job, dict):
        job_id = job.get("id") or job.get("jobId") or job_id
        status_url = job.get("status_url") or job.get("statusUrl") or status_url

    if not job_id:
        raise ApplicationError(
            "Firecrawl start response did not include a job id", non_retryable=True
        )
    job_id_str = str(job_id)
    status_url_clean = _build_provider_status_url(
        "firecrawl", job_id_str, status_url=status_url, kind=kind
    )

    payload = {
        "jobId": job_id_str,
        "statusUrl": status_url_clean,
        "siteId": site.get("_id"),
        "siteUrl": site.get("url"),
        "kind": kind,
        "webhookUrl": webhook.get("url"),
    }
    return payload


async def _log_scrape_error(payload: ScrapeErrorInput) -> None:
    """Persist scrape/HTTP errors to Convex for audit visibility."""

    from ..services.convex_client import convex_mutation

    data = _clean_scrape_error_payload(payload)
    try:
        await convex_mutation("router:insertScrapeError", data)
    except Exception:
        # Best-effort; do not raise
        return


async def _record_pending_firecrawl_webhook(
    job: Dict[str, Any], site: Site, webhook: Dict[str, Any], kind: str
) -> Optional[str]:
    """Insert a placeholder webhook row so missing callbacks can be recovered later."""

    if _should_mock_convex_webhooks():
        return f"mock-webhook-{int(time.time() * 1000)}"

    from ..services.convex_client import get_client

    now_ms = int(time.time() * 1000)
    metadata = webhook.get("metadata") if isinstance(webhook.get("metadata"), dict) else {}
    payload = {
        "jobId": str(job.get("jobId") or job.get("id") or ""),
        "event": "pending",
        "status": "pending",
        "sourceUrl": site.get("url"),
        "siteId": site.get("_id"),
        "statusUrl": job.get("statusUrl") or None,
        "metadata": metadata,
        "payload": {"queuedAt": now_ms, "kind": kind},
        "receivedAt": now_ms,
    }

    # Convex optional strings must be omitted when null/None.
    if payload.get("statusUrl") is None:
        payload.pop("statusUrl", None)

    try:
        client = get_client()
        loop = asyncio.get_running_loop()
        res = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                client.mutation,
                "router:insertFirecrawlWebhookEvent",
                payload,
            ),
            timeout=CONVEX_MUTATION_TIMEOUT_SECONDS,
        )
        if isinstance(res, str):
            return res
    except asyncio.TimeoutError:
        logger.warning(
            "Failed to record pending Firecrawl webhook job_id=%s error=timeout after %ss",
            payload.get("jobId"),
            CONVEX_MUTATION_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to record pending Firecrawl webhook job_id=%s error=%s",
            payload.get("jobId"),
            exc,
        )
    return None


@activity.defn
async def start_firecrawl_webhook_scrape(site: Site) -> Dict[str, Any]:
    """Kick off a Firecrawl batch scrape with a Convex webhook callback."""

    site_type = site.get("type") or "general"
    kind = "greenhouse_listing" if site_type == "greenhouse" else "site_crawl"

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
        from ..testing.firecrawl_mock import MockFirecrawl

        mock_client = MockFirecrawl()
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

        _log_provider_dispatch(
            "firecrawl",
            site["url"],
            kind="greenhouse_listing",
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
            error_payload: ScrapeErrorInput = {
                "sourceUrl": site.get("url"),
                "event": "start_batch_scrape",
                "error": str(exc),
                "metadata": {"kind": "greenhouse_listing"},
            }
            site_id = site.get("_id")
            if site_id is not None:
                error_payload["siteId"] = site_id
            await _log_scrape_error(error_payload)
            msg = str(exc).lower()
            retryable = "429" in msg or "rate" in msg or "timeout" in msg
            raise ApplicationError(f"Firecrawl batch start failed: {exc}", non_retryable=not retryable) from exc

        raw_start = (
            job.model_dump(mode="json", exclude_none=True)
            if hasattr(job, "model_dump")
            else job
        )
        payload = _serialize_firecrawl_job(job, site, webhook_payload, "greenhouse_listing")
        payload["metadata"] = webhook_payload.get("metadata")
        payload["receivedAt"] = int(time.time() * 1000)
        payload["rawStart"] = raw_start
        payload["providerRequest"] = provider_request
        payload["request"] = request_snapshot
        payload["webhookId"] = await _record_pending_firecrawl_webhook(
            payload, site, webhook_payload, "greenhouse_listing"
        )
        _log_sync_response(
            "firecrawl",
            action="start",
            url=site["url"],
            job_id=payload.get("jobId"),
            status_url=_build_provider_status_url(
                "firecrawl", payload.get("jobId"), status_url=payload.get("statusUrl"), kind="greenhouse_listing"
            ),
            kind="greenhouse_listing",
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

    _log_provider_dispatch(
        "firecrawl",
        site["url"],
        kind="site_crawl",
        webhook=webhook_payload.get("url"),
        siteId=site.get("_id"),
        pattern=pattern,
    )
    try:
        job = await _start_firecrawl_batch(_do_start_batch_crawl, webhook_model, webhook_payload)
    except Exception as exc:  # noqa: BLE001
        error_payload: ScrapeErrorInput = {
            "sourceUrl": site.get("url"),
            "event": "start_batch_scrape",
            "error": str(exc),
            "metadata": {"pattern": pattern},
        }
        site_id = site.get("_id")
        if site_id is not None:
            error_payload["siteId"] = site_id
        await _log_scrape_error(error_payload)
        msg = str(exc).lower()
        retryable = "429" in msg or "rate" in msg or "timeout" in msg
        raise ApplicationError(f"Firecrawl batch start failed: {exc}", non_retryable=not retryable) from exc

    raw_start = (
        job.model_dump(mode="json", exclude_none=True)
        if hasattr(job, "model_dump")
        else job
    )
    payload = _serialize_firecrawl_job(job, site, webhook_payload, "site_crawl")
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
        payload, site, webhook_payload, "site_crawl"
    )
    _log_sync_response(
        "firecrawl",
        action="start",
        url=site["url"],
        job_id=payload.get("jobId"),
        status_url=_build_provider_status_url(
            "firecrawl", payload.get("jobId"), status_url=payload.get("statusUrl"), kind="site_crawl"
        ),
        kind="site_crawl",
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
async def scrape_site_fetchfox(site: Site) -> Dict[str, Any]:
    if not settings.fetchfox_api_key:
        # Mark as non-retryable to avoid endless attempts for a known config issue
        raise ApplicationError(
            "FETCHFOX_API_KEY env var is required for FetchFox", non_retryable=True
        )

    pattern = site.get("pattern")
    skip_urls = await fetch_seen_urls_for_site(site["url"], pattern)
    start_urls = [site["url"]]
    template = build_job_template()

    request = FetchFoxScrapeRequest(
        pattern=pattern,
        start_urls=start_urls,
        max_depth=5,
        max_visits=MAX_FETCHFOX_VISITS,
        template=template,
        priority=FetchFoxPriority(skip=skip_urls),
        content_transform="text_only",
    ).model_dump(exclude_none=True)
    request_snapshot = _build_request_snapshot(
        request,
        provider="fetchfox",
        method="POST",
        url="https://api.fetchfox.ai/scrape",
    )

    # Run blocking FetchFox init and scrape in a thread
    def _do_scrape(scrape_payload: Dict[str, Any]):
        fox = FetchFox(api_key=settings.fetchfox_api_key)
        return fox.scrape(scrape_payload)

    _log_provider_dispatch(
        "fetchfox",
        site.get("url") or "",
        pattern=pattern,
        siteId=site.get("_id"),
    )

    started_at = int(time.time() * 1000)

    # FetchFox may return dict or JSON string depending on version
    try:
        result = await asyncio.to_thread(_do_scrape, request)
        result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
    except Exception:
        # Last resort: wrap opaque content
        result_obj = {"raw": "Scrape failed or returned invalid data"}

    normalized_items = normalize_fetchfox_items(result_obj)

    completed_at = int(time.time() * 1000)

    scrape_payload = {
        "sourceUrl": site["url"],
        "pattern": pattern,
        "startedAt": started_at,
        "completedAt": completed_at,
        "request": request_snapshot,
        "items": {
            "normalized": normalized_items,
            "raw": result_obj,
            "request": request_snapshot,
            "seedUrls": start_urls,
        },
        "provider": "fetchfox",
        "costMilliCents": None,
    }

    _log_sync_response(
        "fetchfox",
        action="scrape",
        url=site.get("url"),
        kind="site_crawl",
        summary=f"items={len(normalized_items)}",
        metadata={"siteId": site.get("_id"), "pattern": pattern, "seed": len(start_urls)},
    )

    # Trim heavy fields before sending to Convex
    return trim_scrape_for_convex(scrape_payload)


@activity.defn
async def scrape_site_firecrawl(site: Site, skip_urls: Optional[List[str]] = None) -> Dict[str, Any]:
    firecrawl_api_key = settings.firecrawl_api_key
    if not firecrawl_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY env var is required for Firecrawl", non_retryable=True
        )

    # Queue Firecrawl via webhook; do not wait for crawl completion.
    job_info = await start_firecrawl_webhook_scrape(site)
    now = int(time.time() * 1000)
    async_state = "queued"
    async_response: Dict[str, Any] = {
        "jobId": job_info.get("jobId"),
        "statusUrl": job_info.get("statusUrl"),
        "webhookId": job_info.get("webhookId"),
        "kind": job_info.get("metadata", {}).get("kind"),
        "receivedAt": job_info.get("receivedAt"),
        "rawStart": job_info.get("rawStart") or job_info,
    }
    request_snapshot = job_info.get("request") or _build_request_snapshot(
        {
            "urls": [site.get("url")],
            "pattern": site.get("pattern"),
            "siteType": site.get("type") or "general",
            "skipUrls": skip_urls or [],
        },
        provider="firecrawl",
        method="POST",
        url="https://api.firecrawl.dev/v2/batch/scrape",
    )
    provider_request = job_info.get("providerRequest") or request_snapshot

    scrape_payload = {
        "sourceUrl": site["url"],
        "pattern": site.get("pattern"),
        "jobId": job_info.get("jobId"),
        "webhookId": job_info.get("webhookId"),
        "metadata": job_info.get("metadata"),
        "receivedAt": job_info.get("receivedAt"),
        "startedAt": now,
        "completedAt": now,
        "response": job_info.get("rawStart") or job_info,
        "request": request_snapshot,
        "items": {
            "normalized": [],
            "provider": "firecrawl",
            "queued": True,
            "jobId": job_info.get("jobId"),
            "statusUrl": job_info.get("statusUrl"),
            "webhookId": job_info.get("webhookId"),
            "receivedAt": job_info.get("receivedAt"),
            "request": request_snapshot,
            "rawStart": job_info.get("rawStart") or job_info,
        },
        "provider": "firecrawl",
        "workflowName": "ScraperFirecrawlQueued",
        "asyncState": async_state,
        "asyncResponse": async_response,
        "providerRequest": provider_request,
    }

    return scrape_payload


@activity.defn
async def fetch_greenhouse_listing(site: Site) -> Dict[str, Any]:
    """Fetch a Greenhouse board JSON payload using FetchFox and parse job URLs."""

    if settings.firecrawl_api_key:
        return await fetch_greenhouse_listing_firecrawl(site)
    if not settings.fetchfox_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY or FETCHFOX_API_KEY env var is required",
            non_retryable=True,
        )

    request = FetchFoxScrapeRequest(
        start_urls=[site["url"]],
        max_depth=0,
        max_visits=1,
        template={"raw": "str | None"},
        priority=FetchFoxPriority(skip=[]),
        content_transform="full_html",
    ).model_dump(exclude_none=True)

    def _do_scrape(scrape_payload: Dict[str, Any]):
        fox = FetchFox(api_key=settings.fetchfox_api_key)
        return fox.scrape(scrape_payload)

    _log_provider_dispatch("fetchfox", site.get("url") or "", kind="greenhouse_board", siteId=site.get("_id"))

    started_at = int(time.time() * 1000)

    try:
        result = await asyncio.to_thread(_do_scrape, request)
        result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Failed to fetch Greenhouse board: {exc}") from exc

    raw_text = extract_raw_body_from_fetchfox_result(result_obj)

    try:
        board: GreenhouseBoardResponse = load_greenhouse_board(raw_text or result_obj)
        job_urls = extract_greenhouse_job_urls(board)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Unable to parse Greenhouse board payload: {exc}") from exc

    completed_at = int(time.time() * 1000)

    _log_sync_response(
        "fetchfox",
        action="greenhouse_board",
        url=site.get("url"),
        kind="greenhouse_listing",
        summary=f"job_urls={len(job_urls)}",
        metadata={"siteId": site.get("_id"), "raw_len": len(raw_text or "") if isinstance(raw_text, str) else None},
    )

    return {
        "raw": raw_text,
        "job_urls": job_urls,
        "startedAt": started_at,
        "completedAt": completed_at,
    }


@activity.defn
async def fetch_greenhouse_listing_firecrawl(site: Site) -> Dict[str, Any]:
    """Fetch a Greenhouse board JSON payload using Firecrawl and parse job URLs."""

    firecrawl_api_key = settings.firecrawl_api_key
    if not firecrawl_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY env var is required for Firecrawl",
            non_retryable=True,
        )

    # Greenhouse board endpoint returns JSON; fetching raw HTML via batch scrape avoids LLM prompting cost
    raw_html_format = "rawHtml"

    def _do_scrape() -> Any:
        client = Firecrawl(api_key=firecrawl_api_key)
        return client.batch_scrape(
            [site["url"]],
            formats=[raw_html_format],
            proxy="auto",
            max_age=FIRECRAWL_CACHE_MAX_AGE_MS,
            store_in_cache=True,
            ignore_invalid_urls=True,
        )

    _log_provider_dispatch("firecrawl", site.get("url") or "", kind="greenhouse_board", siteId=site.get("_id"))

    started_at = int(time.time() * 1000)
    try:
        job = await asyncio.to_thread(_do_scrape)
    except RequestTimeoutError as exc:
        raise ApplicationError(
            f"Firecrawl scrape timed out for {site.get('url')}: {exc}", non_retryable=True
        ) from exc
    except ValueError as exc:
        raise ApplicationError(
            f"Firecrawl scrape failed (invalid json format configuration): {exc}", non_retryable=True
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Firecrawl scrape failed: {exc}") from exc

    docs = getattr(job, "data", None) if hasattr(job, "data") else None
    if docs is None and isinstance(job, dict):
        docs = job.get("data")
    first_doc = docs[0] if isinstance(docs, list) and docs else None

    raw_json = getattr(first_doc, "json", None) if first_doc is not None else None
    if raw_json is None and isinstance(first_doc, dict):
        raw_json = first_doc.get("json")
    raw_text = None

    # Prefer raw_html/html/text when using rawHtml format
    if first_doc is not None:
        for key in ("raw_html", "html", "text", "content"):
            val = getattr(first_doc, key, None) if not isinstance(first_doc, dict) else first_doc.get(key)
            if isinstance(val, str) and val.strip():
                raw_text = val
                break

    if raw_text is None:
        if isinstance(raw_json, str):
            raw_text = raw_json
        else:
            try:
                raw_text = json.dumps(raw_json or {}, ensure_ascii=False)
            except Exception:
                raw_text = "{}"

    try:
        board: GreenhouseBoardResponse = load_greenhouse_board(raw_text or raw_json or first_doc or {})
        job_urls = extract_greenhouse_job_urls(board)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Unable to parse Greenhouse board payload (Firecrawl): {exc}") from exc

    _log_sync_response(
        "firecrawl",
        action="greenhouse_board",
        url=site.get("url"),
        kind="greenhouse_listing",
        summary=f"job_urls={len(job_urls)}",
        metadata={
            "siteId": site.get("_id"),
            "raw_len": len(raw_text or "") if isinstance(raw_text, str) else None,
        },
    )

    return {
        "raw": raw_text,
        "job_urls": job_urls,
        "startedAt": started_at,
        "completedAt": int(time.time() * 1000),
    }


@activity.defn
async def filter_existing_job_urls(urls: List[str]) -> List[str]:
    """Return the subset of URLs that already exist in Convex jobs table."""

    if not urls:
        return []
    from ..services.convex_client import convex_query

    try:
        data = await convex_query("router:findExistingJobUrls", {"urls": urls})
    except Exception:
        return []

    existing = data.get("existing", []) if isinstance(data, dict) else []
    if not isinstance(existing, list):
        return []

    return [u for u in existing if isinstance(u, str)]


@activity.defn
async def scrape_greenhouse_jobs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Scrape new Greenhouse job URLs with a single FetchFox request."""

    if settings.firecrawl_api_key:
        return await scrape_greenhouse_jobs_firecrawl(payload)

    if not settings.fetchfox_api_key:
        raise ApplicationError(
            "FETCHFOX_API_KEY env var is required for FetchFox",
            non_retryable=True,
        )

    urls: List[str] = [u for u in payload.get("urls", []) if isinstance(u, str) and u.strip()]
    source_url: str = payload.get("source_url") or (urls[0] if urls else "")

    if not urls:
        return {"scrape": None, "jobsScraped": 0}

    template = build_job_template()
    request = FetchFoxScrapeRequest(
        pattern=None,
        start_urls=urls,
        max_depth=1,
        max_visits=min(MAX_FETCHFOX_VISITS, max(len(urls), 1)),
        template=template,
        priority=FetchFoxPriority(skip=[]),
        content_transform="text_only",
    ).model_dump(exclude_none=True)
    request_snapshot = _build_request_snapshot(
        request,
        provider="fetchfox",
        method="POST",
        url="https://api.fetchfox.ai/scrape",
    )

    def _do_scrape(scrape_payload: Dict[str, Any]):
        fox = FetchFox(api_key=settings.fetchfox_api_key)
        return fox.scrape(scrape_payload)

    _log_provider_dispatch("fetchfox", source_url, kind="greenhouse_jobs", urls=len(urls))

    started_at = int(time.time() * 1000)

    try:
        result = await asyncio.to_thread(_do_scrape, request)
        result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"Greenhouse detail scrape failed: {exc}") from exc

    normalized_items = normalize_fetchfox_items(result_obj)
    completed_at = int(time.time() * 1000)

    scrape_payload = {
        "sourceUrl": source_url,
        "pattern": None,
        "startedAt": started_at,
        "completedAt": completed_at,
        "request": request_snapshot,
        "items": {
            "normalized": normalized_items,
            "raw": result_obj,
            "seedUrls": urls,
            "request": request_snapshot,
        },
    }

    trimmed = trim_scrape_for_convex(scrape_payload)
    items = trimmed.get("items", {})
    if isinstance(items, dict):
        items.setdefault("seedUrls", urls)
        trimmed["items"] = items

    _log_sync_response(
        "fetchfox",
        action="greenhouse_jobs",
        url=source_url,
        kind="greenhouse_jobs",
        summary=f"items={len(normalized_items)}",
        metadata={"urls": len(urls), "siteId": payload.get("site_id") or payload.get("siteId")},
    )

    return {"scrape": trimmed, "jobsScraped": len(normalized_items)}


@activity.defn
async def scrape_greenhouse_jobs_firecrawl(payload: Dict[str, Any]) -> Dict[str, Any]:
    urls: List[str] = [u for u in payload.get("urls", []) if isinstance(u, str) and u.strip()]
    source_url: str = payload.get("source_url") or (urls[0] if urls else "")
    idempotency_key: Optional[str] = payload.get("idempotency_key") or payload.get("webhook_id")

    if not urls:
        return {"scrape": None, "jobsScraped": 0}

    firecrawl_api_key = settings.firecrawl_api_key
    if not firecrawl_api_key:
        raise ApplicationError(
            "FIRECRAWL_API_KEY env var is required for Firecrawl",
            non_retryable=True,
        )

    schema = build_firecrawl_schema()
    scrape_options = ScrapeOptions(
        formats=[
            "markdown",
            {"type": "json", "schema": schema},
        ]
    )

    def _scrape_batch() -> Any:
        client = Firecrawl(api_key=firecrawl_api_key)
        formats: list[Any] = list(scrape_options.formats or [])
        return client.batch_scrape(
            urls,
            formats=formats,
            proxy="auto",
            max_age=FIRECRAWL_CACHE_MAX_AGE_MS,
            store_in_cache=True,
            max_concurrency=5,
            idempotency_key=idempotency_key,
        )

    _log_provider_dispatch(
        "firecrawl",
        source_url,
        kind="greenhouse_jobs",
        urls=len(urls),
        idempotency=idempotency_key,
    )

    try:
        result = await asyncio.to_thread(_scrape_batch)
    except Exception as exc:  # noqa: BLE001
        error_payload: ScrapeErrorInput = {
            "sourceUrl": source_url,
            "event": "batch_scrape",
            "status": "error",
            "error": str(exc),
            "metadata": {"urls": urls},
        }
        if idempotency_key is not None:
            error_payload["jobId"] = idempotency_key
        await _log_scrape_error(error_payload)
        msg = str(exc).lower()
        if isinstance(exc, PaymentRequiredError) or "payment required" in msg or "insufficient credits" in msg:
            raise PaymentRequiredWorkflowError(f"Firecrawl batch scrape failed: {exc}") from exc

        if isinstance(exc, RequestTimeoutError) or "timeout" in msg:
            raise TimeoutWorkflowError(f"Firecrawl batch scrape failed: {exc}") from exc

        if "429" in msg or "too many requests" in msg or "rate" in msg:
            raise RateLimitWorkflowError(f"Firecrawl batch scrape failed: {exc}") from exc

        raise NonRetryableWorkflowError(f"Firecrawl batch scrape failed: {exc}") from exc

    raw_payload = (
        result.model_dump(mode="json", exclude_none=True)
        if hasattr(result, "model_dump")
        else result
    )
    batch_id: str | None = None
    for key in ("batch_id", "batchId", "job_id", "jobId", "id"):
        if hasattr(result, key):
            candidate = getattr(result, key)
            if isinstance(candidate, str) and candidate.strip():
                batch_id = candidate.strip()
                break
        if isinstance(raw_payload, dict) and key in raw_payload and isinstance(raw_payload[key], str):
            candidate = cast(str, raw_payload[key])
            if candidate.strip():
                batch_id = candidate.strip()
                break
    if batch_id is None and isinstance(idempotency_key, str):
        batch_id = idempotency_key

    normalized_items = normalize_firecrawl_items(result)
    completed_at = int(time.time() * 1000)

    request_payload = {
        "urls": urls,
        "options": {
            "formats": scrape_options.formats,
            "proxy": "auto",
            "max_age": FIRECRAWL_CACHE_MAX_AGE_MS,
            "store_in_cache": True,
        },
        "idempotencyKey": idempotency_key,
        "sourceUrl": source_url,
        "kind": "greenhouse_listing",
    }
    if batch_id:
        request_payload["batchId"] = batch_id
    request_snapshot = _build_request_snapshot(
        request_payload,
        provider="firecrawl",
        method="POST",
        url="https://api.firecrawl.dev/v2/batch/scrape",
    )

    scrape_payload = {
        "sourceUrl": source_url,
        "pattern": None,
        "startedAt": int(time.time() * 1000),
        "completedAt": completed_at,
        "batchId": batch_id,
        "request": request_snapshot,
        "providerRequest": request_payload,
        "items": {
            "normalized": normalized_items,
            "raw": raw_payload,
            "seedUrls": urls,
            "provider": "firecrawl",
            "request": request_snapshot,
        },
    }

    trimmed = trim_scrape_for_convex(scrape_payload)
    items = trimmed.get("items", {})
    if isinstance(items, dict):
        items.setdefault("seedUrls", urls)
        trimmed["items"] = items

    _log_sync_response(
        "firecrawl",
        action="greenhouse_jobs",
        url=source_url,
        kind="greenhouse_jobs",
        summary=f"items={len(normalized_items)}",
        metadata={"urls": len(urls), "job_id": batch_id or idempotency_key},
    )

    return {"scrape": trimmed, "jobsScraped": len(normalized_items)}


@activity.defn
async def fetch_pending_firecrawl_webhooks(limit: int = 25, event: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return unprocessed Firecrawl webhook rows from Convex."""

    from ..services.convex_client import convex_query

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

    from ..services.convex_client import convex_query

    try:
        res = await convex_query("router:getFirecrawlWebhookStatus", {"jobId": job_id})
    except Exception:
        return {}
    return res if isinstance(res, dict) else {}


@activity.defn
async def mark_firecrawl_webhook_processed(webhook_id: str, error: Optional[str] = None) -> None:
    """Mark a webhook row as processed and optionally attach an error."""

    from ..services.convex_client import convex_mutation

    payload = {"id": webhook_id}
    if error is not None:
        payload["error"] = error

    await convex_mutation(
        "router:markFirecrawlWebhookProcessed",
        payload,
    )


def _mock_firecrawl_status_response(
    *,
    event: FirecrawlWebhookEvent,
    job_id: str,
    kind: str,
    site_id: Optional[str],
    source_url: Optional[str],
    pattern: Optional[str],
    status_endpoint: str,
    request_snapshot: Dict[str, Any],
    first_seen_ms: int,
) -> Dict[str, Any]:
    """Return a canned Firecrawl status payload for example.com sites."""

    metadata_raw = event.get("metadata")
    metadata: Dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
    now_ms = int(time.time() * 1000)
    async_response_block = {
        "jobId": job_id,
        "status": "completed",
        "event": event.get("event") or event.get("type"),
        "receivedAt": event.get("receivedAt"),
        "payload": event,
        "metadata": metadata,
        "mock": True,
    }

    if kind == "greenhouse_listing":
        return {
            "kind": "greenhouse_listing",
            "siteId": site_id,
            "siteUrl": source_url,
            "status": "completed",
            "httpStatus": "mock",
            "request": request_snapshot,
            "response": {"status": "completed", "raw": "mock_firecrawl_listing"},
            "asyncResponse": async_response_block,
            "itemsCount": 0,
            "jobsScraped": 0,
            "job_urls": [],
            "raw": "{}",
        }

    seed_urls = metadata.get("urls") or metadata.get("seedUrls")
    seed_list = _metadata_urls_to_list(seed_urls)
    scrape_payload = {
        "sourceUrl": source_url,
        "pattern": pattern,
        "startedAt": first_seen_ms,
        "completedAt": now_ms,
        "items": {
            "normalized": [],
            "raw": {"status": "completed", "mock": True},
            "provider": "firecrawl_mock",
            "seedUrls": seed_list,
            "request": request_snapshot,
        },
        "provider": "firecrawl_mock",
        "workflowName": "ProcessWebhookScrape",
        "kind": kind,
        "request": request_snapshot,
    }
    return {
        "kind": kind,
        "siteId": site_id,
        "siteUrl": source_url,
        "status": "completed",
        "httpStatus": "mock",
        "request": request_snapshot,
        "response": {"status": "completed", "mock": True, "statusUrl": status_endpoint},
        "asyncResponse": async_response_block,
        "itemsCount": 0,
        "jobsScraped": 0,
        "scrape": scrape_payload,
    }


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
    kind = metadata.get("kind") or ("greenhouse_listing" if metadata.get("siteType") == "greenhouse" else "site_crawl")
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
        from ..services.convex_client import convex_mutation

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
            cleaned_payload = _clean_scrape_error_payload(error_payload)
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

    if kind == "greenhouse_listing":
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
                    "kind": "greenhouse_listing",
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
            f"job_id={job_id} kind=greenhouse_listing status={status_value} "
            f"urls={len(job_urls)} http={http_status} status_url={status_link or 'n/a'}\x1b[0m"
        )
        return {
            "kind": "greenhouse_listing",
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
    normalized_items = normalize_firecrawl_items(status)
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


def normalize_firecrawl_items(payload: Any) -> List[Dict[str, Any]]:
    """Convert a Firecrawl crawl job into normalized job rows."""

    documents: List[Any] = []
    if hasattr(payload, "data"):
        documents = getattr(payload, "data") or []
    elif isinstance(payload, dict):
        documents = payload.get("data", []) or []

    normalized: List[Dict[str, Any]] = []
    for doc in documents:
        doc_dict: Dict[str, Any] | None = None
        if hasattr(doc, "model_dump"):
            doc_dict = doc.model_dump(mode="json", exclude_none=True)
        elif isinstance(doc, dict):
            doc_dict = doc
        if not doc_dict:
            continue

        json_payload = doc_dict.get("json") or doc_dict.get("data")
        parsed_payload = _parse_firecrawl_json(json_payload)
        for row in _rows_from_firecrawl_payload(parsed_payload):
            norm = normalize_single_row(row)
            if norm:
                normalized.append(norm)

    return normalized


def _extract_first_json_doc(payload: Any) -> Any:
    """Return the first json/data field from a Firecrawl status payload."""

    documents: List[Any] = []
    if hasattr(payload, "data"):
        documents = getattr(payload, "data") or []
    elif isinstance(payload, dict):
        documents = payload.get("data") or []

    for doc in documents:
        val = None
        if hasattr(doc, "json"):
            val = getattr(doc, "json")
        elif isinstance(doc, dict):
            val = doc.get("json") or doc.get("data")
        if val is not None:
            return val

    return None


def _extract_first_text_doc(payload: Any) -> str | None:
    """Return the first raw/html/text field from a Firecrawl status payload."""

    documents: List[Any] = []
    if hasattr(payload, "data"):
        documents = getattr(payload, "data") or []
    elif isinstance(payload, dict):
        documents = payload.get("data") or []

    for doc in documents:
        if isinstance(doc, str):
            return doc
        if hasattr(doc, "raw_html") and isinstance(getattr(doc, "raw_html"), str):
            return getattr(doc, "raw_html")
        if hasattr(doc, "html") and isinstance(getattr(doc, "html"), str):
            return getattr(doc, "html")
        if isinstance(doc, dict):
            for key in ("raw_html", "html", "text", "content"):
                val = doc.get(key)
                if isinstance(val, str):
                    return val
    return None


def _jobs_from_scrape_items(
    items: Any,
    *,
    default_posted_at: int,
    scraped_at: Optional[int] = None,
    scraped_with: Optional[str] = None,
    workflow_name: Optional[str] = None,
    scraped_cost_milli_cents: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Convert trimmed scrape items into Convex job ingest shape, carrying scrape metadata."""

    jobs: List[Dict[str, Any]] = []
    normalized = None
    if isinstance(items, dict):
        normalized = items.get("normalized")
    if not isinstance(normalized, list):
        return jobs

    for row in normalized:
        if not isinstance(row, dict):
            continue
        job = {
            "title": row.get("title") or row.get("job_title") or "Untitled",
            "company": row.get("company") or "Unknown",
            "description": row.get("description") or "",
            "location": row.get("location") or "",
            "remote": bool(row.get("remote")),
            "level": row.get("level") or "mid",
            "totalCompensation": int(row.get("total_compensation") or DEFAULT_TOTAL_COMPENSATION),
            "url": row.get("url") or "",
            "postedAt": int(row.get("posted_at") or default_posted_at),
        }
        if not job["url"]:
            continue
        if scraped_at:
            job["scrapedAt"] = scraped_at
        if scraped_with:
            job["scrapedWith"] = scraped_with
        if workflow_name:
            job["workflowName"] = workflow_name
        if scraped_cost_milli_cents is not None:
            job["scrapedCostMilliCents"] = scraped_cost_milli_cents
        jobs.append(job)

    return jobs


def _parse_firecrawl_json(payload: Any) -> Any:
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return None
    return payload


def _rows_from_firecrawl_payload(payload: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(payload, list):
        rows.extend([item for item in payload if isinstance(item, dict)])
    elif isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            rows.extend([i for i in items if isinstance(i, dict)])
        else:
            rows.append(payload)
    return rows


def normalize_fetchfox_items(payload: Any) -> List[Dict[str, Any]]:
    """Convert a FetchFox scrape response into normalized job objects for Convex ingestion.

    We keep the raw payload alongside normalized rows so the UI can still render the raw JSON
    block while top-level fields stay clean.
    """

    def collect_rows(obj: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if isinstance(obj, list):
            rows.extend([r for r in obj if isinstance(r, dict)])
            return rows

        if isinstance(obj, dict):
            if isinstance(obj.get("normalized"), list):
                rows.extend([r for r in obj["normalized"] if isinstance(r, dict)])
            if isinstance(obj.get("items"), list):
                rows.extend([r for r in obj["items"] if isinstance(r, dict)])
            if isinstance(obj.get("results"), list):
                rows.extend([r for r in obj["results"] if isinstance(r, dict)])
            results_obj = obj.get("results")
            if isinstance(results_obj, dict):
                if isinstance(results_obj.get("items"), list):
                    rows.extend([r for r in results_obj["items"] if isinstance(r, dict)])
                if isinstance(results_obj.get("normalized"), list):
                    rows.extend([r for r in results_obj["normalized"] if isinstance(r, dict)])
            # Some payloads nest under a "data" key
            if isinstance(obj.get("data"), dict):
                rows.extend(collect_rows(obj.get("data")))

        return rows

    raw_rows = collect_rows(payload)
    normalized: List[Dict[str, Any]] = []
    for row in raw_rows:
        norm = normalize_single_row(row)
        if norm:
            normalized.append(norm)

    return normalized


def normalize_single_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_title_value = row.get("job_title") or row.get("title")
    raw_title = stringify(raw_title_value) if raw_title_value is not None else ""
    title = raw_title or stringify(row.get("job_title") or row.get("title") or "Untitled")
    url = stringify(row.get("url") or row.get("link") or row.get("href") or row.get("_url") or "")
    if not url:
        return None
    if not title_matches_required_keywords(raw_title or None):
        return None

    company_raw = stringify(
        row.get("company") or row.get("employer") or row.get("organization") or ""
    )
    company = company_raw or derive_company_from_url(url) or "Unknown"

    location = stringify(row.get("location") or row.get("city") or row.get("region") or "")
    remote = coerce_remote(row.get("remote"), location, title)
    if not location:
        location = "Remote" if remote else "Unknown"

    level = coerce_level(row.get("level"), title)
    description = extract_description(row)
    if len(description) > MAX_DESCRIPTION_CHARS:
        description = description[:MAX_DESCRIPTION_CHARS]
    total_comp = parse_compensation(
        row.get("total_compensation") or row.get("salary") or row.get("compensation")
    )
    posted_at = parse_posted_at(
        row.get("posted_at") or row.get("postedAt") or row.get("date") or row.get("_timestamp")
    )

    normalized_row: Dict[str, Any] = {
        "job_title": title,
        "title": title,
        "company": company,
        "location": location,
        "remote": remote,
        "level": level,
        "total_compensation": total_comp,
        "url": url,
        "description": description,
        "posted_at": posted_at,
    }

    return normalized_row


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return str(value)


def derive_company_from_url(url: str) -> str:
    try:
        hostname = urlparse(url).hostname or ""
    except Exception:
        return ""

    hostname = hostname.lower()
    # Strip common subdomains seen on career sites
    for prefix in ("careers.", "jobs.", "boards.", "boards-", "job-", "boards-"):
        if hostname.startswith(prefix):
            hostname = hostname[len(prefix) :]
            break

    parts = hostname.split(".")
    if len(parts) >= 2:
        name = parts[-2]
    elif parts:
        name = parts[0]
    else:
        return ""

    # Convert hyphenated hostnames to title case words
    cleaned = re.sub(r"[^a-z0-9]+", " ", name).strip()
    return cleaned.title() if cleaned else ""


def coerce_remote(value: Any, location: str, title: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "yes", "remote", "hybrid", "fully remote"}:
            return True
    loc_lower = (location or "").lower()
    title_lower = (title or "").lower()
    return "remote" in loc_lower or "remote" in title_lower


def coerce_level(value: Any, title: str) -> str:
    if isinstance(value, str):
        normalized = value.lower()
    else:
        normalized = ""

    title_lower = title.lower()
    markers = normalized or title_lower
    if any(token in markers for token in ("staff", "principal")):
        return "staff"
    if any(token in markers for token in ("senior", "sr ", "sr.", "sr-", "sr/")):
        return "senior"
    if any(token in markers for token in ("lead", "manager", "director", "vp", "chief", "head")):
        return "senior"
    if "intern" in markers:
        return "junior"
    if "jr" in markers or "junior" in markers:
        return "junior"
    return "mid"


def parse_compensation(value: Any) -> int:
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    if isinstance(value, str):
        numbers = re.findall(r"[0-9][0-9,\.]+", value.replace("\u00a0", " "))
        if numbers:
            try:
                # Choose the highest number to approximate total comp if range provided
                parsed = max(float(num.replace(",", "")) for num in numbers)
                if parsed > 0:
                    return int(parsed)
            except ValueError:
                pass
    return DEFAULT_TOTAL_COMPENSATION


def extract_description(row: Dict[str, Any]) -> str:
    for key in ("description", "job_description", "desc", "body", "summary"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    # Fallback to raw JSON if nothing else is available
    try:
        return json.dumps(row, ensure_ascii=False)
    except Exception:
        return str(row)


def parse_posted_at(value: Any) -> int:
    """Return a UNIX epoch (ms). Defaults to current time if missing."""

    now_ms = int(time.time() * 1000)
    if value is None:
        return now_ms

    if isinstance(value, (int, float)):
        # Heuristic: numbers above 10^12 are already ms, otherwise treat as seconds
        if value > 1e12:
            return int(value)
        if value > 1e9:
            return int(value * 1000)
        return now_ms

    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            pass

    return now_ms


def _shrink_payload(value: Any, max_chars: int) -> Any:
    """Return payload if small enough, otherwise a truncated string preview."""

    if value is None:
        return None

    try:
        serialized = json.dumps(value, ensure_ascii=False)
    except Exception:
        try:
            serialized = str(value)
        except Exception:
            return None

    if len(serialized) <= max_chars:
        return value

    return f"{serialized[:max_chars]}... (+{len(serialized) - max_chars} chars)"


def _trim_request_snapshot(raw_request: Any, max_chars: int) -> Any:
    """Trim a request object while keeping headers/body readable."""

    if raw_request is None:
        return None

    if isinstance(raw_request, dict) and (
        "body" in raw_request or "headers" in raw_request or "url" in raw_request or "method" in raw_request
    ):
        trimmed: Dict[str, Any] = {}
        if raw_request.get("method"):
            trimmed["method"] = raw_request.get("method")
        if raw_request.get("url"):
            trimmed["url"] = raw_request.get("url")
        if "body" in raw_request:
            trimmed_body = _shrink_payload(raw_request.get("body"), max_chars)
            if trimmed_body is not None:
                trimmed["body"] = trimmed_body
        if "headers" in raw_request:
            trimmed_headers = _sanitize_headers(raw_request.get("headers"))
            if trimmed_headers:
                trimmed["headers"] = trimmed_headers
        for meta_key in ("provider", "label"):
            if raw_request.get(meta_key) is not None:
                trimmed[meta_key] = raw_request.get(meta_key)
        return trimmed if trimmed else None

    return _shrink_payload(raw_request, max_chars)


def trim_scrape_for_convex(
    scrape: Dict[str, Any],
    *,
    max_items: int = 400,
    max_description: int = MAX_DESCRIPTION_CHARS,
    raw_preview_chars: int = 8000,
    request_max_chars: int = 4000,
) -> Dict[str, Any]:
    """
    Reduce scrape payload size so it fits Convex document limits.

    - Limits number of normalized rows
    - Truncates descriptions
    - Drops embedded _raw blobs
    - Keeps an optional raw preview string instead of full raw object
    """

    items = scrape.get("items", {})
    normalized: list[Dict[str, Any]] = []

    if isinstance(items, dict):
        raw_normalized = items.get("normalized", [])
        if isinstance(raw_normalized, list):
            for row in raw_normalized[: max_items]:
                if not isinstance(row, dict):
                    continue
                new_row = dict(row)
                # Strip heavy fields
                new_row.pop("_raw", None)
                desc = stringify(new_row.get("description", ""))
                if len(desc) > max_description:
                    new_row["description"] = desc[:max_description]
                normalized.append(new_row)

    raw_preview = None
    if isinstance(items, dict) and "raw" in items and raw_preview_chars > 0:
        try:
            raw_str = json.dumps(items["raw"], ensure_ascii=False)
            raw_preview = raw_str[:raw_preview_chars]
        except Exception:
            raw_preview = None

    trimmed_items: Dict[str, Any] = {"normalized": normalized}

    def _copy_meta(key: str, value: Any) -> None:
        if value is None:
            return
        if key == "seedUrls" and isinstance(value, list):
            trimmed_items[key] = value[:200]
            return
        trimmed_items[key] = value

    request_payload = scrape.get("request")

    if isinstance(items, dict):
        for meta_key in ("provider", "seedUrls", "jobId", "statusUrl", "webhookId", "receivedAt", "queued"):
            if meta_key in items:
                _copy_meta(meta_key, items[meta_key])

        if request_payload is None:
            request_payload = (
                items.get("request")
                or items.get("request_data")
                or items.get("requestData")
            )
        raw_block = items.get("raw")
        if request_payload is None and isinstance(raw_block, dict):
            for candidate in ("request", "request_data", "requestData", "requestBody", "input"):
                if candidate in raw_block and raw_block[candidate] is not None:
                    request_payload = raw_block[candidate]
                    break

    trimmed_request = _trim_request_snapshot(request_payload, request_max_chars)
    if trimmed_request is not None:
        trimmed_items["request"] = trimmed_request

    provider_request_payload = scrape.get("providerRequest")
    trimmed_provider_request = _trim_request_snapshot(provider_request_payload, request_max_chars)

    if raw_preview:
        trimmed_items["rawPreview"] = raw_preview

    trimmed = {k: v for k, v in scrape.items() if k not in {"items", "request", "providerRequest"}}
    trimmed["items"] = trimmed_items
    if trimmed_request is not None:
        trimmed["request"] = trimmed_request
    if trimmed_provider_request is not None:
        trimmed["providerRequest"] = trimmed_provider_request
    return trimmed


@activity.defn
async def store_scrape(scrape: Dict[str, Any]) -> str:
    from ..services.convex_client import convex_mutation

    payload = trim_scrape_for_convex(scrape)
    now = int(time.time() * 1000)
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

    def _base_payload(data: Dict[str, Any]) -> Dict[str, Any]:
        body = {
            "sourceUrl": data["sourceUrl"],
            "startedAt": data.get("startedAt", now),
            "completedAt": data.get("completedAt", now),
            "items": data.get("items"),
            "provider": scraped_with,
            "workflowName": workflow_name,
        }
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

    try:
        scrape_id = await convex_mutation(
            "router:insertScrapeRecord",
            _base_payload(payload),
        )
    except Exception:
        # Fallback: aggressively trim and retry once so we still record the run
        fallback = trim_scrape_for_convex(
            scrape,
            max_items=100,
            max_description=400,
            raw_preview_chars=0,
        )
        if isinstance(fallback.get("items"), dict):
            fallback["items"]["truncated"] = True
        scrape_id = await convex_mutation(
            "router:insertScrapeRecord",
            _base_payload(fallback),
        )

    # Best-effort job ingestion (mimics router.ts behavior)
    try:
        jobs = _jobs_from_scrape_items(
            payload.get("items"),
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
            await convex_mutation("router:ingestJobsFromScrape", {"jobs": jobs})
    except Exception:
        # Non-fatal: ingestion failures shouldn't block scrape recording
        pass

    return str(scrape_id)


@activity.defn
async def complete_site(site_id: str) -> None:
    from ..services.convex_client import convex_mutation

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
    from ..services.convex_client import convex_mutation

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
    from ..services.convex_client import convex_mutation

    payload = {k: v for k, v in run.items() if v is not None}
    try:
        await convex_mutation("temporal:recordWorkflowRun", payload)
    except asyncio.CancelledError:
        # Shutdown/interrupt paths shouldn't surface as activity failures
        return None
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to record workflow run: {e}") from e


def _shrink_for_scratchpad(data: Any, max_len: int = 900) -> Any:
    """Keep scratchpad payloads small to fit Convex doc limits."""

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
    """Write a lightweight scratchpad entry to Convex."""

    from ..services.convex_client import convex_mutation

    payload = _with_firecrawl_suffix({k: v for k, v in entry.items() if v is not None})
    if "data" in payload:
        payload["data"] = _shrink_for_scratchpad(payload.get("data"))

    try:
        await convex_mutation("scratchpad:append", payload)
    except asyncio.CancelledError:
        return None
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to record scratchpad entry: {e}") from e
