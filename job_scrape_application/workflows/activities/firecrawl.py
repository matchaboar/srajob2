from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from temporalio.exceptions import ApplicationError

from ..helpers.firecrawl import metadata_urls_to_list, should_mock_convex_webhooks
from ..helpers.provider import build_provider_status_url
from .constants import CONVEX_MUTATION_TIMEOUT_SECONDS, FirecrawlJobKind

if TYPE_CHECKING:
    from . import Site

logger = logging.getLogger("temporal.worker.activities")


class WebhookModel:
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


async def start_firecrawl_batch(
    start_fn: Callable[[Any], Any],
    webhook_model: Any,
    webhook_payload: Dict[str, Any],
) -> Any:
    """Invoke a Firecrawl start function, retrying once if webhook lacks model_dump."""

    logger.info(
        "firecrawl.start_batch entering webhook_keys=%s metadata_keys=%s",
        list(webhook_payload.keys()),
        list((webhook_payload.get("metadata") or {}).keys()),
    )
    try:
        return await asyncio.to_thread(start_fn, webhook_model)
    except AttributeError as exc:
        if "model_dump" not in str(exc):
            raise

        retry_webhook = WebhookModel(webhook_payload)
        logger.warning("Retrying Firecrawl start with wrapped webhook after model_dump error")
        return await asyncio.to_thread(start_fn, retry_webhook)


def serialize_firecrawl_job(
    job: Any, site: "Site", webhook: Dict[str, Any], kind: FirecrawlJobKind
) -> Dict[str, Any]:
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
    status_url_clean = build_provider_status_url(
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


async def record_pending_firecrawl_webhook(
    job: Dict[str, Any], site: "Site", webhook: Dict[str, Any], kind: FirecrawlJobKind
) -> Optional[str]:
    """Insert a placeholder webhook row so missing callbacks can be recovered later."""

    if should_mock_convex_webhooks():
        return f"mock-webhook-{int(time.time() * 1000)}"

    from ...services.convex_client import get_client

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


def mock_firecrawl_status_response(
    *,
    event: Dict[str, Any],
    job_id: str,
    kind: FirecrawlJobKind | str,
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

    if kind == FirecrawlJobKind.GREENHOUSE_LISTING:
        return {
            "kind": FirecrawlJobKind.GREENHOUSE_LISTING,
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
    seed_list = metadata_urls_to_list(seed_urls)
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
