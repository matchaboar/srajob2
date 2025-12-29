from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, TYPE_CHECKING

from firecrawl.v2.types import ScrapeOptions
from firecrawl.v2.utils.error_handler import PaymentRequiredError, RequestTimeoutError
from temporalio.exceptions import ApplicationError

from ...components.models import GreenhouseBoardResponse
from ..helpers.scrape_utils import MAX_JOB_DESCRIPTION_CHARS
from ..exceptions import (
    NonRetryableWorkflowError,
    PaymentRequiredWorkflowError,
    RateLimitWorkflowError,
    TimeoutWorkflowError,
)
from ...services import telemetry
from .base import BaseScraper

if TYPE_CHECKING:
    from ..activities import Site


@dataclass
class FirecrawlDependencies:
    start_firecrawl_webhook_scrape: Callable[[Site], Awaitable[Dict[str, Any]]]
    build_request_snapshot: Callable[..., Dict[str, Any]]
    settings: Any
    firecrawl_cls: Any
    build_firecrawl_schema: Callable[[], Dict[str, Any]]
    log_provider_dispatch: Callable[..., None]
    log_sync_response: Callable[..., None]
    trim_scrape_for_convex: Callable[[Dict[str, Any]], Dict[str, Any]]
    normalize_firecrawl_items: Callable[[Any], List[Dict[str, Any]]]
    log_scrape_error: Callable[[Dict[str, Any]], Awaitable[None]]
    load_greenhouse_board: Callable[[Any], GreenhouseBoardResponse]
    extract_greenhouse_job_urls: Callable[[GreenhouseBoardResponse], List[str]]
    firecrawl_cache_max_age_ms: int


class FirecrawlScraper(BaseScraper):
    provider = "firecrawl"

    def __init__(self, deps: FirecrawlDependencies):
        self.deps = deps

    async def scrape_site(
        self,
        site: Site,
        *,
        skip_urls: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        firecrawl_api_key = self.deps.settings.firecrawl_api_key
        if not firecrawl_api_key:
            raise ApplicationError(
                "FIRECRAWL_API_KEY env var is required for Firecrawl", non_retryable=True
            )

        job_info = await self.deps.start_firecrawl_webhook_scrape(site)
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
        request_snapshot = job_info.get("request") or self.deps.build_request_snapshot(
            {
                "urls": [site.get("url")],
                "pattern": site.get("pattern"),
                "siteType": site.get("type") or "general",
                "skipUrls": skip_urls or [],
            },
            provider=self.provider,
            method="POST",
            url="https://api.firecrawl.dev/v2/batch/scrape",
        )
        provider_request = job_info.get("providerRequest") or request_snapshot

        return {
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
                "provider": self.provider,
                "queued": True,
                "jobId": job_info.get("jobId"),
                "statusUrl": job_info.get("statusUrl"),
                "webhookId": job_info.get("webhookId"),
                "receivedAt": job_info.get("receivedAt"),
                "request": request_snapshot,
                "rawStart": job_info.get("rawStart") or job_info,
            },
            "provider": self.provider,
            "workflowName": "ScraperFirecrawlQueued",
            "asyncState": async_state,
            "asyncResponse": async_response,
            "providerRequest": provider_request,
        }

    async def fetch_greenhouse_listing(self, site: Site) -> Dict[str, Any]:  # type: ignore[override]
        firecrawl_api_key = self.deps.settings.firecrawl_api_key
        if not firecrawl_api_key:
            raise ApplicationError(
                "FIRECRAWL_API_KEY env var is required for Firecrawl",
                non_retryable=True,
            )

        raw_html_format = "rawHtml"

        def _do_scrape() -> Any:
            client = self.deps.firecrawl_cls(api_key=firecrawl_api_key)
            return client.batch_scrape(
                [site["url"]],
                formats=[raw_html_format],
                proxy="auto",
                max_age=self.deps.firecrawl_cache_max_age_ms,
                store_in_cache=True,
                ignore_invalid_urls=True,
            )

        self.deps.log_provider_dispatch(
            self.provider, site.get("url") or "", kind="greenhouse_board", siteId=site.get("_id")
        )

        started_at = int(time.time() * 1000)
        try:
            job = await asyncio.to_thread(_do_scrape)
        except RequestTimeoutError as exc:
            site_url = site.get("url") or ""
            payload = {
                "event": "scrape.greenhouse_listing.fetch_failed",
                "level": "error",
                "siteUrl": site_url,
                "data": {
                    "provider": self.provider,
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
                        "event": "scrape.greenhouse_listing.fetch_failed",
                        "siteUrl": site_url,
                        "siteId": site.get("_id"),
                        "provider": self.provider,
                    },
                )
            except Exception:
                pass
            raise ApplicationError(
                f"Firecrawl scrape timed out for {site.get('url')}: {exc}", non_retryable=True
            ) from exc
        except ValueError as exc:
            site_url = site.get("url") or ""
            payload = {
                "event": "scrape.greenhouse_listing.fetch_failed",
                "level": "error",
                "siteUrl": site_url,
                "data": {
                    "provider": self.provider,
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
                        "event": "scrape.greenhouse_listing.fetch_failed",
                        "siteUrl": site_url,
                        "siteId": site.get("_id"),
                        "provider": self.provider,
                    },
                )
            except Exception:
                pass
            raise ApplicationError(
                f"Firecrawl scrape failed (invalid json format configuration): {exc}", non_retryable=True
            ) from exc
        except Exception as exc:  # noqa: BLE001
            site_url = site.get("url") or ""
            payload = {
                "event": "scrape.greenhouse_listing.fetch_failed",
                "level": "error",
                "siteUrl": site_url,
                "data": {
                    "provider": self.provider,
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
                        "event": "scrape.greenhouse_listing.fetch_failed",
                        "siteUrl": site_url,
                        "siteId": site.get("_id"),
                        "provider": self.provider,
                    },
                )
            except Exception:
                pass
            raise ApplicationError(f"Firecrawl scrape failed: {exc}") from exc

        docs = getattr(job, "data", None) if hasattr(job, "data") else None
        if docs is None and isinstance(job, dict):
            docs = job.get("data")
        first_doc = docs[0] if isinstance(docs, list) and docs else None

        raw_json = getattr(first_doc, "json", None) if first_doc is not None else None
        if raw_json is None and isinstance(first_doc, dict):
            raw_json = first_doc.get("json")
        raw_text = None

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
            board: GreenhouseBoardResponse = self.deps.load_greenhouse_board(raw_text or raw_json or first_doc or {})
            job_urls = self.deps.extract_greenhouse_job_urls(board)
        except Exception as exc:  # noqa: BLE001
            site_url = site.get("url") or ""
            payload = {
                "event": "scrape.greenhouse_listing.parse_failed",
                "level": "error",
                "siteUrl": site_url,
                "data": {
                    "provider": self.provider,
                    "siteId": site.get("_id"),
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
                        "event": "scrape.greenhouse_listing.parse_failed",
                        "siteUrl": site_url,
                        "siteId": site.get("_id"),
                        "provider": self.provider,
                    },
                )
            except Exception:
                pass
            raise ApplicationError(f"Unable to parse Greenhouse board payload (Firecrawl): {exc}") from exc

        self.deps.log_sync_response(
            self.provider,
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

    async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        urls: List[str] = [u for u in payload.get("urls", []) if isinstance(u, str) and u.strip()]
        source_url: str = payload.get("source_url") or (urls[0] if urls else "")
        idempotency_key: Optional[str] = payload.get("idempotency_key") or payload.get("webhook_id")

        if not urls:
            return {"scrape": None, "jobsScraped": 0}

        firecrawl_api_key = self.deps.settings.firecrawl_api_key
        if not firecrawl_api_key:
            raise ApplicationError(
                "FIRECRAWL_API_KEY env var is required for Firecrawl",
                non_retryable=True,
            )

        schema = self.deps.build_firecrawl_schema()
        scrape_options = ScrapeOptions(
            formats=[
                "markdown",
                {"type": "json", "schema": schema},
            ]
        )

        def _scrape_batch() -> Any:
            client = self.deps.firecrawl_cls(api_key=firecrawl_api_key)
            formats: list[Any] = list(scrape_options.formats or [])
            return client.batch_scrape(
                urls,
                formats=formats,
                proxy="auto",
                max_age=self.deps.firecrawl_cache_max_age_ms,
                store_in_cache=True,
                max_concurrency=5,
                idempotency_key=idempotency_key,
            )

        self.deps.log_provider_dispatch(
            self.provider,
            source_url,
            kind="greenhouse_jobs",
            urls=len(urls),
            idempotency=idempotency_key,
        )

        try:
            result = await asyncio.to_thread(_scrape_batch)
        except Exception as exc:  # noqa: BLE001
            error_payload: Dict[str, Any] = {
                "sourceUrl": source_url,
                "event": "batch_scrape",
                "status": "error",
                "error": str(exc),
                "metadata": {"urls": urls},
            }
            if idempotency_key is not None:
                error_payload["jobId"] = idempotency_key
            await self.deps.log_scrape_error(error_payload)
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
                candidate = raw_payload[key]
                if candidate.strip():
                    batch_id = candidate.strip()
                    break
        if batch_id is None and isinstance(idempotency_key, str):
            batch_id = idempotency_key

        normalized_items = self.deps.normalize_firecrawl_items(raw_payload)
        completed_at = int(time.time() * 1000)

        request_payload = {
            "urls": urls,
            "options": {
                "formats": scrape_options.formats,
                "proxy": "auto",
                "max_age": self.deps.firecrawl_cache_max_age_ms,
                "store_in_cache": True,
            },
            "idempotencyKey": idempotency_key,
            "sourceUrl": source_url,
            "kind": "greenhouse_listing",
        }
        if batch_id:
            request_payload["batchId"] = batch_id
        request_snapshot = self.deps.build_request_snapshot(
            request_payload,
            provider=self.provider,
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
                "provider": self.provider,
                "request": request_snapshot,
            },
        }

        trimmed = self.deps.trim_scrape_for_convex(
            scrape_payload,
            max_description=MAX_JOB_DESCRIPTION_CHARS,
        )
        items = trimmed.get("items", {})
        if isinstance(items, dict):
            items.setdefault("seedUrls", urls)
            trimmed["items"] = items

        self.deps.log_sync_response(
            self.provider,
            action="greenhouse_jobs",
            url=source_url,
            kind="greenhouse_jobs",
            summary=f"items={len(normalized_items)}",
            metadata={"urls": len(urls), "job_id": batch_id or idempotency_key},
        )

        return {"scrape": trimmed, "jobsScraped": len(normalized_items)}
