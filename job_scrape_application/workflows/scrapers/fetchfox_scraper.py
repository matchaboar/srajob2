from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, TYPE_CHECKING

from fetchfox_sdk import FetchFox
from temporalio.exceptions import ApplicationError

from ...components.models import FetchFoxPriority, FetchFoxScrapeRequest, GreenhouseBoardResponse, MAX_FETCHFOX_VISITS
from ..helpers.scrape_utils import MAX_JOB_DESCRIPTION_CHARS, _shrink_payload, parse_posted_at
from ..helpers.link_extractors import normalize_url
from ..helpers.regex_patterns import GREENHOUSE_BOARDS_PATH_PATTERN
from ...services import telemetry
from .base import BaseScraper

if TYPE_CHECKING:
    from ..activities import Site


@dataclass
class FetchfoxDependencies:
    fetch_seen_urls_for_site: Callable[[str, Optional[str]], Awaitable[List[str]]]
    build_job_template: Callable[[], Dict[str, str]]
    build_request_snapshot: Callable[..., Dict[str, Any]]
    log_provider_dispatch: Callable[..., None]
    log_sync_response: Callable[..., None]
    normalize_fetchfox_items: Callable[[Any], List[Dict[str, Any]]]
    trim_scrape_for_convex: Callable[[Dict[str, Any]], Dict[str, Any]]
    settings: Any
    load_greenhouse_board: Callable[[Any], GreenhouseBoardResponse]
    extract_greenhouse_job_urls: Callable[[GreenhouseBoardResponse], List[str]]
    extract_raw_body_from_fetchfox_result: Callable[[Any], str]


class FetchfoxScraper(BaseScraper):
    provider = "fetchfox"

    def __init__(self, deps: FetchfoxDependencies):
        self.deps = deps

    async def scrape_site(
        self,
        site: Site,
        *,
        skip_urls: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        settings = self.deps.settings
        if not settings.fetchfox_api_key:
            raise ApplicationError(
                "FETCHFOX_API_KEY env var is required for FetchFox", non_retryable=True
            )

        pattern = site.get("pattern")
        skip_urls = (
            skip_urls
            if skip_urls is not None
            else await self.deps.fetch_seen_urls_for_site(site["url"], pattern)
        )
        start_urls = [site["url"]]
        template = self.deps.build_job_template()

        request = FetchFoxScrapeRequest(
            pattern=pattern,
            start_urls=start_urls,
            max_depth=5,
            max_visits=MAX_FETCHFOX_VISITS,
            template=template,
            priority=FetchFoxPriority(skip=skip_urls),
            content_transform="text_only",
        ).model_dump(exclude_none=True)
        request_snapshot = self.deps.build_request_snapshot(
            request,
            provider=self.provider,
            method="POST",
            url="https://api.fetchfox.ai/scrape",
        )

        def _do_scrape(scrape_payload: Dict[str, Any]):
            fox = FetchFox(api_key=settings.fetchfox_api_key)
            return fox.scrape(scrape_payload)

        self.deps.log_provider_dispatch(
            self.provider,
            site.get("url") or "",
            pattern=pattern,
            siteId=site.get("_id"),
        )

        started_at = int(time.time() * 1000)

        try:
            result = await asyncio.to_thread(_do_scrape, request)
            result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
        except Exception:
            result_obj = {"raw": "Scrape failed or returned invalid data"}

        normalized_items = self.deps.normalize_fetchfox_items(result_obj)
        raw_urls: List[str] = []
        if isinstance(result_obj, dict):
            urls_field = result_obj.get("urls")
            if isinstance(urls_field, list):
                raw_urls = [u for u in urls_field if isinstance(u, str)]

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
            "provider": self.provider,
            "costMilliCents": None,
        }

        self.deps.log_sync_response(
            self.provider,
            action="scrape",
            url=site.get("url"),
            kind="site_crawl",
            summary=f"items={len(normalized_items)} urls={len(raw_urls)}",
            metadata={"siteId": site.get("_id"), "pattern": pattern, "seed": len(start_urls)},
            response=_shrink_payload(result_obj, 20000),
        )

        return self.deps.trim_scrape_for_convex(
            scrape_payload,
            max_description=MAX_JOB_DESCRIPTION_CHARS,
        )

    async def fetch_greenhouse_listing(self, site: Site) -> Dict[str, Any]:  # type: ignore[override]
        settings = self.deps.settings
        if not settings.fetchfox_api_key:
            raise ApplicationError(
                "FETCHFOX_API_KEY env var is required for FetchFox",
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

        self.deps.log_provider_dispatch(
            self.provider, site.get("url") or "", kind="greenhouse_board", siteId=site.get("_id")
        )

        started_at = int(time.time() * 1000)

        try:
            result = await asyncio.to_thread(_do_scrape, request)
            result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
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
            raise ApplicationError(f"Failed to fetch Greenhouse board: {exc}") from exc

        raw_text = self.deps.extract_raw_body_from_fetchfox_result(result_obj)

        try:
            board: GreenhouseBoardResponse = self.deps.load_greenhouse_board(raw_text or result_obj)
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
            raise ApplicationError(f"Unable to parse Greenhouse board payload: {exc}") from exc

        slug = ""
        site_url = site.get("url") or ""
        if site_url:
            match = re.search(GREENHOUSE_BOARDS_PATH_PATTERN, site_url)
            if match:
                slug = match.group(1)

        posted_at_by_url: Dict[str, int] = {}

        def _add_posted_at(candidate_url: str | None, posted_at: int) -> None:
            if not candidate_url:
                return
            normalized = normalize_url(candidate_url) or candidate_url
            posted_at_by_url[normalized] = posted_at

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
                _add_posted_at(getattr(job, "absolute_url", None), posted_at)
                job_id = getattr(job, "id", None)
                if slug and job_id is not None:
                    api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}"
                    _add_posted_at(api_url, posted_at)

        completed_at = int(time.time() * 1000)

        self.deps.log_sync_response(
            self.provider,
            action="greenhouse_board",
            url=site.get("url"),
            kind="greenhouse_listing",
            summary=f"job_urls={len(job_urls)}",
            metadata={"siteId": site.get("_id"), "raw_len": len(raw_text or "") if isinstance(raw_text, str) else None},
        )

        return {
            "raw": raw_text,
            "job_urls": job_urls,
            "posted_at_by_url": posted_at_by_url if posted_at_by_url else None,
            "startedAt": started_at,
            "completedAt": completed_at,
        }

    async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        settings = self.deps.settings
        if not settings.fetchfox_api_key:
            raise ApplicationError(
                "FETCHFOX_API_KEY env var is required for FetchFox",
                non_retryable=True,
            )

        urls: List[str] = [u for u in payload.get("urls", []) if isinstance(u, str) and u.strip()]
        source_url: str = payload.get("source_url") or (urls[0] if urls else "")
        posted_at_by_url: Dict[str, int] = {}
        raw_posted = payload.get("posted_at_by_url")
        if isinstance(raw_posted, dict):
            for key, value in raw_posted.items():
                if not isinstance(key, str):
                    continue
                if not isinstance(value, (int, float)):
                    continue
                normalized_key = normalize_url(key) or key
                posted_at_by_url[normalized_key] = int(value)

        if not urls:
            return {"scrape": None, "jobsScraped": 0}

        template = self.deps.build_job_template()
        request = FetchFoxScrapeRequest(
            pattern=None,
            start_urls=urls,
            max_depth=1,
            max_visits=min(MAX_FETCHFOX_VISITS, max(len(urls), 1)),
            template=template,
            priority=FetchFoxPriority(skip=[]),
            content_transform="text_only",
        ).model_dump(exclude_none=True)
        request_snapshot = self.deps.build_request_snapshot(
            request,
            provider=self.provider,
            method="POST",
            url="https://api.fetchfox.ai/scrape",
        )

        def _do_scrape(scrape_payload: Dict[str, Any]):
            fox = FetchFox(api_key=settings.fetchfox_api_key)
            return fox.scrape(scrape_payload)

        self.deps.log_provider_dispatch(self.provider, source_url, kind="greenhouse_jobs", urls=len(urls))

        started_at = int(time.time() * 1000)

        try:
            result = await asyncio.to_thread(_do_scrape, request)
            result_obj: Dict[str, Any] = result if isinstance(result, dict) else json.loads(result)
        except Exception as exc:  # noqa: BLE001
            raise ApplicationError(f"Greenhouse detail scrape failed: {exc}") from exc

        normalized_items = self.deps.normalize_fetchfox_items(result_obj)
        if posted_at_by_url and isinstance(normalized_items, list):
            for row in normalized_items:
                if not isinstance(row, dict):
                    continue
                url_val = row.get("url") or row.get("job_url") or row.get("absolute_url")
                if not isinstance(url_val, str) or not url_val.strip():
                    continue
                normalized_key = normalize_url(url_val) or url_val
                override = posted_at_by_url.get(normalized_key)
                if override is not None:
                    row["posted_at"] = int(override)
                    row["posted_at_unknown"] = False
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
            metadata={"urls": len(urls), "siteId": payload.get("site_id") or payload.get("siteId")},
        )

        return {"scrape": trimmed, "jobsScraped": len(normalized_items)}
