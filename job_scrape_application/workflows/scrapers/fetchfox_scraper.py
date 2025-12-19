from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, TYPE_CHECKING

from fetchfox_sdk import FetchFox
from temporalio.exceptions import ApplicationError

from ...components.models import FetchFoxPriority, FetchFoxScrapeRequest, GreenhouseBoardResponse, MAX_FETCHFOX_VISITS
from ..helpers.scrape_utils import MAX_JOB_DESCRIPTION_CHARS, _shrink_payload
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
            raise ApplicationError(f"Failed to fetch Greenhouse board: {exc}") from exc

        raw_text = self.deps.extract_raw_body_from_fetchfox_result(result_obj)

        try:
            board: GreenhouseBoardResponse = self.deps.load_greenhouse_board(raw_text or result_obj)
            job_urls = self.deps.extract_greenhouse_job_urls(board)
        except Exception as exc:  # noqa: BLE001
            raise ApplicationError(f"Unable to parse Greenhouse board payload: {exc}") from exc

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
