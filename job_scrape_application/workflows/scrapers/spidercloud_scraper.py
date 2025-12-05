from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

import httpx
from spider import AsyncSpider
from temporalio.exceptions import ApplicationError

from ...components.models import extract_greenhouse_job_urls, load_greenhouse_board
from ...constants import title_matches_required_keywords
from ...config import runtime_config
from ..helpers.scrape_utils import (
    UNKNOWN_COMPENSATION_REASON,
    coerce_level,
    coerce_remote,
    derive_company_from_url,
    strip_known_nav_blocks,
)
from .base import BaseScraper

if TYPE_CHECKING:
    from ..activities import Site

SPIDERCLOUD_BATCH_SIZE = 50


logger = logging.getLogger("temporal.worker.activities")


@dataclass
class SpidercloudDependencies:
    mask_secret: Callable[[Optional[str]], Optional[str]]
    sanitize_headers: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]]
    build_request_snapshot: Callable[..., Dict[str, Any]]
    log_dispatch: Callable[..., None]
    log_sync_response: Callable[..., None]
    trim_scrape_for_convex: Callable[[Dict[str, Any]], Dict[str, Any]]
    settings: Any
    fetch_seen_urls_for_site: Callable[[str, Optional[str]], Awaitable[List[str]]]


class SpiderCloudScraper(BaseScraper):
    provider = "spidercloud"

    def __init__(self, deps: SpidercloudDependencies):
        self.deps = deps

    def supports_greenhouse(self) -> bool:  # type: ignore[override]
        return True

    def _api_key(self) -> str:
        configured_key = self.deps.settings.spider_api_key
        env_key = os.getenv("SPIDER_API_KEY")
        alt_env_key = os.getenv("SPIDER_KEY")
        key = configured_key or env_key or alt_env_key
        source = (
            "settings.spider_api_key"
            if configured_key
            else "SPIDER_API_KEY"
            if env_key
            else "SPIDER_KEY"
            if alt_env_key
            else None
        )
        if not key:
            raise ApplicationError(
                "SPIDER_API_KEY env var is required for SpiderCloud", non_retryable=True
            )
        logger.debug(
            "SpiderCloud API key resolved from %s value=%s",
            source or "unknown",
            self.deps.mask_secret(key),
        )
        return key

    def _try_parse_json(self, raw: str) -> Any | None:
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _extract_markdown(self, obj: Any) -> Optional[str]:
        """Return the first markdown/text-like payload found in a response fragment."""

        keys = {"markdown", "commonmark", "content", "text", "body", "result"}

        def _walk(value: Any) -> Optional[str]:
            if isinstance(value, str):
                if value.strip():
                    return value
                return None
            if isinstance(value, dict):
                for key, val in value.items():
                    if key.lower() in keys and isinstance(val, str) and val.strip():
                        return val
                    found = _walk(val)
                    if found:
                        return found
            if isinstance(value, list):
                for item in value:
                    found = _walk(item)
                    if found:
                        return found
            return None

        return _walk(obj)

    def _extract_credits(self, obj: Any) -> Optional[float]:
        """Heuristically pull a credit usage number from a payload."""

        matches: List[float] = []

        def _walk(value: Any) -> None:
            if isinstance(value, dict):
                for key, val in value.items():
                    if isinstance(val, (int, float)) and "credit" in key.lower():
                        matches.append(float(val))
                    else:
                        _walk(val)
            elif isinstance(value, list):
                for item in value:
                    _walk(item)

        _walk(obj)
        if not matches:
            return None
        return max(matches)

    def _extract_cost_usd(self, obj: Any) -> Optional[float]:
        """Pull a dollar-denominated cost (e.g., total_cost) from a payload."""

        costs: List[float] = []

        def _walk(value: Any) -> None:
            if isinstance(value, dict):
                for key, val in value.items():
                    if isinstance(val, (int, float)) and "cost" in key.lower():
                        costs.append(float(val))
                    _walk(val)
            elif isinstance(value, list):
                for item in value:
                    _walk(item)

        _walk(obj)
        if not costs:
            return None
        return max(costs)

    def _title_from_events(self, events: List[Any]) -> Optional[str]:
        for evt in events:
            if not isinstance(evt, dict):
                continue
            for key in ("title", "job_title", "heading"):
                val = evt.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
        return None

    def _title_from_markdown(self, markdown: str) -> Optional[str]:
        for line in markdown.splitlines():
            if not line.strip():
                continue
            heading_match = re.match(r"^#{1,6}\s*(.+)$", line.strip())
            if heading_match:
                return heading_match.group(1).strip()
            if len(line.strip()) > 6:
                return line.strip()
        return None

    def _title_from_url(self, url: str) -> str:
        slug = url.split("/")[-1] if "/" in url else url
        slug = slug or url
        cleaned = re.sub(r"[-_]+", " ", slug).strip()
        cleaned = re.sub(r"\?.*$", "", cleaned)
        if not cleaned:
            return "Untitled"
        return cleaned.title()

    def _is_placeholder_title(self, title: str) -> bool:
        placeholders = {"page_title", "title", "job_title", "untitled", "unknown"}
        return title.strip().lower() in placeholders

    def _regex_extract_job_urls(self, text: str) -> List[str]:
        """
        Fallback extraction for Greenhouse listings when structured parsing fails.

        Looks for greenhouse job URLs and returns a deduped list.
        """

        if not text:
            return []
        # Capture both boards.greenhouse.io and api.greenhouse.io absolute URLs
        pattern = re.compile(r"https?://[\w.-]*greenhouse\.io/[^\s\"'>]+")
        seen: set[str] = set()
        urls: list[str] = []
        for match in pattern.findall(text):
            if "jobs" not in match:
                continue
            url = match.strip()
            if url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def _normalize_job(
        self, url: str, markdown: str, events: List[Any], started_at: int
    ) -> Dict[str, Any] | None:
        payload_title = self._title_from_events(events)
        from_content = False
        if payload_title and self._is_placeholder_title(payload_title):
            payload_title = None
        if not payload_title:
            payload_title = self._title_from_markdown(markdown)
            from_content = bool(payload_title)
        else:
            from_content = True

        title = payload_title or self._title_from_url(url)
        cleaned_markdown = strip_known_nav_blocks(markdown or "")
        if from_content and not title_matches_required_keywords(title):
            logger.info(
                "SpiderCloud dropping job due to missing required keyword url=%s title=%s",
                url,
                title,
            )
            self._last_ignored_job = {
                "url": url,
                "reason": "missing_required_keyword",
                "title": title,
                "description": cleaned_markdown,
            }
            return None
        company = derive_company_from_url(url) or "Unknown"
        remote = coerce_remote(None, "", f"{title}\n{cleaned_markdown}")
        level = coerce_level(None, title)
        description = cleaned_markdown or ""

        self._last_ignored_job = None
        return {
            "job_title": title,
            "title": title,
            "company": company,
            "location": "Unknown" if not remote else "Remote",
            "remote": remote,
            "level": level,
            "description": description,
            "job_description": description,
            "total_compensation": 0,
            "compensation_unknown": True,
            "compensation_reason": UNKNOWN_COMPENSATION_REASON,
            "url": url,
            "posted_at": started_at,
        }

    def _consume_chunk(self, chunk: Any, buffer: str) -> Tuple[str, List[Any]]:
        events: List[Any] = []
        text: str | None = None
        if isinstance(chunk, (bytes, bytearray)):
            text = chunk.decode("utf-8", errors="replace")
        elif isinstance(chunk, str):
            text = chunk
        elif chunk is not None:
            events.append(chunk)
            return buffer, events

        if text is not None:
            buffer += text
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                parsed = self._try_parse_json(line)
                events.append(parsed if parsed is not None else line)

        if events:
            logger.debug(
                "SpiderCloud chunk parsed events=%s buffer_len=%s sample_type=%s",
                len(events),
                len(buffer),
                type(events[0]).__name__,
            )
        return buffer, events

    async def _scrape_single_url(
        self,
        client: AsyncSpider,
        url: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        buffer = ""
        raw_events: List[Any] = []
        markdown_parts: List[str] = []
        credit_candidates: List[float] = []
        cost_candidates_usd: List[float] = []
        started_at = int(time.time() * 1000)
        logger.info(
            "SpiderCloud scrape started url=%s params=%s", url, params
        )

        try:
            async for chunk in client.crawl_url(
                url,
                params=params,
                stream=True,
                content_type="application/jsonl",
            ):
                buffer, events = self._consume_chunk(chunk, buffer)
                for evt in events:
                    raw_events.append(evt)
                    if isinstance(evt, dict):
                        credit_value = self._extract_credits(evt)
                        cost_value = self._extract_cost_usd(evt)
                        if credit_value is not None:
                            credit_candidates.append(credit_value)
                        if cost_value is not None:
                            cost_candidates_usd.append(cost_value)
                        text = self._extract_markdown(evt)
                        if text:
                            markdown_parts.append(text)

            tail = buffer.strip()
            if tail:
                parsed = self._try_parse_json(tail)
                raw_events.append(parsed if parsed is not None else tail)
                if isinstance(parsed, dict):
                    credit_value = self._extract_credits(parsed)
                    cost_value = self._extract_cost_usd(parsed)
                    if credit_value is not None:
                        credit_candidates.append(credit_value)
                    if cost_value is not None:
                        cost_candidates_usd.append(cost_value)
                    text = self._extract_markdown(parsed)
                    if text:
                        markdown_parts.append(text)
                elif isinstance(parsed, str):
                    markdown_parts.append(parsed)

            if not markdown_parts:
                logger.info("SpiderCloud stream empty; falling back to non-stream fetch url=%s", url)
                async for resp in client.crawl_url(
                    url,
                    params=params,
                    stream=False,
                    content_type="application/json",
                ):
                    raw_events.append(resp)
                    if isinstance(resp, dict):
                        credit_value = self._extract_credits(resp)
                        cost_value = self._extract_cost_usd(resp)
                        if credit_value is not None:
                            credit_candidates.append(credit_value)
                        if cost_value is not None:
                            cost_candidates_usd.append(cost_value)
                        text = self._extract_markdown(resp)
                        if text:
                            markdown_parts.append(text)
        except Exception as exc:  # noqa: BLE001
            logger.error("SpiderCloud scrape failed url=%s error=%s", url, exc)
            raise ApplicationError(f"SpiderCloud scrape failed for {url}: {exc}") from exc

        logger.info(
            "SpiderCloud stream finished url=%s events=%s markdown_fragments=%s credit_candidates=%s",
            url,
            len(raw_events),
            len(markdown_parts),
            len(credit_candidates),
        )

        markdown_text = "\n\n".join(
            [part for part in markdown_parts if isinstance(part, str) and part.strip()]
        ).strip()
        credits_used = max(credit_candidates) if credit_candidates else None
        cost_milli_cents = (
            int(max(cost_candidates_usd) * 100000) if cost_candidates_usd else None
        )
        if cost_milli_cents is None and credits_used is not None:
            cost_milli_cents = int(float(credits_used) * 10)
        cost_usd = (cost_milli_cents / 100000) if isinstance(cost_milli_cents, (int, float)) else None
        normalized = self._normalize_job(url, markdown_text, raw_events, started_at)
        ignored_entry = getattr(self, "_last_ignored_job", None)

        logger.info(
            "SpiderCloud stream finished url=%s events=%s markdown_fragments=%s credits=%s cost_mc=%s cost_usd=%s",
            url,
            len(raw_events),
            len(markdown_parts),
            credits_used,
            cost_milli_cents,
            cost_usd,
        )

        return {
            "normalized": normalized,
            "raw": {
                "url": url,
                "events": raw_events,
                "markdown": markdown_text,
                "creditsUsed": credits_used,
            },
            "creditsUsed": credits_used,
            "costMilliCents": cost_milli_cents,
            "startedAt": started_at,
            "ignored": ignored_entry,
        }
        logger.info(
            "SpiderCloud normalized url=%s title=%s credits=%s description_len=%s",
            url,
            normalized.get("title"),
            credits_used,
            len(markdown_text),
        )

    async def _scrape_urls_batch(
        self,
        urls: List[str],
        *,
        source_url: str,
        pattern: Optional[str] = None,
    ) -> Dict[str, Any]:
        urls = urls[:SPIDERCLOUD_BATCH_SIZE]
        logger.info(
            "SpiderCloud batch start urls=%s pattern=%s", len(urls), pattern
        )
        if not urls:
            logger.info("SpiderCloud batch empty; returning no-op payload")
            return {
                "sourceUrl": source_url,
                "pattern": pattern,
                "startedAt": int(time.time() * 1000),
                "completedAt": int(time.time() * 1000),
                "items": {"normalized": [], "raw": [], "provider": self.provider, "seedUrls": urls},
                "provider": self.provider,
            }

        api_key = self._api_key()
        params: Dict[str, Any] = {
            "return_format": ["commonmark"],
            "metadata": True,
            "request": "smart",
            "limit": 1,
        }
        started_at = int(time.time() * 1000)
        normalized_items: List[Dict[str, Any]] = []
        raw_items: List[Dict[str, Any]] = []
        ignored_items: List[Dict[str, Any]] = []
        total_cost_milli_cents = 0.0
        saw_cost_field = False

        async with AsyncSpider(api_key=api_key) as client:
            for url in urls:
                result = await self._scrape_single_url(client, url, params)
                if result.get("normalized"):
                    normalized_items.append(result["normalized"])
                if result.get("ignored"):
                    ignored_items.append(result["ignored"])
                if result.get("raw"):
                    raw_items.append(result["raw"])
                cost_mc = result.get("costMilliCents")
                credits = result.get("creditsUsed")
                if isinstance(cost_mc, (int, float)):
                    total_cost_milli_cents += float(cost_mc)
                    saw_cost_field = True
                elif isinstance(credits, (int, float)):
                    total_cost_milli_cents += float(credits) * 10
                logger.info(
                    "SpiderCloud batch item url=%s normalized=%s credits=%s cost_mc=%s markdown_len=%s",
                    url,
                    bool(result.get("normalized")),
                    credits,
                    cost_mc,
                    len(result.get("raw", {}).get("markdown") or ""),
                )

        cost_milli_cents: int | None = None
        if saw_cost_field or total_cost_milli_cents > 0:
            cost_milli_cents = int(total_cost_milli_cents)
        provider_request: Dict[str, Any] = {
            "urls": urls,
            "params": params,
            "contentType": "application/jsonl",
        }
        request_snapshot = self.deps.build_request_snapshot(
            provider_request,
            provider=self.provider,
            method="POST",
            url="https://api.spider.cloud/v1/crawl",
            headers={
                "authorization": f"Bearer {self.deps.mask_secret(api_key)}",
            },
        )

        completed_at = int(time.time() * 1000)
        items_block: Dict[str, Any] = {
            "normalized": normalized_items,
            "raw": raw_items,
            "provider": self.provider,
            "seedUrls": urls,
            "request": request_snapshot,
        }
        if ignored_items:
            items_block["ignored"] = ignored_items
        if cost_milli_cents is not None:
            items_block["costMilliCents"] = cost_milli_cents

        scrape_payload: Dict[str, Any] = {
            "sourceUrl": source_url,
            "pattern": pattern,
            "startedAt": started_at,
            "completedAt": completed_at,
            "items": items_block,
            "provider": self.provider,
            "subUrls": urls,
            "request": request_snapshot,
            "providerRequest": provider_request,
        }
        if cost_milli_cents is not None:
            scrape_payload["costMilliCents"] = cost_milli_cents

        trimmed = self.deps.trim_scrape_for_convex(scrape_payload)
        trimmed_items = trimmed.get("items")
        if isinstance(trimmed_items, dict):
            trimmed_items.setdefault("seedUrls", urls)
            trimmed["items"] = trimmed_items

        cost_cents = (cost_milli_cents / 1000) if isinstance(cost_milli_cents, (int, float)) else None
        cost_usd = (cost_milli_cents / 100000) if isinstance(cost_milli_cents, (int, float)) else None
        cost_mc_display = str(cost_milli_cents) if cost_milli_cents is not None else "n/a"
        cost_cents_display = f"{float(cost_cents):.3f}" if cost_cents is not None else "n/a"
        cost_usd_display = f"{float(cost_usd):.5f}" if cost_usd is not None else "n/a"
        logger.info(
            "SpiderCloud batch complete source=%s items=%s cost_mc=%s cost_usd=%s",
            source_url,
            len(trimmed_items.get("normalized") if isinstance(trimmed_items, dict) else []),
            cost_milli_cents,
            cost_usd,
        )

        self.deps.log_sync_response(
            self.provider,
            action="scrape",
            url=source_url,
            summary=(
                "urls="
                f"{len(urls)} "
                f"items={len(normalized_items)} "
                f"cost_mc={cost_mc_display} "
                f"cost_cents={cost_cents_display} "
                f"cost_usd={cost_usd_display}"
            ),
            metadata={"pattern": pattern, "seed": len(urls)},
            response=trimmed,
        )

        return trimmed

    async def scrape_site(
        self,
        site: Site,
        *,
        skip_urls: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        source_url = site.get("url") or ""
        urls = [u for u in [source_url] if isinstance(u, str) and u.strip()]

        resolved_skip: Optional[List[str]] = skip_urls
        if resolved_skip is None and source_url:
            try:
                resolved_skip = await self.deps.fetch_seen_urls_for_site(source_url, site.get("pattern"))
            except Exception:
                resolved_skip = []

        skip_set = set(resolved_skip or [])
        urls = [u for u in urls if u not in skip_set]
        logger.info(
            "SpiderCloud scrape_site source=%s pattern=%s skip=%s final_urls=%s",
            source_url,
            site.get("pattern"),
            len(skip_set),
            len(urls),
        )

        self.deps.log_dispatch(
            self.provider,
            source_url,
            pattern=site.get("pattern"),
            siteId=site.get("_id"),
            skip=len(skip_urls or []),
        )
        return await self._scrape_urls_batch(
            urls,
            source_url=source_url,
            pattern=site.get("pattern"),
        )

    async def fetch_greenhouse_listing(self, site: Site) -> Dict[str, Any]:  # type: ignore[override]
        """Fetch a Greenhouse board JSON feed directly."""

        url = site.get("url") or ""
        slug = ""
        try:
            parts = url.split("/")
            # Prefer a slug that appears between /boards/{slug}/jobs so api.greenhouse.io
            # links still resolve correctly.
            match = re.search(r"/boards/([^/]+)/jobs", url)
            if match:
                slug = match.group(1)
            elif "boards" in parts:
                idx = parts.index("boards")
                if idx + 1 < len(parts):
                    slug = parts[idx + 1]
            if not slug and "greenhouse" in url and parts:
                slug = parts[-1]
        except Exception:
            slug = ""
        api_url = f"https://boards.greenhouse.io/v1/boards/{slug}/jobs" if slug else url

        logger.info(
            "SpiderCloud greenhouse listing fetch url=%s slug=%s api_url=%s",
            url,
            slug,
            api_url,
        )
        self.deps.log_dispatch(self.provider, url, kind="greenhouse_board", siteId=site.get("_id"))
        started_at = int(time.time() * 1000)
        try:
            async with httpx.AsyncClient(timeout=runtime_config.spidercloud_http_timeout_seconds) as client:
                resp = await client.get(api_url)
                resp.raise_for_status()
                raw_text = resp.text
        except Exception as exc:  # noqa: BLE001
            logger.error("SpiderCloud greenhouse listing http error url=%s error=%s", api_url, exc)
            raise ApplicationError(f"Failed to fetch Greenhouse board via SpiderCloud: {exc}") from exc

        try:
            board = load_greenhouse_board(raw_text)
            job_urls = extract_greenhouse_job_urls(board)
            if len(job_urls) <= 1:
                regex_urls = self._regex_extract_job_urls(raw_text)
                if regex_urls:
                    job_urls = regex_urls
        except Exception as exc:  # noqa: BLE001
            logger.error("SpiderCloud greenhouse listing parse error url=%s error=%s", api_url, exc)
            regex_urls = self._regex_extract_job_urls(raw_text)
            if regex_urls:
                logger.info(
                    "SpiderCloud greenhouse listing falling back to regex extraction url=%s urls=%s",
                    api_url,
                    len(regex_urls),
                )
                return {
                    "raw": raw_text,
                    "job_urls": regex_urls,
                    "startedAt": started_at,
                    "completedAt": int(time.time() * 1000),
                }
            raise ApplicationError(f"Unable to parse Greenhouse board payload (SpiderCloud): {exc}") from exc

        completed_at = int(time.time() * 1000)
        logger.info(
            "SpiderCloud greenhouse listing parsed url=%s job_urls=%s duration_ms=%s",
            url,
            len(job_urls),
            completed_at - started_at,
        )
        self.deps.log_sync_response(
            self.provider,
            action="greenhouse_board",
            url=url,
            summary=f"job_urls={len(job_urls)}",
            metadata={"siteId": site.get("_id")},
        )

        return {
            "raw": raw_text,
            "job_urls": job_urls,
            "startedAt": started_at,
            "completedAt": completed_at,
        }

    async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        urls: List[str] = [u for u in payload.get("urls", []) if isinstance(u, str) and u.strip()]
        deduped: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            deduped.append(url)
        urls = deduped

        source_url: str = payload.get("source_url") or (urls[0] if urls else "")
        if not urls:
            logger.info("SpiderCloud greenhouse_jobs received empty url list; returning noop")
            return {"scrape": None, "jobsScraped": 0}
        logger.info(
            "SpiderCloud greenhouse_jobs start urls=%s deduped=%s source=%s",
            len(payload.get("urls", [])),
            len(urls),
            source_url,
        )

        self.deps.log_dispatch(
            self.provider,
            source_url,
            kind="greenhouse_jobs",
            urls=len(urls),
        )
        scrape_payload = await self._scrape_urls_batch(
            urls,
            source_url=source_url,
            pattern=None,
        )
        items = scrape_payload.get("items") if isinstance(scrape_payload, dict) else {}
        normalized = items.get("normalized") if isinstance(items, dict) else []
        jobs_scraped = len(normalized) if isinstance(normalized, list) else 0
        logger.info(
            "SpiderCloud greenhouse_jobs complete source=%s jobs_scraped=%s",
            source_url,
            jobs_scraped,
        )

        return {"scrape": scrape_payload, "jobsScraped": jobs_scraped}
