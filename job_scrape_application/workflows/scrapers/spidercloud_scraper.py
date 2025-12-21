from __future__ import annotations

import asyncio
import json
import html
import logging
import os
import re
import time
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING
from spider import AsyncSpider
from temporalio.exceptions import ApplicationError

from ...components.models import extract_greenhouse_job_urls, load_greenhouse_board
from ...constants import title_matches_required_keywords
from ...config import runtime_config
from ..helpers.scrape_utils import (
    MAX_JOB_DESCRIPTION_CHARS,
    UNKNOWN_COMPENSATION_REASON,
    coerce_level,
    coerce_remote,
    derive_company_from_url,
    looks_like_error_landing,
    looks_like_job_listing_page,
    parse_markdown_hints,
    strip_known_nav_blocks,
)
from ..site_handlers import BaseSiteHandler, get_site_handler
from .base import BaseScraper

if TYPE_CHECKING:
    from ..activities import Site

HTML_BR_TAG_PATTERN = r"(?i)<br\\s*/?>"
HTML_PARAGRAPH_CLOSE_PATTERN = r"(?i)</p>"
HTML_SCRIPT_BLOCK_PATTERN = r"(?is)<script[^>]*>.*?</script>"
HTML_STYLE_BLOCK_PATTERN = r"(?is)<style[^>]*>.*?</style>"
HTML_TAG_PATTERN = r"<[^>]+>"
MULTI_NEWLINE_PATTERN = r"\n{3,}"
MARKDOWN_HEADING_PATTERN = r"^#{1,6}\s*(.+)$"
MARKDOWN_HEADING_PREFIX_PATTERN = r"^#{1,6}\s*"
SLUG_SEPARATOR_PATTERN = r"[-_]+"
QUERY_STRING_PATTERN = r"\?.*$"
GREENHOUSE_URL_PATTERN = r"https?://[\w.-]*greenhouse\.io/[^\s\"'>]+"
GREENHOUSE_BOARDS_PATH_PATTERN = r"/boards/([^/]+)/jobs"

SPIDERCLOUD_BATCH_SIZE = 50
CAPTCHA_RETRY_LIMIT = 2
CAPTCHA_PROXY_SEQUENCE = ("residential", "isp")
MAX_TITLE_CHARS = 500


logger = logging.getLogger("temporal.worker.activities")


class CaptchaDetectedError(Exception):
    """Raised when a SpiderCloud response looks like a captcha wall."""

    def __init__(self, marker: str, markdown: str | None = None, events: Optional[List[Any]] = None):
        super().__init__(marker)
        self.marker = marker
        self.markdown = markdown or ""
        self.events = events or []


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

    def _get_site_handler(self, url: str, site_type: str | None = None) -> BaseSiteHandler | None:
        return get_site_handler(url, site_type)

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

    def _trim_scrape_payload(self, scrape_payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return self.deps.trim_scrape_for_convex(
                scrape_payload,
                max_description=MAX_JOB_DESCRIPTION_CHARS,
            )
        except TypeError as exc:
            if "max_description" not in str(exc):
                raise
            return self.deps.trim_scrape_for_convex(scrape_payload)

    def _try_parse_json(self, raw: str) -> Any | None:
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _html_to_markdown(self, raw_html: str) -> str:
        """Convert HTML to markdown (or plain text fallback)."""

        if not raw_html:
            return ""

        # Prefer markdownify if available (no hard dependency).
        try:  # noqa: SIM105
            from markdownify import markdownify as md

            return md(raw_html, strip=["style", "script"], heading_style="ATX").strip()
        except Exception:
            pass

        # Lightweight fallback: strip tags and preserve basic breaks.
        text = re.sub(HTML_BR_TAG_PATTERN, "\n", raw_html)
        text = re.sub(HTML_PARAGRAPH_CLOSE_PATTERN, "\n\n", text)
        text = re.sub(HTML_SCRIPT_BLOCK_PATTERN, "", text)
        text = re.sub(HTML_STYLE_BLOCK_PATTERN, "", text)
        text = re.sub(HTML_TAG_PATTERN, "", text)
        text = html.unescape(text)
        text = re.sub(MULTI_NEWLINE_PATTERN, "\n\n", text)
        return text.strip()

    def _extract_markdown(self, obj: Any) -> Optional[str]:
        """Return the first markdown/text-like payload found in a response fragment."""

        keys = {"markdown", "commonmark", "content", "text", "body", "result", "html", "raw_html"}

        def _walk(value: Any) -> Optional[str]:
            if isinstance(value, str):
                if not value.strip():
                    return None
                # Detect obvious HTML and convert to markdown before returning.
                looks_like_html = "<" in value and ">" in value and ("<html" in value.lower() or "<div" in value.lower() or "<p" in value.lower())
                return self._html_to_markdown(value) if looks_like_html else value
            if isinstance(value, dict):
                for key, val in value.items():
                    if key.lower() in keys and isinstance(val, str) and val.strip():
                        looks_like_html = key.lower() in {"html", "raw_html"} or ("<" in val and ">" in val)
                        return self._html_to_markdown(val) if looks_like_html else val
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

    def _detect_captcha(self, markdown_text: str, events: List[Any]) -> Optional[str]:
        """Return a matched captcha marker when the payload looks like a bot check."""

        haystack_parts: List[str] = []
        if isinstance(markdown_text, str) and markdown_text.strip():
            haystack_parts.append(markdown_text)

        for evt in events:
            if not isinstance(evt, dict):
                continue
            for key in ("title", "reason", "description", "body", "message"):
                val = evt.get(key)
                if isinstance(val, str) and val.strip():
                    haystack_parts.append(val)

        haystack = " ".join(haystack_parts).lower()
        markers = (
            "vercel security checkpoint",
            "checking your browser",
            "are you human",
            "captcha",
            "security check",
            "robot check",
            "access denied",
        )

        for marker in markers:
            if marker in haystack:
                return marker
        return None

    def _extract_structured_job_posting(self, events: List[Any]) -> Optional[Dict[str, Any]]:
        """Best-effort extraction of JSON-LD JobPosting data from raw HTML events."""

        def _gather_strings(node: Any) -> List[str]:
            results: List[str] = []
            if isinstance(node, str):
                if node.strip():
                    results.append(node)
                return results
            if isinstance(node, dict):
                for child in node.values():
                    results.extend(_gather_strings(child))
            elif isinstance(node, list):
                for child in node:
                    results.extend(_gather_strings(child))
            return results

        def _is_job_posting(candidate: Dict[str, Any]) -> bool:
            raw_type = candidate.get("@type") or candidate.get("type")
            if isinstance(raw_type, list):
                if any("jobposting" in str(item).lower() for item in raw_type):
                    return True
            if isinstance(raw_type, str) and "jobposting" in raw_type.lower():
                return True
            if "title" in candidate and "description" in candidate:
                if any(key in candidate for key in ("datePosted", "hiringOrganization", "jobLocation")):
                    return True
            return False

        def _find_job_posting(node: Any) -> Optional[Dict[str, Any]]:
            if isinstance(node, dict):
                if _is_job_posting(node):
                    return node
                for val in node.values():
                    found = _find_job_posting(val)
                    if found:
                        return found
            elif isinstance(node, list):
                for child in node:
                    found = _find_job_posting(child)
                    if found:
                        return found
            return None

        script_pattern = re.compile(
            r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(?P<payload>.*?)</script>",
            flags=re.IGNORECASE | re.DOTALL,
        )

        for text in _gather_strings(events):
            if "<script" not in text.lower():
                continue
            for match in script_pattern.finditer(text):
                payload_raw = html.unescape(match.group("payload").strip())
                parsed = self._try_parse_json(payload_raw)
                if parsed is None:
                    continue
                found = _find_job_posting(parsed)
                if found:
                    return found

        return None

    def _location_from_job_posting(self, payload: Dict[str, Any]) -> Optional[str]:
        def _normalize_part(value: Any) -> Optional[str]:
            if isinstance(value, dict):
                value = value.get("name") or value.get("value") or value.get("label")
            if not isinstance(value, str):
                return None
            cleaned = value.strip()
            if not cleaned or cleaned.upper() == "UNAVAILABLE":
                return None
            return cleaned

        def _format_address(address: Any) -> Optional[str]:
            if not isinstance(address, dict):
                return _normalize_part(address)
            parts = []
            seen_tokens: set[str] = set()
            for key in ("addressLocality", "addressRegion", "addressCountry"):
                part = _normalize_part(address.get(key))
                if not part:
                    continue
                tokens = [token.strip().lower() for token in part.split(",") if token.strip()]
                if tokens and all(token in seen_tokens for token in tokens):
                    continue
                parts.append(part)
                seen_tokens.update(tokens)
            if not parts:
                return None
            return ", ".join(parts)

        locations: List[str] = []
        raw_locations = payload.get("jobLocation")
        if isinstance(raw_locations, list):
            candidates = raw_locations
        elif raw_locations is not None:
            candidates = [raw_locations]
        else:
            candidates = []

        for entry in candidates:
            if isinstance(entry, dict):
                address = entry.get("address")
                formatted = _format_address(address or entry)
            else:
                formatted = _normalize_part(entry)
            if formatted and formatted not in locations:
                locations.append(formatted)

        if locations:
            return locations[0]
        return None

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
        if "```" in markdown:
            fenced_match = re.search(
                r"```(?:json)?\s*(\{.*?\})\s*```",
                markdown,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if fenced_match:
                try:
                    parsed = json.loads(fenced_match.group(1))
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    for key in ("title", "job_title", "heading"):
                        val = parsed.get(key)
                        if isinstance(val, str) and val.strip():
                            return val.strip()
        for line in markdown.splitlines():
            if not line.strip():
                continue
            heading_match = re.match(MARKDOWN_HEADING_PATTERN, line.strip())
            if heading_match:
                return heading_match.group(1).strip()
            stripped = line.strip()
            if stripped.startswith(("{", "[")):
                continue
            if len(stripped) > 6:
                return stripped
        return None

    def _title_with_required_keyword(self, markdown: str) -> Optional[str]:
        """Find the first markdown line that satisfies required title keywords."""

        if not markdown:
            return None

        for raw_line in markdown.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            # Strip heading markers before evaluating keywords so `# Title` works.
            line = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", line)
            if title_matches_required_keywords(line):
                return line.strip()

        return None

    def _title_from_url(self, url: str) -> str:
        slug = url.split("/")[-1] if "/" in url else url
        slug = slug or url
        cleaned = re.sub(SLUG_SEPARATOR_PATTERN, " ", slug).strip()
        cleaned = re.sub(QUERY_STRING_PATTERN, "", cleaned)
        if not cleaned:
            return "Untitled"
        return cleaned.title()

    def _is_placeholder_title(self, title: str) -> bool:
        placeholders = {"page_title", "title", "job_title", "untitled", "unknown"}
        stripped = title.strip().lower()
        if stripped in placeholders:
            return True
        # Reject IDs masquerading as titles (e.g., numeric requisition IDs).
        return bool(re.fullmatch(r"\d{3,}", stripped))

    def _regex_extract_job_urls(self, text: str) -> List[str]:
        """
        Fallback extraction for Greenhouse listings when structured parsing fails.

        Looks for greenhouse job URLs and returns a deduped list.
        """

        if not text:
            return []
        # Capture both boards.greenhouse.io and api.greenhouse.io absolute URLs
        pattern = re.compile(GREENHOUSE_URL_PATTERN)
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

    async def _fetch_site_api(
        self,
        handler: BaseSiteHandler,
        source_url: str,
        *,
        pattern: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        api_url = handler.get_listing_api_uri(source_url) or handler.get_api_uri(source_url)
        if not api_url:
            return None

        params = {"includeCompensation": "false"}
        request_url = self._merge_query_params(api_url, params)
        started_at = int(time.time() * 1000)
        api_key = self._api_key()
        spider_params: Dict[str, Any] = {
            "return_format": ["raw_html"],
            "metadata": True,
            "request": "chrome",
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        }
        spider_params.update(handler.get_spidercloud_config(request_url))
        try:
            async with AsyncSpider(api_key=api_key) as client:
                scrape_fn = getattr(client, "scrape_url", None) or getattr(client, "crawl_url")
                response = scrape_fn(  # type: ignore[call-arg]
                    request_url,
                    params=spider_params,
                    stream=False,
                    content_type="application/json",
                )
                raw_events: list[Any] = []
                async for chunk in self._iterate_scrape_response(response):
                    raw_events.append(chunk)
                payload = self._extract_json_payload(raw_events)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Site API fetch failed handler=%s url=%s error=%s",
                handler.name,
                request_url,
                exc,
            )
            return None

        if not isinstance(payload, dict):
            logger.warning(
                "Site API fetch returned non-dict handler=%s url=%s payload_type=%s",
                handler.name,
                request_url,
                type(payload).__name__ if payload is not None else "none",
            )
            return None

        job_urls = handler.get_links_from_json(payload)
        jobs = payload.get("jobs") if isinstance(payload, dict) else None
        job_count = len(jobs) if isinstance(jobs, list) else 0
        if job_count and not job_urls:
            logger.warning(
                "Site API fetch missing job URLs handler=%s url=%s jobs=%s; falling back to rendered scrape",
                handler.name,
                api_url,
                job_count,
            )
            return None

        provider_request: Dict[str, Any] = {
            "url": request_url,
            "params": spider_params,
            "method": "GET",
            "contentType": "application/json",
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
            "normalized": [],
            "provider": self.provider,
            "seedUrls": [source_url],
            "job_urls": job_urls,
            "raw": payload,
            "request": request_snapshot,
            "requestedFormat": "json",
        }

        scrape_payload: Dict[str, Any] = {
            "sourceUrl": source_url,
            "pattern": pattern,
            "startedAt": started_at,
            "completedAt": completed_at,
            "items": items_block,
            "provider": self.provider,
            "subUrls": [api_url],
            "request": request_snapshot,
            "providerRequest": provider_request,
            "requestedFormat": "json",
        }

        trimmed = self._trim_scrape_payload(scrape_payload)
        logger.info(
            "Site API fetch succeeded handler=%s url=%s jobs=%s job_urls=%s",
            handler.name,
            request_url,
            job_count,
            len(job_urls),
        )
        self.deps.log_sync_response(
            self.provider,
            action="scrape",
            url=source_url,
            summary=f"{handler.name}_api jobs={job_count} urls={len(job_urls)}",
            metadata={"pattern": pattern, "seed": 1, "api": True},
            response=trimmed,
        )
        return trimmed

    def _merge_query_params(self, url: str, params: Dict[str, Any]) -> str:
        if not params:
            return url
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        query_items = parse_qsl(parsed.query, keep_blank_values=True)
        if params:
            filtered = [(key, val) for key, val in query_items if key not in params]
            merged = filtered + [(key, str(val)) for key, val in params.items()]
        else:
            merged = query_items
        new_query = urlencode(merged, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def _extract_json_payload(self, value: Any) -> Optional[Dict[str, Any]]:
        def _find_jobs_payload(node: Any) -> Optional[Dict[str, Any]]:
            if isinstance(node, dict):
                jobs = node.get("jobs")
                if isinstance(jobs, list):
                    return node
                for child in node.values():
                    found = _find_jobs_payload(child)
                    if found:
                        return found
            elif isinstance(node, list):
                for child in node:
                    found = _find_jobs_payload(child)
                    if found:
                        return found
            return None

        def _gather_strings(node: Any) -> list[str]:
            results: list[str] = []
            if isinstance(node, str):
                if node.strip():
                    results.append(node)
                return results
            if isinstance(node, dict):
                for child in node.values():
                    results.extend(_gather_strings(child))
            elif isinstance(node, list):
                for child in node:
                    results.extend(_gather_strings(child))
            return results

        def _parse_json_text(text: str) -> Any | None:
            try:
                parsed = json.loads(text)
            except Exception:
                return None
            if isinstance(parsed, str):
                try:
                    parsed = json.loads(parsed)
                except Exception:
                    return parsed
            return parsed

        def _extract_json_from_html(text: str) -> Optional[Dict[str, Any]]:
            if "<pre" not in text.lower():
                return None
            match = re.search(r"<pre[^>]*>(?P<content>.*?)</pre>", text, flags=re.IGNORECASE | re.DOTALL)
            content = match.group("content") if match else text
            if not content:
                return None
            content = html.unescape(content).strip()
            if not content:
                return None
            candidate = content
            if not candidate.lstrip().startswith("{"):
                brace_match = re.search(r"{.*}", candidate, flags=re.DOTALL)
                if brace_match:
                    candidate = brace_match.group(0)
            parsed = _parse_json_text(candidate)
            if parsed is None and candidate:
                try:
                    unescaped = candidate.encode("utf-8", errors="ignore").decode("unicode_escape")
                except Exception:
                    unescaped = ""
                if unescaped:
                    parsed = _parse_json_text(unescaped)
            return _find_jobs_payload(parsed) if parsed is not None else None

        found = _find_jobs_payload(value)
        if found:
            return found

        for text in _gather_strings(value):
            parsed = _parse_json_text(text)
            if parsed is not None:
                found = _find_jobs_payload(parsed)
                if found:
                    return found
            html_found = _extract_json_from_html(text)
            if html_found:
                return html_found

        return None

    def _payload_has_job_urls(self, payload: Dict[str, Any]) -> bool:
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            return False
        url_keys = ("jobUrl", "applyUrl", "jobPostingUrl", "postingUrl", "url")
        for job in jobs:
            if not isinstance(job, dict):
                continue
            for key in url_keys:
                value = job.get(key)
                if isinstance(value, str) and value.strip():
                    return True
        return False

    def _extract_listing_job_urls(
        self,
        handler: BaseSiteHandler,
        raw_events: List[Any],
        markdown_text: str,
    ) -> List[str]:
        payload = self._extract_json_payload(raw_events)
        if payload is None and markdown_text:
            payload = self._extract_json_payload(markdown_text)
        if not isinstance(payload, dict):
            return []
        urls = handler.get_links_from_json(payload)
        return [u for u in urls if isinstance(u, str) and u.strip()]

    def _normalize_job(
        self,
        url: str,
        markdown: str,
        events: List[Any],
        started_at: int,
        *,
        require_keywords: bool = True,
    ) -> Dict[str, Any] | None:
        parsed_title = None
        parsed_markdown = markdown or ""
        if parsed_markdown.lstrip().startswith(("{", "[")):
            handler = self._get_site_handler(url)
            if handler:
                parsed_markdown, parsed_title = handler.normalize_markdown(parsed_markdown)

        listing_payload = self._extract_json_payload(events) or self._extract_json_payload(parsed_markdown)
        if isinstance(listing_payload, dict) and self._payload_has_job_urls(listing_payload):
            self._last_ignored_job = {
                "url": url,
                "reason": "listing_payload",
                "title": self._title_from_url(url),
                "description": "listing_payload",
            }
            return None

        structured_payload = self._extract_structured_job_posting(events)
        structured_title = None
        structured_location = None
        structured_description = None
        if structured_payload:
            raw_title = structured_payload.get("title")
            if isinstance(raw_title, str) and raw_title.strip():
                structured_title = raw_title.strip()
            raw_description = structured_payload.get("description")
            if isinstance(raw_description, str) and raw_description.strip():
                structured_description = raw_description.strip()
            structured_location = self._location_from_job_posting(structured_payload)

        if structured_description and len(parsed_markdown.strip()) < 50:
            parsed_markdown = self._html_to_markdown(structured_description)

        cleaned_markdown = strip_known_nav_blocks(parsed_markdown or "")
        hints = parse_markdown_hints(cleaned_markdown)
        hint_title = hints.get("title") if isinstance(hints, dict) else None

        payload_title = parsed_title or structured_title or self._title_from_events(events)
        from_content = False
        if payload_title and self._is_placeholder_title(payload_title):
            payload_title = None
        if not payload_title:
            payload_title = hint_title or self._title_from_markdown(cleaned_markdown)
            from_content = bool(payload_title)
        else:
            from_content = True

        candidate_title = payload_title or parsed_title
        if looks_like_job_listing_page(candidate_title, cleaned_markdown, url):
            self._last_ignored_job = {
                "url": url,
                "reason": "listing_page",
                "title": candidate_title or self._title_from_url(url),
                "description": cleaned_markdown,
            }
            return None
        if looks_like_error_landing(candidate_title, cleaned_markdown):
            self._last_ignored_job = {
                "url": url,
                "reason": "error_landing",
                "title": candidate_title,
                "description": cleaned_markdown,
            }
            return None

        title = payload_title or self._title_from_url(url)
        if isinstance(title, str) and len(title) > MAX_TITLE_CHARS:
            logger.info(
                "SpiderCloud title too long; falling back to URL title url=%s title_len=%s",
                url,
                len(title),
            )
            title = self._title_from_url(url)

        if from_content and not title_matches_required_keywords(title):
            keyword_title = self._title_with_required_keyword(cleaned_markdown)
            if keyword_title:
                title = keyword_title
        if from_content and not title_matches_required_keywords(title):
            logger.info(
                "SpiderCloud dropping job due to missing required keyword url=%s title=%s",
                url,
                title,
            )
            if require_keywords:
                self._last_ignored_job = {
                    "url": url,
                    "reason": "missing_required_keyword",
                    "title": title,
                    "description": cleaned_markdown,
                }
                return None
        company = derive_company_from_url(url) or "Unknown"
        location_hint = hints.get("location") if isinstance(hints, dict) else None
        location = structured_location or location_hint
        remote = coerce_remote(hints.get("remote") if isinstance(hints, dict) else None, location or "", f"{title}\n{cleaned_markdown}")
        level = coerce_level(hints.get("level") if isinstance(hints, dict) else None, title)
        description = cleaned_markdown or ""

        self._last_ignored_job = None
        return {
            "job_title": title,
            "title": title,
            "company": company,
            "location": location or ("Remote" if remote else "Unknown"),
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

    async def _iterate_scrape_response(self, response: Any):
        """Normalize SpiderCloud responses to an async iterable.

        Some test doubles return a coroutine instead of an async generator.  We
        accept either shape so that captcha errors raised before the first
        yield still propagate cleanly.
        """

        if hasattr(response, "__aiter__"):
            async for item in response:
                yield item
            return

        if hasattr(response, "__await__"):
            result = await response
            if result is not None:
                yield result
            return

        # Fallback for unexpected synchronous return values in tests/mocks.
        if response is not None:
            yield response

    async def _scrape_single_url(
        self,
        client: AsyncSpider,
        url: str,
        params: Dict[str, Any],
        *,
        attempt: int = 0,
    ) -> Dict[str, Any]:
        buffer = ""
        raw_events: List[Any] = []
        markdown_parts: List[str] = []
        credit_candidates: List[float] = []
        cost_candidates_usd: List[float] = []
        started_at = int(time.time() * 1000)
        logger.debug("SpiderCloud scrape started url=%s", url)

        # Prefer SpiderCloud /scrape endpoint when available, fall back to /crawl.
        scrape_fn = getattr(client, "scrape_url", None) or getattr(client, "crawl_url")
        local_params = dict(params)
        handler = self._get_site_handler(url)
        if handler:
            local_params.update(handler.get_spidercloud_config(url))

        try:
            async for chunk in self._iterate_scrape_response(
                scrape_fn(  # type: ignore[call-arg]
                    url,
                    params=local_params,
                    stream=True,
                    content_type="application/jsonl",
                )
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
                logger.debug("SpiderCloud stream empty; falling back to non-stream fetch url=%s", url)
                async for resp in self._iterate_scrape_response(
                    scrape_fn(  # type: ignore[call-arg]
                        url,
                        params=local_params,
                        stream=False,
                        content_type="application/json",
                    )
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
        except CaptchaDetectedError:
            # Surface captcha markers to the batch loop so it can retry with proxies.
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("SpiderCloud scrape failed url=%s error=%s", url, exc)
            raise ApplicationError(f"SpiderCloud scrape failed for {url}: {exc}") from exc

        logger.debug(
            "SpiderCloud stream parsed url=%s events=%s markdown_fragments=%s credit_candidates=%s",
            url,
            len(raw_events),
            len(markdown_parts),
            len(credit_candidates),
        )

        # Detect captcha walls early so the caller can decide whether to retry with a proxy.
        marker = self._detect_captcha("\n\n".join(markdown_parts), raw_events)
        if marker:
            logger.warning(
                "SpiderCloud captcha detected url=%s attempt=%s marker=%s",
                url,
                attempt,
                marker,
            )
            raise CaptchaDetectedError(marker, "\n\n".join(markdown_parts), raw_events)

        markdown_text = "\n\n".join(
            [part for part in markdown_parts if isinstance(part, str) and part.strip()]
        ).strip()
        if handler and handler.is_api_detail_url(url):
            markdown_text, gh_title = handler.normalize_markdown(markdown_text)
            if gh_title:
                raw_events.append({"title": gh_title, "gh_api_title": True})
        listing_job_urls: List[str] = []
        if handler and handler.supports_listing_api:
            try:
                listing_job_urls = self._extract_listing_job_urls(handler, raw_events, markdown_text)
            except Exception:
                listing_job_urls = []
        credits_used = max(credit_candidates) if credit_candidates else None
        cost_milli_cents = (
            int(max(cost_candidates_usd) * 100000) if cost_candidates_usd else None
        )
        if cost_milli_cents is None and credits_used is not None:
            cost_milli_cents = int(float(credits_used) * 10)
        cost_usd = (cost_milli_cents / 100000) if isinstance(cost_milli_cents, (int, float)) else None
        require_keywords = attempt <= 1
        normalized = self._normalize_job(
            url,
            markdown_text,
            raw_events,
            started_at,
            require_keywords=require_keywords,
        )
        ignored_entry = getattr(self, "_last_ignored_job", None)
        if listing_job_urls and normalized:
            normalized = None
            ignored_entry = {
                "url": url,
                "reason": "listing_payload",
                "title": self._title_from_url(url),
                "description": "listing_payload",
            }
            self._last_ignored_job = ignored_entry

        logger.debug(
            "SpiderCloud stream complete url=%s events=%s markdown_fragments=%s credits=%s cost_usd=%s",
            url,
            len(raw_events),
            len(markdown_parts),
            credits_used,
            cost_usd,
        )

        return {
            "normalized": normalized,
            "raw": {
                "url": url,
                "events": raw_events,
                "markdown": markdown_text,
                "creditsUsed": credits_used,
                "job_urls": listing_job_urls,
            },
            "job_urls": listing_job_urls,
            "creditsUsed": credits_used,
            "costMilliCents": cost_milli_cents,
            "startedAt": started_at,
            "ignored": ignored_entry,
        }
        logger.debug(
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
            "SpiderCloud batch start source=%s urls=%s pattern=%s",
            source_url,
            len(urls),
            pattern,
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
        def _infer_spidercloud_config(url: str) -> Dict[str, Any]:
            try:
                parsed = urlparse(url)
            except Exception:
                return {}
            host = (parsed.hostname or "").lower()
            path = (parsed.path or "").lower()
            if not host or not path:
                return {}
            if path.startswith("/jobs") and "careers." in host:
                return {"return_format": ["raw_html"], "preserve_host": True}
            if "careers" in path and "/jobs" in path and "careers." in host:
                return {"return_format": ["raw_html"], "preserve_host": True}
            return {}

        handler_configs: List[Dict[str, Any]] = []
        for url in urls:
            handler = self._get_site_handler(url)
            if handler:
                config = handler.get_spidercloud_config(url)
                if not config:
                    config = _infer_spidercloud_config(url)
                handler_configs.append(config)
            else:
                handler_configs.append(_infer_spidercloud_config(url))

        def _wants_raw_html(config: Dict[str, Any]) -> bool:
            value = config.get("return_format")
            if isinstance(value, list):
                return "raw_html" in value
            if isinstance(value, str):
                return "raw_html" in value
            return False

        use_raw_html = any(_wants_raw_html(cfg) for cfg in handler_configs)
        preserve_host = all(cfg.get("preserve_host", True) for cfg in handler_configs)
        requested_format = "raw_html" if use_raw_html else "commonmark"
        params: Dict[str, Any] = {
            "return_format": ["raw_html"] if use_raw_html else ["commonmark"],
            "metadata": True,
            "request": "chrome",
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": preserve_host,
            "limit": 1,
        }
        started_at = int(time.time() * 1000)
        normalized_items: List[Dict[str, Any]] = []
        raw_items: List[Dict[str, Any]] = []
        ignored_items: List[Dict[str, Any]] = []
        listing_job_urls: List[str] = []
        total_cost_milli_cents = 0.0
        saw_cost_field = False

        timeout_seconds = runtime_config.spidercloud_http_timeout_seconds
        max_concurrency = max(1, int(runtime_config.spidercloud_job_details_concurrency))
        max_concurrency = min(max_concurrency, len(urls))
        semaphore = asyncio.Semaphore(max_concurrency)

        async with AsyncSpider(api_key=api_key) as client:
            async def _scrape_one(idx: int, url: str) -> tuple[int, str, Dict[str, Any] | None]:
                async with semaphore:
                    # When we receive an API detail URL, try to also capture a
                    # marketing-friendly apply URL for downstream preference.
                    handler = self._get_site_handler(url)
                    marketing_url = (
                        handler.get_company_uri(url) if handler and handler.name == "greenhouse" else None
                    )

                    attempt = 0
                    result: Dict[str, Any] | None = None
                    proxy: Optional[str] = None
                    while attempt <= CAPTCHA_RETRY_LIMIT:
                        attempt += 1
                        local_params = dict(params)
                        if proxy:
                            local_params["proxy"] = proxy
                        try:
                            scrape_coro = self._scrape_single_url(
                                client,
                                url,
                                local_params,
                                attempt=attempt,
                            )
                            if timeout_seconds and timeout_seconds > 0:
                                result = await asyncio.wait_for(scrape_coro, timeout=timeout_seconds)
                            else:
                                result = await scrape_coro
                            break
                        except CaptchaDetectedError as err:
                            proxy = CAPTCHA_PROXY_SEQUENCE[min(attempt - 1, len(CAPTCHA_PROXY_SEQUENCE) - 1)]
                            logger.warning(
                                "SpiderCloud captcha retry url=%s attempt=%s/%s proxy=%s marker=%s",
                                url,
                                attempt,
                                CAPTCHA_RETRY_LIMIT + 1,
                                proxy,
                                err.marker,
                            )
                            if attempt > CAPTCHA_RETRY_LIMIT:
                                self.deps.log_sync_response(
                                    self.provider,
                                    action="scrape",
                                    url=url,
                                    summary=f"captcha_failed marker={err.marker}",
                                    metadata={"attempts": attempt, "proxy": proxy},
                                )
                                break
                        except asyncio.TimeoutError:
                            logger.warning(
                                "SpiderCloud scrape timed out url=%s timeout=%s",
                                url,
                                timeout_seconds,
                            )
                            break
                        except Exception:
                            # Bubble up unexpected errors
                            raise

                    if not result:
                        logger.warning("SpiderCloud skipping url after retries url=%s", url)
                        return idx, url, None

                    if marketing_url and isinstance(result, dict):
                        normalized_block = result.get("normalized")
                        if isinstance(normalized_block, dict) and not normalized_block.get("apply_url"):
                            normalized_block["apply_url"] = marketing_url

                    return idx, url, result

            tasks = [asyncio.create_task(_scrape_one(idx, url)) for idx, url in enumerate(urls)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, BaseException):
                    raise res
            results.sort(key=lambda item: item[0])

            for _, url, result in results:
                if not result:
                    continue
                if result.get("normalized"):
                    normalized_items.append(result["normalized"])
                if result.get("ignored"):
                    ignored_items.append(result["ignored"])
                if result.get("raw"):
                    raw_items.append(result["raw"])
                if result.get("job_urls"):
                    try:
                        listing_job_urls.extend([u for u in result.get("job_urls") if isinstance(u, str)])
                    except Exception:
                        pass
                cost_mc = result.get("costMilliCents")
                credits = result.get("creditsUsed")
                if isinstance(cost_mc, (int, float)):
                    total_cost_milli_cents += float(cost_mc)
                    saw_cost_field = True
                elif isinstance(credits, (int, float)):
                    total_cost_milli_cents += float(credits) * 10
                logger.debug(
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
            "requestedFormat": requested_format,
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
            "requestedFormat": requested_format,
        }
        if listing_job_urls:
            deduped: List[str] = []
            seen_urls: set[str] = set()
            for url in listing_job_urls:
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                deduped.append(url)
            items_block["job_urls"] = deduped
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
            "requestedFormat": requested_format,
        }
        if cost_milli_cents is not None:
            scrape_payload["costMilliCents"] = cost_milli_cents

        trimmed = self._trim_scrape_payload(scrape_payload)
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
            "SpiderCloud batch complete source=%s urls=%s items=%s cost_mc=%s cost_usd=%s",
            source_url,
            len(urls),
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

        handler = self._get_site_handler(source_url, site.get("type"))
        if handler and handler.supports_listing_api:
            api_payload = await self._fetch_site_api(
                handler,
                source_url,
                pattern=site.get("pattern"),
            )
            if api_payload is not None:
                return api_payload

        urls = [u for u in [source_url] if isinstance(u, str) and u.strip()]

        resolved_skip: Optional[List[str]] = skip_urls
        if resolved_skip is None and source_url:
            try:
                resolved_skip = await self.deps.fetch_seen_urls_for_site(source_url, site.get("pattern"))
            except Exception:
                resolved_skip = []

        skip_set = set(resolved_skip or [])
        urls = [u for u in urls if u not in skip_set]
        if site.get("pattern") and source_url and source_url in skip_set:
            urls = [source_url]
        skip_source = "precomputed" if skip_urls is not None else "fetched"
        logger.info(
            "SpiderCloud scrape_site source=%s pattern=%s skip=%s final_urls=%s",
            source_url,
            site.get("pattern"),
            len(skip_set),
            len(urls),
        )
        logger.info(
            "SpiderCloud skip list source=%s source_url=%s size=%s",
            skip_source,
            source_url,
            len(skip_set),
        )

        self.deps.log_dispatch(
            self.provider,
            source_url,
            pattern=site.get("pattern"),
            siteId=site.get("_id"),
            skip=len(skip_set),
        )
        return await self._scrape_urls_batch(
            urls,
            source_url=source_url,
            pattern=site.get("pattern"),
        )

    async def _fetch_greenhouse_listing_payload(
        self,
        api_url: str,
        handler: BaseSiteHandler | None,
    ) -> tuple[str, list[Any]]:
        api_key = self._api_key()
        spider_params: Dict[str, Any] = {
            "return_format": ["raw_html"],
            "metadata": True,
            "request": "chrome",
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        }
        if handler:
            spider_params.update(handler.get_spidercloud_config(api_url))

        async def _do_fetch() -> list[Any]:
            async with AsyncSpider(api_key=api_key) as client:
                scrape_fn = getattr(client, "scrape_url", None) or getattr(client, "crawl_url")
                response = scrape_fn(  # type: ignore[call-arg]
                    api_url,
                    params=spider_params,
                    stream=False,
                    content_type="application/json",
                )
                raw_events: list[Any] = []
                async for chunk in self._iterate_scrape_response(response):
                    raw_events.append(chunk)
                return raw_events

        timeout_seconds = runtime_config.spidercloud_http_timeout_seconds
        try:
            if timeout_seconds and timeout_seconds > 0:
                raw_events = await asyncio.wait_for(_do_fetch(), timeout=timeout_seconds)
            else:
                raw_events = await _do_fetch()
        except asyncio.TimeoutError as exc:
            logger.error(
                "SpiderCloud greenhouse listing timed out url=%s timeout=%s",
                api_url,
                timeout_seconds,
            )
            raise ApplicationError(
                f"Failed to fetch Greenhouse board via SpiderCloud (timeout {timeout_seconds}s)."
            ) from exc
        except Exception as exc:  # noqa: BLE001
            logger.error("SpiderCloud greenhouse listing fetch failed url=%s error=%s", api_url, exc)
            raise ApplicationError(f"Failed to fetch Greenhouse board via SpiderCloud: {exc}") from exc

        def _extract_text(value: Any) -> str:
            if isinstance(value, dict):
                for key in ("content", "raw_html", "html", "text", "body", "result"):
                    candidate = value.get(key)
                    if isinstance(candidate, (bytes, bytearray)):
                        candidate = candidate.decode("utf-8", errors="replace")
                    if isinstance(candidate, str) and candidate.strip():
                        return candidate
                for child in value.values():
                    found = _extract_text(child)
                    if found:
                        return found
                return ""
            if isinstance(value, list):
                for child in value:
                    found = _extract_text(child)
                    if found:
                        return found
                return ""
            if isinstance(value, (bytes, bytearray)):
                return value.decode("utf-8", errors="replace")
            if isinstance(value, str):
                return value
            return ""

        raw_text = ""
        for event in raw_events:
            candidate = _extract_text(event)
            if isinstance(candidate, str) and candidate.strip():
                raw_text = candidate
                break

        return raw_text, raw_events

    async def fetch_greenhouse_listing(self, site: Site) -> Dict[str, Any]:  # type: ignore[override]
        """Fetch a Greenhouse board JSON feed directly."""

        url = site.get("url") or ""
        handler = self._get_site_handler(url, site.get("type"))
        api_url = handler.get_listing_api_uri(url) if handler else None
        api_url = api_url or url
        slug = ""
        match = re.search(GREENHOUSE_BOARDS_PATH_PATTERN, api_url)
        if match:
            slug = match.group(1)

        logger.info(
            "SpiderCloud greenhouse listing fetch url=%s slug=%s api_url=%s",
            url,
            slug,
            api_url,
        )
        self.deps.log_dispatch(self.provider, url, kind="greenhouse_board", siteId=site.get("_id"))
        started_at = int(time.time() * 1000)
        raw_text, raw_events = await self._fetch_greenhouse_listing_payload(api_url, handler)
        payload = self._extract_json_payload(raw_events)
        if not raw_text and payload is not None:
            try:
                raw_text = json.dumps(payload, ensure_ascii=False)
            except Exception:
                raw_text = str(payload)

        try:
            board = load_greenhouse_board(payload or raw_text or {})
            # Structured extraction first.
            job_urls = extract_greenhouse_job_urls(board)

            # Prefer API detail URLs when we know the board slug and job IDs.
            if slug and job_urls:
                api_urls: list[str] = []
                seen_api: set[str] = set()
                for job in board.jobs:
                    if not job.absolute_url or not title_matches_required_keywords(job.title):
                        continue
                    # Only build API URLs when the original link clearly points to a Greenhouse flow
                    # (either greenhouse domain or gh_jid markers).
                    if (
                        "greenhouse.io" not in job.absolute_url
                        and "gh_jid" not in job.absolute_url
                        and "gh_jid=" not in job.absolute_url
                    ):
                        continue
                    job_id = getattr(job, "id", None)
                    if job_id is None:
                        continue
                    api_url = (
                        handler.get_api_uri(f"https://boards.greenhouse.io/{slug}/jobs/{job_id}")
                        if handler
                        else None
                    )
                    api_url = api_url or f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}"
                    if api_url not in seen_api:
                        seen_api.add(api_url)
                        api_urls.append(api_url)
                if api_urls:
                    job_urls = api_urls

            # If structured extraction yields nothing (or a single item), fall back to regex
            # parsing so we still return a useful list for downstream workflows.
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
            "SpiderCloud greenhouse_jobs start urls_total=%s urls_deduped=%s source=%s",
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
