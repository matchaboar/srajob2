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
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING
from spider import AsyncSpider
from temporalio.exceptions import ApplicationError

from ...components.models import extract_greenhouse_job_urls, load_greenhouse_board
from ...constants import title_matches_required_keywords
from ...config import runtime_config
from ..helpers.scrape_utils import (
    _JOB_DETAIL_MARKERS as JOB_DETAIL_MARKERS,
    _METADATA_LABEL_KEYS,
    _normalize_country_label,
    _normalize_section_heading,
    _strip_embedded_theme_json,
    MAX_JOB_DESCRIPTION_CHARS,
    UNKNOWN_COMPENSATION_REASON,
    coerce_level,
    coerce_remote,
    derive_company_from_url,
    looks_like_error_landing,
    looks_like_job_listing_page,
    parse_markdown_hints,
    parse_posted_at,
    parse_posted_at_with_unknown,
    split_description_metadata,
    strip_known_nav_blocks,
)
from ..helpers.link_extractors import gather_strings, normalize_url
from ..helpers.regex_patterns import (
    CAPTCHA_PROVIDER_PATTERN,
    CAPTCHA_WORD_PATTERN,
    CODE_FENCE_CONTENT_PATTERN,
    CODE_FENCE_END_PATTERN,
    CODE_FENCE_JSON_OBJECT_PATTERN,
    CODE_FENCE_START_PATTERN,
    GREENHOUSE_BOARDS_PATH_PATTERN,
    GREENHOUSE_URL_PATTERN,
    HTML_BR_TAG_PATTERN,
    HTML_SCRIPT_BLOCK_PATTERN,
    HTML_STYLE_BLOCK_PATTERN,
    HTML_TAG_PATTERN,
    INVALID_JSON_ESCAPE_PATTERN,
    JSON_LD_SCRIPT_PATTERN,
    JSON_OBJECT_PATTERN,
    JOB_ID_PATH_PATTERN,
    MARKDOWN_HEADING_PATTERN,
    MARKDOWN_HEADING_PREFIX_PATTERN,
    MIN_THREE_DIGIT_PATTERN,
    PRE_PATTERN,
    QUERY_STRING_PATTERN,
    SLUG_SEPARATOR_PATTERN,
    SPIDERCLOUD_HTML_PARAGRAPH_CLOSE_PATTERN,
    SPIDERCLOUD_MULTI_NEWLINE_PATTERN,
    _SALARY_BETWEEN_RE,
    _SALARY_K_RE,
    _SALARY_RANGE_LABEL_RE,
    _SALARY_RE,
    _TITLE_BAR_RE,
    _TITLE_IN_BAR_RE,
)
from ..site_handlers import BaseSiteHandler, get_site_handler
from ...services import telemetry
from .base import BaseScraper

if TYPE_CHECKING:
    from ..activities import Site

SPIDERCLOUD_BATCH_SIZE = 50
CAPTCHA_RETRY_LIMIT = 2
CAPTCHA_PROXY_SEQUENCE = ("residential", "isp")
STRUCTURED_DESCRIPTION_CHROME_MARKERS = (
    "saved jobs",
    "recently viewed jobs",
    "job alerts",
    "sign up for job alerts",
    "join our talent community",
    "view all of our available opportunities",
    "view all jobs",
    "cookie settings",
)
MAX_TITLE_CHARS = 500
UUID_LIKE_RE = re.compile(
    r"[0-9a-f]{8}([-\s]?[0-9a-f]{4}){3}[-\s]?[0-9a-f]{12}",
    flags=re.IGNORECASE,
)
ORDERED_LIST_LINE_RE = re.compile(r"^\d+[\.)]\s+\S+")
JOB_TITLE_KEYWORDS = {
    "accountant",
    "analyst",
    "architect",
    "associate",
    "backend",
    "business",
    "cloud",
    "consultant",
    "data",
    "design",
    "designer",
    "developer",
    "development",
    "devops",
    "engineer",
    "engineering",
    "finance",
    "frontend",
    "fullstack",
    "growth",
    "hr",
    "infrastructure",
    "intern",
    "ios",
    "legal",
    "manager",
    "marketing",
    "mobile",
    "operations",
    "people",
    "platform",
    "principal",
    "product",
    "program",
    "project",
    "qa",
    "quality",
    "recruiter",
    "research",
    "sales",
    "scientist",
    "security",
    "senior",
    "sre",
    "staff",
    "support",
}


logger = logging.getLogger("temporal.worker.activities")


class CaptchaDetectedError(Exception):
    """Raised when a SpiderCloud response looks like a captcha wall."""

    def __init__(
        self,
        marker: str,
        markdown: str | None = None,
        events: Optional[List[Any]] = None,
        match_text: str | None = None,
    ):
        super().__init__(marker)
        self.marker = marker
        self.markdown = markdown or ""
        self.events = events or []
        self.match_text = match_text


class CaptchaRetriesExceededError(Exception):
    """Raised when captcha retries are exhausted for a SpiderCloud scrape."""

    def __init__(
        self,
        url: str,
        marker: str,
        match_text: str | None,
        attempts: int,
        proxy: str | None,
        markdown: str | None = None,
        events: Optional[List[Any]] = None,
    ) -> None:
        message = (
            "Captcha retries exhausted"
            f" url={url} attempts={attempts} marker={marker} match={match_text} proxy={proxy}"
        )
        super().__init__(message)
        self.url = url
        self.marker = marker
        self.match_text = match_text
        self.attempts = attempts
        self.proxy = proxy
        self.markdown = markdown or ""
        self.events = events or []


@dataclass(frozen=True)
class CaptchaMatch:
    marker: str
    match_text: str | None = None


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
        text = re.sub(SPIDERCLOUD_HTML_PARAGRAPH_CLOSE_PATTERN, "\n\n", text)
        text = re.sub(r"</(?:title|h[1-6])\s*>", "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(HTML_SCRIPT_BLOCK_PATTERN, "", text)
        text = re.sub(HTML_STYLE_BLOCK_PATTERN, "", text)
        text = re.sub(HTML_TAG_PATTERN, "", text)
        text = html.unescape(text)
        text = re.sub(SPIDERCLOUD_MULTI_NEWLINE_PATTERN, "\n\n", text)
        return text.strip()

    def _extract_meta_description(self, raw_html: str) -> Optional[str]:
        if not raw_html:
            return None
        meta_pattern = re.compile(r"<meta\s+[^>]*>", re.IGNORECASE)
        attr_pattern = re.compile(r'([a-zA-Z0-9:_-]+)\s*=\s*(["\'])(.*?)\2', re.DOTALL)
        desc = None
        title = None
        for tag in meta_pattern.findall(raw_html):
            attrs = {
                key.lower(): html.unescape(value).strip()
                for key, _, value in attr_pattern.findall(tag)
            }
            if not attrs:
                continue
            name = (
                attrs.get("name")
                or attrs.get("property")
                or attrs.get("itemprop")
                or ""
            ).lower()
            content = attrs.get("content")
            if not content:
                continue
            if not desc and name in {"description", "og:description", "twitter:description"}:
                desc = content.strip()
            if not title and name in {"og:title", "twitter:title"}:
                title = content.strip()
        if not title:
            title_match = re.search(r"<title[^>]*>(.*?)</title>", raw_html, re.IGNORECASE | re.DOTALL)
            if title_match:
                title = html.unescape(title_match.group(1)).strip()
        if desc:
            desc = re.sub(r"\s+", " ", desc).strip()
        if title:
            title = re.sub(r"\s+", " ", title).strip()
        if not desc:
            return None
        if title and title.lower() not in desc.lower():
            return f"# {title}\n\n{desc}"
        return desc

    def _extract_meta_description_from_events(self, events: List[Any]) -> Optional[str]:
        for text in gather_strings(events):
            if not isinstance(text, str):
                continue
            if "<meta" not in text.lower():
                continue
            desc = self._extract_meta_description(text)
            if desc:
                return desc
        return None

    def _extract_greenhouse_location_from_events(self, events: List[Any]) -> Optional[str]:
        def _normalize(value: Any) -> Optional[str]:
            if isinstance(value, dict):
                value = value.get("name") or value.get("location")
            if not isinstance(value, str):
                return None
            cleaned = value.strip()
            return cleaned or None

        for text in gather_strings(events):
            if not isinstance(text, str) or not text.strip():
                continue
            payload = None
            if "<pre" in text.lower():
                payload = BaseSiteHandler._extract_json_payload_from_html(text)  # noqa: SLF001
            if payload is None:
                parsed = self._try_parse_json(text)
                if isinstance(parsed, dict):
                    payload = parsed
            if not isinstance(payload, dict):
                continue
            location = _normalize(payload.get("location"))
            if location:
                return location
            offices = payload.get("offices")
            if isinstance(offices, list) and offices:
                location = _normalize(offices[0])
                if location:
                    return location
        return None

    def _extract_markdown(self, obj: Any) -> Optional[str]:
        """Return a markdown/text payload found in a response fragment."""

        keys = {
            "markdown",
            "commonmark",
            "content",
            "text",
            "body",
            "result",
            "html",
            "raw_html",
            "raw",
        }
        preferred_keys = (
            "commonmark",
            "markdown",
            "content",
            "text",
            "body",
            "result",
            "html",
            "raw_html",
            "raw",
        )
        min_description_len = 80

        def _parse_json_blob(text: str) -> Any | None:
            text = text.strip()
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                pass
            try:
                unescaped = text.encode("utf-8", errors="ignore").decode("unicode_escape")
            except Exception:
                unescaped = ""
            if unescaped:
                try:
                    return json.loads(unescaped)
                except Exception:
                    pass
            match = re.search(JSON_OBJECT_PATTERN, text, flags=re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except Exception:
                    return None
            return None

        def _find_job_description(node: Any) -> tuple[str, str | None] | None:
            if isinstance(node, dict):
                info = node.get("jobPostingInfo")
                if isinstance(info, dict):
                    desc = info.get("jobDescription") or info.get("description")
                    title = info.get("title") or info.get("jobPostingId")
                    if isinstance(desc, str) and desc.strip():
                        return desc, title if isinstance(title, str) and title.strip() else None
                desc = node.get("jobDescription")
                if isinstance(desc, str) and desc.strip():
                    title = node.get("title") if isinstance(node.get("title"), str) else None
                    return desc, title
                for val in node.values():
                    found = _find_job_description(val)
                    if found:
                        return found
            if isinstance(node, list):
                for child in node:
                    found = _find_job_description(child)
                    if found:
                        return found
            return None

        def _job_description_from_pre(raw_html: str) -> Optional[str]:
            if "<pre" not in raw_html.lower():
                return None
            match = PRE_PATTERN.search(raw_html)
            if not match:
                return None
            content = html.unescape(match.group("content") or "").strip()
            if not content:
                return None
            parsed = _parse_json_blob(content)
            if parsed is None:
                return None
            found = _find_job_description(parsed)
            if not found:
                return None
            desc_html, title = found
            desc_html = html.unescape(desc_html)
            rendered = _markdown_from_html(desc_html)
            if title:
                normalized_title = title.strip()
                if normalized_title:
                    first_line = rendered.lstrip().splitlines()[0].strip() if rendered.strip() else ""
                    if first_line.lstrip("#").strip() != normalized_title:
                        return f"# {normalized_title}\n\n{rendered}".strip()
            return rendered.strip()

        def _markdown_from_html(value: str) -> str:
            rendered = self._html_to_markdown(value)
            if len(rendered.strip()) < min_description_len:
                meta_desc = self._extract_meta_description(value)
                if meta_desc:
                    return meta_desc
            return rendered

        def _metadata_description(value: Any) -> Optional[str]:
            if not isinstance(value, dict):
                return None
            meta = value.get("metadata")
            if not isinstance(meta, dict):
                return None
            for key in ("commonmark", "markdown", "content", "raw"):
                entry = meta.get(key)
                if not isinstance(entry, dict):
                    continue
                raw_desc = entry.get("description") or entry.get("content")
                if not isinstance(raw_desc, str):
                    continue
                desc = raw_desc.strip()
                if len(desc) < min_description_len:
                    continue
                raw_title = entry.get("title")
                if isinstance(raw_title, str) and raw_title.strip():
                    return f"# {raw_title.strip()}\n\n{desc}"
                return desc
            return None

        def _direct_markdown_from_dict(value: dict[str, Any]) -> Optional[str]:
            for key in preferred_keys:
                if key not in value:
                    continue
                val = value.get(key)
                if not isinstance(val, str) or not val.strip():
                    continue
                looks_like_html = key in {"html", "raw_html"} or ("<" in val and ">" in val)
                if looks_like_html and "<pre" in val.lower():
                    extracted = _job_description_from_pre(val)
                    if extracted:
                        return extracted
                return _markdown_from_html(val) if looks_like_html else val
            return None

        def _walk(value: Any) -> Optional[str]:
            if isinstance(value, str):
                if not value.strip():
                    return None
                # Detect obvious HTML and convert to markdown before returning.
                looks_like_html = "<" in value and ">" in value and (
                    "<html" in value.lower() or "<div" in value.lower() or "<p" in value.lower()
                )
                if looks_like_html and "<pre" in value.lower():
                    extracted = _job_description_from_pre(value)
                    if extracted:
                        return extracted
                return _markdown_from_html(value) if looks_like_html else value
            if isinstance(value, dict):
                metadata_candidate = _metadata_description(value)
                content_val = value.get("content")
                if isinstance(content_val, dict):
                    content_direct = _direct_markdown_from_dict(content_val)
                    if content_direct:
                        lines = [line for line in content_direct.splitlines() if line.strip()]
                        looks_like_title_only = (
                            len(lines) <= 1 and len(content_direct.strip()) < min_description_len
                        )
                        if metadata_candidate and looks_like_title_only:
                            return metadata_candidate
                        return content_direct

                direct = _direct_markdown_from_dict(value)
                if direct:
                    lines = [line for line in direct.splitlines() if line.strip()]
                    looks_like_title_only = len(lines) <= 1 and len(direct.strip()) < min_description_len
                    if metadata_candidate and looks_like_title_only:
                        return metadata_candidate
                    return direct
                if metadata_candidate:
                    return metadata_candidate
                for key, val in value.items():
                    if key.lower() == "metadata":
                        continue
                    if key.lower() in keys and isinstance(val, str) and val.strip():
                        looks_like_html = key.lower() in {"html", "raw_html"} or ("<" in val and ">" in val)
                        return _markdown_from_html(val) if looks_like_html else val
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

    def _is_greenhouse_job_payload(self, payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        content = payload.get("content")
        title = payload.get("title")
        job_id = payload.get("id") or payload.get("internal_job_id") or payload.get("job_id")
        if not isinstance(content, str) or not content.strip():
            return False
        if not isinstance(title, str) or not title.strip():
            return False
        if job_id is None:
            return False
        return True

    def _has_valid_greenhouse_job_payload(
        self,
        events: List[Any],
        markdown_parts: List[str],
    ) -> bool:
        candidates: list[str] = []
        for text in gather_strings(events):
            if isinstance(text, str) and text.strip():
                candidates.append(text)
        for text in markdown_parts:
            if isinstance(text, str) and text.strip():
                candidates.append(text)

        for text in candidates:
            payload = self._try_parse_json(text)
            if isinstance(payload, str):
                payload = self._try_parse_json(payload)
            if self._is_greenhouse_job_payload(payload):
                return True
        return False

    def _detect_captcha(self, markdown_text: str, events: List[Any]) -> Optional[CaptchaMatch]:
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

        haystack = " ".join(haystack_parts)
        security_check_pattern = re.compile(
            r"(?:security check|security checks).{0,80}(?:browser|captcha|human|robot|verify|cloudflare|ddos)",
            re.IGNORECASE,
        )
        markers: tuple[tuple[str, Optional[re.Pattern[str]]], ...] = (
            ("vercel security checkpoint", None),
            ("checking your browser", None),
            ("are you human", None),
            ("security check", security_check_pattern),
            ("robot check", None),
            ("access denied", None),
            ("captcha", re.compile(CAPTCHA_WORD_PATTERN)),
            (
                "recaptcha",
                re.compile(CAPTCHA_PROVIDER_PATTERN, re.IGNORECASE),
            ),
        )

        def _match_pattern(text: str, compiled: re.Pattern[str]) -> Optional[re.Match[str]]:
            if compiled.flags & re.IGNORECASE:
                return compiled.search(text)
            return re.search(compiled.pattern, text, compiled.flags | re.IGNORECASE)

        def _match_marker(text: str, marker_text: str) -> Optional[re.Match[str]]:
            return re.search(re.escape(marker_text), text, re.IGNORECASE)

        for marker, pattern in markers:
            for candidate in haystack_parts:
                if not isinstance(candidate, str) or not candidate.strip():
                    continue
                if pattern:
                    match = _match_pattern(candidate, pattern)
                    if match:
                        return CaptchaMatch(marker=marker, match_text=match.group(0))
                else:
                    match = _match_marker(candidate, marker)
                    if match:
                        return CaptchaMatch(marker=marker, match_text=match.group(0))
        if haystack and isinstance(haystack, str):
            lowered = haystack.lower()
            for marker, pattern in markers:
                if pattern:
                    if _match_pattern(lowered, pattern):
                        return CaptchaMatch(marker=marker, match_text=marker)
                elif marker in lowered:
                    return CaptchaMatch(marker=marker, match_text=marker)
        return None

    def _captcha_context(
        self,
        marker: str,
        match_text: str | None,
        markdown_text: str | None,
        events: List[Any] | None,
        *,
        radius: int = 80,
    ) -> Optional[str]:
        if not marker:
            return None
        needle = (match_text or marker).lower()
        candidates: list[str] = []
        if isinstance(markdown_text, str) and markdown_text.strip():
            candidates.append(markdown_text)
        for evt in events or []:
            if not isinstance(evt, dict):
                continue
            for key in ("title", "reason", "description", "body", "message"):
                val = evt.get(key)
                if isinstance(val, str) and val.strip():
                    candidates.append(val)
        for text in candidates:
            lowered = text.lower()
            idx = lowered.find(needle)
            if idx == -1:
                continue
            start = max(idx - radius, 0)
            end = min(idx + radius, len(text))
            return text[start:end].strip()
        return None

    def _emit_captcha_warn(
        self,
        *,
        url: str,
        marker: str,
        match_text: str | None,
        attempt: int,
        proxy: Optional[str],
        markdown_text: str | None,
        events: List[Any] | None,
    ) -> None:
        context = self._captcha_context(marker, match_text, markdown_text, events)
        payload = {
            "event": "scrape.captcha_detected",
            "level": "warn",
            "siteUrl": url,
            "data": {
                "provider": self.provider,
                "marker": marker,
                "matchText": match_text,
                "attempt": attempt,
                "proxy": proxy,
                "context": context,
            },
        }
        try:
            telemetry.emit_posthog_log(payload)
        except Exception:
            # best-effort logging; never fail the scrape on telemetry errors
            pass
        try:
            telemetry.emit_posthog_exception(
                CaptchaRetriesExceededError(
                    url=url,
                    marker=marker,
                    match_text=match_text,
                    attempts=attempt,
                    proxy=proxy,
                    markdown=markdown_text,
                    events=events,
                ),
                properties={
                    "event": "scrape.captcha_failed",
                    "siteUrl": url,
                    "provider": self.provider,
                    "captchaMarker": marker,
                    "captchaMatchText": match_text,
                    "captchaContext": context,
                    "attempt": attempt,
                    "proxy": proxy,
                },
            )
        except Exception:
            # best-effort logging; never fail the scrape on telemetry errors
            pass

    def _emit_scrape_log(
        self,
        *,
        event: str,
        level: str,
        site_url: str,
        api_url: str | None = None,
        data: Dict[str, Any] | None = None,
        exc: BaseException | None = None,
        capture_exception: bool = False,
    ) -> None:
        payload: Dict[str, Any] = {
            "event": event,
            "level": level,
            "siteUrl": site_url,
            "data": {
                "provider": self.provider,
            },
        }
        if api_url:
            payload["data"]["apiUrl"] = api_url
        if data:
            payload["data"].update(data)
        if exc:
            payload["data"]["error"] = str(exc)

        try:
            telemetry.emit_posthog_log(payload)
        except Exception:
            # best-effort logging; never fail the scrape on telemetry errors
            pass

        if exc and capture_exception:
            try:
                telemetry.emit_posthog_exception(
                    exc,
                    properties={
                        "event": event,
                        "siteUrl": site_url,
                        "apiUrl": api_url,
                        "provider": self.provider,
                    },
                )
            except Exception:
                pass

    def _extract_structured_job_posting(self, events: List[Any]) -> Optional[Dict[str, Any]]:
        """Best-effort extraction of JSON-LD JobPosting data from raw HTML events."""

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

        script_pattern = re.compile(JSON_LD_SCRIPT_PATTERN, flags=re.IGNORECASE | re.DOTALL)

        logged_parse_error = False
        for text in (t for t in gather_strings(events) if isinstance(t, str) and t.strip()):
            if "<script" not in text.lower():
                continue
            for match in script_pattern.finditer(text):
                payload_raw = html.unescape(match.group("payload").strip())
                parsed = self._try_parse_json(payload_raw)
                if parsed is None:
                    if not logged_parse_error and "jobposting" in payload_raw.lower():
                        logged_parse_error = True
                        try:
                            telemetry.emit_posthog_exception(
                                ValueError("Failed to parse JSON-LD JobPosting payload"),
                                properties={
                                    "event": "scrape.structured_data.parse_failed",
                                    "provider": self.provider,
                                    "payloadSnippet": payload_raw[:500],
                                },
                            )
                        except Exception:
                            pass
                    continue
                found = _find_job_posting(parsed)
                if found:
                    return found

        return None

    def _should_use_structured_description(self, markdown: str) -> bool:
        if not markdown or not markdown.strip():
            return True
        cleaned = markdown.strip()
        if len(cleaned) < 200:
            return True
        lower = cleaned.lower()
        chrome_hits = sum(1 for marker in STRUCTURED_DESCRIPTION_CHROME_MARKERS if marker in lower)
        if chrome_hits >= 2:
            return True
        return not any(marker in lower for marker in JOB_DETAIL_MARKERS)

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
        def _looks_like_sentence_title(value: str) -> bool:
            stripped = value.strip()
            if not stripped:
                return True
            if stripped.startswith(("*", "-")) or ORDERED_LIST_LINE_RE.match(stripped):
                return True
            lowered = stripped.lower()
            if lowered.endswith((".", "!", "?")):
                return True
            if lowered.startswith(("as the ", "as a ", "as an ")):
                return True
            if re.search(r"\b(?:you|your|we|our|will|you'll|you\u2019ll|join us)\b", lowered):
                return True
            if len(lowered.split()) > 12:
                return True
            return False

        def _looks_like_metadata_line(value: str) -> bool:
            stripped = value.strip()
            if not stripped:
                return True
            lowered = stripped.lower()
            if lowered in {"remote", "hybrid", "onsite", "on-site"}:
                return True
            if lowered in {"intern", "junior", "mid", "mid-level", "senior", "staff", "principal", "lead", "manager", "director", "vp", "cto"}:
                return True
            if _normalize_country_label(stripped):
                return True
            if _SALARY_RE.search(stripped) or _SALARY_K_RE.search(stripped):
                return True
            if _SALARY_RANGE_LABEL_RE.search(stripped) or _SALARY_BETWEEN_RE.search(stripped):
                return True
            if re.search(r"[$£€]\s*\d", stripped):
                return True
            if re.fullmatch(r"\d+\s+words?", lowered):
                return True
            if "posted" in lowered and ("ago" in lowered or re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", lowered)):
                return True
            return False

        def _looks_like_job_title(value: str) -> bool:
            lowered = value.strip().lower()
            if not lowered:
                return False
            if re.fullmatch(r"\d+\s+words?", lowered):
                return False
            if _looks_like_metadata_line(value):
                return False
            if len(lowered.split()) >= 2:
                return True
            return any(keyword in lowered for keyword in JOB_TITLE_KEYWORDS)

        def _maybe_select_title(value: Any, *, fallback: Optional[str]) -> tuple[Optional[str], Optional[str]]:
            if not isinstance(value, str):
                return None, fallback
            candidate = value.strip()
            if not candidate:
                return None, fallback
            if self._is_placeholder_title(candidate):
                return None, fallback
            if _looks_like_sentence_title(candidate):
                return None, fallback or candidate
            if not _looks_like_job_title(candidate):
                return None, fallback
            return candidate, fallback

        fallback: Optional[str] = None
        for evt in events:
            if not isinstance(evt, dict):
                continue
            for key in ("title", "job_title", "heading"):
                selected, fallback = _maybe_select_title(evt.get(key), fallback=fallback)
                if selected:
                    return selected
            metadata = evt.get("metadata")
            if isinstance(metadata, dict):
                for meta_key in ("commonmark", "markdown", "raw", "html", "text"):
                    meta_val = metadata.get(meta_key)
                    if isinstance(meta_val, dict):
                        for key in ("title", "job_title", "heading"):
                            selected, fallback = _maybe_select_title(meta_val.get(key), fallback=fallback)
                            if selected:
                                return selected
                    else:
                        selected, fallback = _maybe_select_title(meta_val, fallback=fallback)
                        if selected:
                            return selected
                for key in ("title", "job_title", "heading"):
                    selected, fallback = _maybe_select_title(metadata.get(key), fallback=fallback)
                    if selected:
                        return selected
        return fallback

    def _title_from_markdown(self, markdown: str) -> Optional[str]:
        application_headers = {
            "application",
            "job application",
            "application form",
        }
        def _looks_like_list_item(value: str) -> bool:
            stripped = value.strip()
            if stripped.startswith(("*", "-")):
                return True
            return bool(ORDERED_LIST_LINE_RE.match(stripped))

        def _looks_like_qualification_line(value: str) -> bool:
            stripped = value.strip()
            if not stripped:
                return False
            lowered = stripped.lower()
            if lowered in {
                "qualifications",
                "requirements",
                "minimum qualifications",
                "preferred qualifications",
                "minimum requirements",
                "preferred requirements",
            }:
                return True
            if re.search(r"\b\d+\+?\s+years?\s+of\s+experience\b", lowered):
                return True
            if "years of experience" in lowered:
                return True
            if "degree" in lowered and any(
                token in lowered
                for token in (
                    "bachelor",
                    "master",
                    "phd",
                    "ph.d",
                    "mba",
                    "m.s",
                    "ms",
                    "b.s",
                    "bs",
                    "b.a",
                    "ba",
                )
            ):
                return True
            return False

        def _looks_like_metadata_line(value: str) -> bool:
            stripped = value.strip()
            if not stripped:
                return True
            lowered = stripped.lower()
            if lowered in {"remote", "hybrid", "onsite", "on-site"}:
                return True
            if lowered in {"intern", "junior", "mid", "mid-level", "senior", "staff", "principal", "lead", "manager", "director", "vp", "cto"}:
                return True
            if _looks_like_qualification_line(stripped):
                return True
            if _normalize_country_label(stripped):
                return True
            if _SALARY_RE.search(stripped) or _SALARY_K_RE.search(stripped):
                return True
            if _SALARY_RANGE_LABEL_RE.search(stripped) or _SALARY_BETWEEN_RE.search(stripped):
                return True
            if re.search(r"[$£€]\s*\d", stripped):
                return True
            if re.fullmatch(r"\d+\s+words?", lowered):
                return True
            if "posted" in lowered and ("ago" in lowered or re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", lowered)):
                return True
            return False

        def _looks_like_job_title(value: str) -> bool:
            lowered = value.strip().lower()
            if not lowered:
                return False
            if re.fullmatch(r"\d+\s+words?", lowered):
                return False
            if _looks_like_metadata_line(value):
                return False
            if lowered in {"location", "locations", "link", "links"}:
                return False
            if lowered.startswith(("other locations", "other location", "other links")):
                return False
            if len(lowered.split()) >= 2:
                return True
            return any(keyword in lowered for keyword in JOB_TITLE_KEYWORDS)

        def _looks_like_sentence(value: str) -> bool:
            lowered = value.strip().lower()
            if not lowered:
                return False
            if lowered.endswith((".", "!", "?")):
                return True
            if lowered.startswith(("as the ", "as a ", "as an ")):
                return True
            if re.search(r"\b(?:you|your|we|our|will|you'll|you\u2019ll|join us)\b", lowered):
                return True
            if len(lowered.split()) > 12:
                return True
            return False

        def _looks_like_strong_title(value: str) -> bool:
            if not _looks_like_job_title(value):
                return False
            return not _looks_like_sentence(value)

        def _looks_like_skip_line(value: str) -> bool:
            lowered = value.strip().lower()
            if lowered in {"back", "apply", "apply now", "direct apply", "apply with ai"}:
                return True
            if lowered.startswith(("http://", "https://")):
                return True
            if _looks_like_qualification_line(value):
                return True
            if _looks_like_list_item(value):
                return True
            return self._is_placeholder_title(value)

        def _title_from_description_section(text: str) -> Optional[str]:
            description_headers = {
                "description",
                "job description",
                "role description",
                "position description",
            }
            description_header = False
            scanned_lines = 0
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                normalized = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", stripped).strip()
                normalized = normalized.rstrip(":").lower()
                if normalized in description_headers or re.fullmatch(
                    r"(?:job\s+|role\s+|position\s+)?description\b(?:\s+\d+\s+words?)?",
                    normalized,
                ):
                    description_header = True
                    scanned_lines = 0
                    continue
                if not description_header:
                    continue
                if _looks_like_skip_line(stripped) or _looks_like_metadata_line(stripped):
                    continue
                if stripped.startswith(("{", "[")):
                    continue
                scanned_lines += 1
                if scanned_lines <= 3 and _looks_like_strong_title(stripped):
                    return stripped
                if scanned_lines >= 3:
                    break
            return None

        description_title = _title_from_description_section(markdown)
        if description_title:
            return description_title

        application_header = False
        if "```" in markdown:
            fenced_match = re.search(
                CODE_FENCE_JSON_OBJECT_PATTERN,
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
                            candidate = val.strip()
                            if candidate.lower() in application_headers:
                                application_header = True
                                continue
                            if not _looks_like_skip_line(candidate) and not _looks_like_metadata_line(candidate):
                                if _looks_like_sentence(candidate):
                                    continue
                                if application_header and not _looks_like_job_title(candidate):
                                    continue
                                return candidate
        fallback_title: Optional[str] = None
        for line in markdown.splitlines():
            if not line.strip():
                continue
            normalized_line = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", line).strip()
            if normalized_line.lower() in application_headers:
                application_header = True
                continue
            heading_match = re.match(MARKDOWN_HEADING_PATTERN, line.strip())
            if heading_match:
                candidate = heading_match.group(1).strip()
                if candidate and not _looks_like_skip_line(candidate) and not _looks_like_metadata_line(candidate):
                    if _looks_like_sentence(candidate):
                        continue
                    if application_header and not _looks_like_job_title(candidate):
                        continue
                    if _looks_like_job_title(candidate):
                        return candidate
                    if fallback_title is None:
                        fallback_title = candidate
                continue
            stripped = line.strip()
            if _looks_like_skip_line(stripped) or _looks_like_metadata_line(stripped):
                continue
            if stripped.startswith(("{", "[")):
                continue
            bar_match = _TITLE_IN_BAR_RE.match(stripped) or _TITLE_BAR_RE.match(stripped)
            if bar_match:
                candidate = bar_match.group("title").strip()
                if candidate and not _looks_like_skip_line(candidate) and not _looks_like_metadata_line(candidate):
                    if _looks_like_sentence(candidate):
                        continue
                    if application_header and not _looks_like_job_title(candidate):
                        continue
                    if _looks_like_job_title(candidate):
                        return candidate
                    if fallback_title is None:
                        fallback_title = candidate
            if len(stripped) > 6:
                if _looks_like_sentence(stripped):
                    continue
                if application_header and not _looks_like_job_title(stripped):
                    continue
                if _looks_like_job_title(stripped):
                    return stripped
                if fallback_title is None:
                    fallback_title = stripped
        return fallback_title

    def _title_with_required_keyword(self, markdown: str) -> Optional[str]:
        """Find the first markdown line that satisfies required title keywords."""

        if not markdown:
            return None

        def _looks_like_list_item(value: str) -> bool:
            stripped = value.strip()
            if stripped.startswith(("*", "-")):
                return True
            return bool(ORDERED_LIST_LINE_RE.match(stripped))

        def _looks_like_qualification_line(value: str) -> bool:
            stripped = value.strip()
            if not stripped:
                return False
            lowered = stripped.lower()
            if lowered in {
                "qualifications",
                "requirements",
                "minimum qualifications",
                "preferred qualifications",
                "minimum requirements",
                "preferred requirements",
            }:
                return True
            if re.search(r"\b\d+\+?\s+years?\s+of\s+experience\b", lowered):
                return True
            if "years of experience" in lowered:
                return True
            if "degree" in lowered and any(
                token in lowered
                for token in (
                    "bachelor",
                    "master",
                    "phd",
                    "ph.d",
                    "mba",
                    "m.s",
                    "ms",
                    "b.s",
                    "bs",
                    "b.a",
                    "ba",
                )
            ):
                return True
            return False

        def _looks_like_metadata_line(value: str) -> bool:
            stripped = value.strip()
            if not stripped:
                return True
            lowered = stripped.lower()
            if lowered in {"remote", "hybrid", "onsite", "on-site"}:
                return True
            if lowered in {"intern", "junior", "mid", "mid-level", "senior", "staff", "principal", "lead", "manager", "director", "vp", "cto"}:
                return True
            if _looks_like_qualification_line(stripped):
                return True
            if _normalize_country_label(stripped):
                return True
            if _SALARY_RE.search(stripped) or _SALARY_K_RE.search(stripped):
                return True
            if _SALARY_RANGE_LABEL_RE.search(stripped) or _SALARY_BETWEEN_RE.search(stripped):
                return True
            if re.search(r"[$£€]\s*\d", stripped):
                return True
            if re.fullmatch(r"\d+\s+words?", lowered):
                return True
            if "posted" in lowered and ("ago" in lowered or re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", lowered)):
                return True
            return False

        def _looks_like_sentence(value: str) -> bool:
            lowered = value.strip().lower()
            if not lowered:
                return False
            if lowered.endswith((".", "!", "?")):
                return True
            if lowered.startswith(("as the ", "as a ", "as an ")):
                return True
            if re.search(r"\b(?:you|your|we|our|will|you'll|you\u2019ll|join us)\b", lowered):
                return True
            if len(lowered.split()) > 12:
                return True
            return False

        for raw_line in markdown.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(("http://", "https://")):
                continue
            if _looks_like_list_item(line) or _looks_like_metadata_line(line):
                continue
            # Strip heading markers before evaluating keywords so `# Title` works.
            line = re.sub(MARKDOWN_HEADING_PREFIX_PATTERN, "", line).strip()
            if not line or self._is_placeholder_title(line):
                continue
            bar_match = _TITLE_IN_BAR_RE.match(line) or _TITLE_BAR_RE.match(line)
            if bar_match:
                line = bar_match.group("title").strip()
            if not line or _looks_like_sentence(line):
                continue
            if title_matches_required_keywords(line):
                if self._title_is_metadata_value(markdown, line):
                    continue
                return line.strip()

        return None

    def _title_from_url(self, url: str) -> str:
        slug = url.split("/")[-1] if "/" in url else url
        slug = slug or url
        cleaned = re.sub(SLUG_SEPARATOR_PATTERN, " ", slug).strip()
        cleaned = re.sub(QUERY_STRING_PATTERN, "", cleaned)
        if not cleaned:
            return "Untitled"
        if self._is_placeholder_title(cleaned):
            return "Untitled"
        return cleaned.title()

    def _is_placeholder_title(self, title: str) -> bool:
        placeholders = {
            "page_title",
            "title",
            "job_title",
            "untitled",
            "unknown",
            "jobs",
            "job",
            "careers",
            "career",
            "open roles",
            "openings",
            "job description",
            "description",
            "company overview",
            "stay in the loop",
        }
        stripped = title.strip().lower().rstrip(":.!?")
        if stripped in placeholders:
            return True
        try:
            parsed = urlparse(stripped)
        except Exception:
            parsed = None
        if parsed and parsed.scheme in {"http", "https"} and parsed.netloc:
            return True
        if UUID_LIKE_RE.search(stripped):
            return True
        if re.fullmatch(r"[0-9a-f]{12,}", stripped):
            return True
        # Reject IDs masquerading as titles (e.g., numeric requisition IDs).
        return bool(re.fullmatch(MIN_THREE_DIGIT_PATTERN, stripped))

    def _title_is_metadata_value(self, markdown: str, title: str) -> bool:
        if not markdown or not title:
            return False
        _, metadata_block = split_description_metadata(markdown)
        if not metadata_block:
            return False
        target = title.strip().lower()
        expect_value = False
        for line in metadata_block.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if ":" in stripped:
                label, value = stripped.split(":", 1)
                if _normalize_section_heading(label) in _METADATA_LABEL_KEYS:
                    if value.strip().lower() == target:
                        return True
            normalized = _normalize_section_heading(stripped)
            if normalized in _METADATA_LABEL_KEYS:
                expect_value = True
                continue
            if expect_value:
                value = re.sub(r"^[#*\-\u2022]+\s*", "", stripped).strip().lower()
                if value == target:
                    return True
                expect_value = False
        return False

    def _regex_extract_job_urls(self, text: str) -> List[str]:
        """
        Fallback extraction for Greenhouse listings when structured parsing fails.

        Looks for greenhouse job URLs and returns a deduped list.
        """

        if not text:
            return []

        def _is_greenhouse_job_url(url: str) -> bool:
            try:
                parsed = urlparse(url)
            except Exception:
                return False
            if "gh_jid" in (parsed.query or ""):
                return True
            return bool(re.search(JOB_ID_PATH_PATTERN, parsed.path or ""))

        # Capture both boards.greenhouse.io and api.greenhouse.io absolute URLs
        pattern = re.compile(GREENHOUSE_URL_PATTERN)
        seen: set[str] = set()
        urls: list[str] = []
        for match in pattern.findall(text):
            if "jobs" not in match:
                continue
            url = match.strip()
            if not _is_greenhouse_job_url(url):
                continue
            if url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def _regex_extract_job_urls_from_events(self, raw_text: str, raw_events: list[Any]) -> List[str]:
        urls = self._regex_extract_job_urls(raw_text)
        if not raw_events:
            return urls
        seen: set[str] = set(urls)
        for candidate in gather_strings(raw_events):
            for url in self._regex_extract_job_urls(candidate):
                if url in seen:
                    continue
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
        if job_urls:
            job_urls = handler.filter_job_urls(job_urls)
        pagination_urls = handler.get_pagination_urls_from_json(payload, request_url)
        if pagination_urls:
            job_urls.extend(pagination_urls)
            job_urls = handler.filter_job_urls(job_urls)
        else:
            if handler.name == "netflix":
                count = payload.get("count") if isinstance(payload, dict) else None
                positions = payload.get("positions") if isinstance(payload, dict) else None
                page_size = len(positions) if isinstance(positions, list) else 0
                if isinstance(count, int) and count > 0 and page_size > 0 and count > page_size:
                    logger.warning(
                        "Site API fetch missing pagination handler=%s url=%s count=%s page_size=%s; falling back to rendered scrape",
                        handler.name,
                        api_url,
                        count,
                        page_size,
                    )
                    return None
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
                positions = node.get("positions")
                if isinstance(positions, list) and any(isinstance(position, dict) for position in positions):
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

        def _scan_json_candidates(text: str) -> Iterable[Any]:
            decoder = json.JSONDecoder()
            for match in re.finditer(r"[{[]", text):
                idx = match.start()
                try:
                    parsed, _ = decoder.raw_decode(text[idx:])
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, str):
                    try:
                        parsed = json.loads(parsed)
                    except Exception:
                        pass
                yield parsed

        def _parse_json_text(text: str) -> Any | None:
            text = _strip_code_fences(text)
            text = re.sub(INVALID_JSON_ESCAPE_PATTERN, "", text)
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
            match = PRE_PATTERN.search(text)
            content = match.group("content") if match else text
            if not content:
                return None
            content = html.unescape(content).strip()
            if not content:
                return None
            candidate = content
            if not candidate.lstrip().startswith("{"):
                brace_match = re.search(JSON_OBJECT_PATTERN, candidate, flags=re.DOTALL)
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

        for text in (t for t in gather_strings(value) if isinstance(t, str) and t.strip()):
            parsed = _parse_json_text(text)
            if parsed is not None:
                found = _find_jobs_payload(parsed)
                if found:
                    return found
            html_found = _extract_json_from_html(text)
            if html_found:
                return html_found
            if "{" in text or "[" in text:
                cleaned = _strip_code_fences(text)
                cleaned = re.sub(INVALID_JSON_ESCAPE_PATTERN, "", cleaned)
                for candidate in _scan_json_candidates(cleaned):
                    found = _find_jobs_payload(candidate)
                    if found:
                        return found

        return None

    def _merge_json_fragments(self, value: Any) -> str | None:
        fragments: list[str] = []

        def _add_fragment(candidate: Any) -> None:
            if isinstance(candidate, (bytes, bytearray)):
                candidate = candidate.decode("utf-8", errors="replace")
            if isinstance(candidate, str):
                stripped = candidate.strip()
                if stripped:
                    fragments.append(stripped)

        if isinstance(value, list):
            for event in value:
                if isinstance(event, dict):
                    content = event.get("content", event)
                    if isinstance(content, dict):
                        for key in ("raw", "raw_html", "html", "text", "body", "result"):
                            _add_fragment(content.get(key))
                    else:
                        _add_fragment(content)
                else:
                    _add_fragment(event)
        else:
            _add_fragment(value)

        if len(fragments) < 2:
            return None
        merged = "".join(fragments)
        if not any(ch in merged for ch in ("{", "}", "[", "]")):
            return None
        if "jobs" not in merged and "positions" not in merged:
            return None
        return merged

    def _payload_has_job_urls(self, payload: Dict[str, Any]) -> bool:
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            jobs = None
        url_keys = (
            "jobUrl",
            "applyUrl",
            "jobPostingUrl",
            "postingUrl",
            "url",
            "absolute_url",
            "absoluteUrl",
        )
        if isinstance(jobs, list):
            for job in jobs:
                if not isinstance(job, dict):
                    continue
                for key in url_keys:
                    value = job.get(key)
                    if isinstance(value, str) and value.strip():
                        return True
        positions = payload.get("positions")
        if isinstance(positions, list):
            for position in positions:
                if not isinstance(position, dict):
                    continue
                url = position.get("canonicalPositionUrl")
                if isinstance(url, str) and url.strip():
                    return True
        return False

    def _payload_looks_like_job_detail(self, payload: Dict[str, Any]) -> bool:
        singleview = payload.get("singleview")
        if isinstance(singleview, bool):
            if singleview:
                return True
        if isinstance(singleview, int) and singleview == 1:
            return True
        positions = payload.get("positions")
        if isinstance(positions, list) and len(positions) == 1:
            if payload.get("pid"):
                return True
            count = payload.get("count")
            if isinstance(count, int) and count == 1:
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

    def _extract_listing_job_urls_from_events(
        self,
        handler: BaseSiteHandler,
        raw_events: List[Any],
        markdown_text: str,
        *,
        base_url: str | None = None,
    ) -> List[str]:
        candidates: List[str] = []
        for text in gather_strings(raw_events):
            if isinstance(text, str) and text.strip():
                candidates.append(text)
        if isinstance(markdown_text, str) and markdown_text.strip():
            candidates.append(markdown_text)

        urls: List[str] = []
        for text in candidates:
            if "<" not in text or ">" not in text:
                continue
            urls = handler.get_links_from_raw_html(text)
            if urls:
                break

        if not urls:
            for text in candidates:
                if not text.strip():
                    continue
                urls = handler.get_links_from_markdown(text)
                if urls:
                    break

        if not urls:
            return []

        filtered = handler.filter_job_urls([u for u in urls if isinstance(u, str) and u.strip()])
        normalized: List[str] = []
        seen: set[str] = set()
        for url in filtered:
            normalized_url = normalize_url(url, base_url=base_url)
            if not normalized_url:
                continue
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            normalized.append(normalized_url)
        return normalized

    def _extract_listing_links_from_html(
        self,
        handler: BaseSiteHandler,
        raw_events: List[Any],
        markdown_text: str,
    ) -> List[str]:
        candidates: List[str] = []
        if isinstance(markdown_text, str) and "<" in markdown_text and ">" in markdown_text:
            candidates.append(markdown_text)
        for text in gather_strings(raw_events):
            if not isinstance(text, str):
                continue
            if "<" not in text or ">" not in text:
                continue
            candidates.append(text)
        if not candidates:
            return []
        for candidate in candidates:
            links = handler.get_links_from_raw_html(candidate)
            if links:
                return links
        return []

    def _normalize_job(
        self,
        url: str,
        markdown: str,
        events: List[Any],
        started_at: int,
        *,
        require_keywords: bool = True,
    ) -> Dict[str, Any] | None:
        handler = self._get_site_handler(url)
        parsed_title = None
        parsed_markdown = markdown or ""
        if not parsed_markdown.strip():
            extracted = self._extract_markdown(events)
            if isinstance(extracted, str) and extracted.strip():
                parsed_markdown = extracted
        raw_markdown = parsed_markdown
        if handler:
            normalized_markdown, normalized_title = handler.normalize_markdown(parsed_markdown)
            if isinstance(normalized_markdown, str) and normalized_markdown.strip():
                parsed_markdown = normalized_markdown
            if normalized_title:
                parsed_title = normalized_title

        listing_payload = self._extract_json_payload(events) or self._extract_json_payload(parsed_markdown)
        if (
            isinstance(listing_payload, dict)
            and self._payload_has_job_urls(listing_payload)
            and not self._payload_looks_like_job_detail(listing_payload)
        ):
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
        structured_present = False
        if structured_payload:
            structured_present = True
            raw_title = structured_payload.get("title")
            if isinstance(raw_title, str) and raw_title.strip():
                structured_title = raw_title.strip()
            raw_description = structured_payload.get("description")
            if isinstance(raw_description, str) and raw_description.strip():
                structured_description = raw_description.strip()
            structured_location = self._location_from_job_posting(structured_payload)
        if structured_description and not structured_present:
            structured_present = True

        structured_markdown = None
        if structured_description:
            structured_markdown = self._html_to_markdown(structured_description)
            if structured_markdown.strip():
                if self._should_use_structured_description(parsed_markdown):
                    parsed_markdown = structured_markdown
                else:
                    listing_probe = strip_known_nav_blocks(parsed_markdown or "")
                    listing_probe = _strip_embedded_theme_json(listing_probe)
                    if looks_like_job_listing_page(parsed_title, listing_probe, url):
                        parsed_markdown = structured_markdown

        cleaned_markdown = strip_known_nav_blocks(parsed_markdown or "")
        cleaned_markdown = _strip_embedded_theme_json(cleaned_markdown)
        if len(cleaned_markdown.strip()) < 200:
            meta_description = self._extract_meta_description_from_events(events)
            if meta_description and len(meta_description) > len(cleaned_markdown.strip()):
                cleaned_markdown = meta_description
        cleaned_markdown_len = len(cleaned_markdown.strip())
        hints = parse_markdown_hints(cleaned_markdown)
        hint_title = hints.get("title") if isinstance(hints, dict) else None
        content_title = hint_title or self._title_from_markdown(cleaned_markdown)

        event_title = self._title_from_events(events)
        payload_title = parsed_title or structured_title or event_title
        title_source: str | None = None
        if parsed_title or structured_title:
            title_source = "structured"
        elif event_title:
            title_source = "event"
        from_content = False
        if payload_title and self._is_placeholder_title(payload_title):
            payload_title = None
        if not payload_title:
            payload_title = content_title
            if payload_title:
                title_source = "hint" if hint_title else "markdown"
            from_content = bool(payload_title)
        else:
            from_content = True
            if (
                event_title
                and payload_title == event_title
                and content_title
                and content_title != event_title
                and self._title_is_metadata_value(cleaned_markdown, event_title)
            ):
                payload_title = content_title
                title_source = "hint" if hint_title else "markdown"
        if payload_title and self._is_placeholder_title(payload_title):
            payload_title = None
            title_source = None
            from_content = False

        candidate_title = payload_title or parsed_title
        if handler and handler.is_listing_url(url):
            if structured_present:
                self._emit_scrape_log(
                    event="scrape.normalization.listing_misdetection",
                    level="error",
                    site_url=url,
                    data={
                        "reason": "handler_listing_url",
                        "title": candidate_title or "",
                        "structuredTitle": structured_title,
                        "structuredLocation": structured_location,
                        "markdownLength": len(cleaned_markdown.strip()),
                    },
                    exc=ValueError("Listing heuristic matched on structured job detail page."),
                    capture_exception=True,
                )
            self._last_ignored_job = {
                "url": url,
                "reason": "listing_page",
                "title": candidate_title or self._title_from_url(url),
                "description": cleaned_markdown,
            }
            return None
        if looks_like_job_listing_page(candidate_title, cleaned_markdown, url):
            if structured_present:
                self._emit_scrape_log(
                    event="scrape.normalization.listing_misdetection",
                    level="error",
                    site_url=url,
                    data={
                        "reason": "listing_page_heuristic",
                        "title": candidate_title or "",
                        "structuredTitle": structured_title,
                        "structuredLocation": structured_location,
                        "markdownLength": len(cleaned_markdown.strip()),
                    },
                    exc=ValueError("Listing heuristic matched on structured job detail page."),
                    capture_exception=True,
                )
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
        if isinstance(title, str) and hint_title:
            normalized_title = title.strip()
            if normalized_title.lower().startswith("job application for"):
                title = hint_title
        if isinstance(title, str):
            match = re.match(
                r"^(?:job\s+)?application\s+for\s+(?P<title>.+?)(?:\s+at\s+.+)?$",
                title.strip(),
                flags=re.IGNORECASE,
            )
            if match:
                cleaned = match.group("title").strip()
                if cleaned:
                    title = cleaned
        if isinstance(title, str) and len(title) > MAX_TITLE_CHARS:
            logger.info(
                "SpiderCloud title too long; falling back to URL title url=%s title_len=%s",
                url,
                len(title),
            )
            title = self._title_from_url(url)

        keyword_title = None
        if from_content and not title_matches_required_keywords(title):
            keyword_title = self._title_with_required_keyword(cleaned_markdown)
            can_replace_title = title_source in {None, "hint", "markdown", "event"}
            if keyword_title and can_replace_title:
                title = keyword_title
        if from_content and not title_matches_required_keywords(title):
            logger.info(
                "SpiderCloud dropping job due to missing required keyword url=%s title=%s",
                url,
                title,
            )
            if require_keywords and not keyword_title and cleaned_markdown_len < 200:
                self._last_ignored_job = {
                    "url": url,
                    "reason": "missing_required_keyword",
                    "title": title,
                    "description": cleaned_markdown,
                }
                return None
        company = derive_company_from_url(url) or "Unknown"
        location_hint = hints.get("location") if isinstance(hints, dict) else None
        handler_location = None
        if handler:
            handler_location = handler.extract_location_hint(raw_markdown)
        greenhouse_location = None
        if handler and handler.name == "greenhouse":
            greenhouse_location = self._extract_greenhouse_location_from_events(events)
        location = structured_location or greenhouse_location or handler_location or location_hint
        remote = coerce_remote(hints.get("remote") if isinstance(hints, dict) else None, location or "", f"{title}\n{cleaned_markdown}")
        level = coerce_level(hints.get("level") if isinstance(hints, dict) else None, title)
        description = cleaned_markdown or ""
        if not description.strip() and (raw_markdown or structured_description):
            self._emit_scrape_log(
                event="scrape.normalization.missing_description",
                level="error",
                site_url=url,
                data={
                    "title": title,
                    "markdownLength": len(cleaned_markdown.strip()),
                    "structuredDescription": bool(structured_description),
                },
                exc=ValueError("Normalized job missing description content."),
                capture_exception=True,
            )

        posted_at = started_at
        posted_at_unknown = True
        if handler:
            extractor = getattr(handler, "extract_posted_at", None)
            if callable(extractor):
                raw_posted_at = extractor(listing_payload, url) if listing_payload is not None else None
                if raw_posted_at is None and structured_payload is not None:
                    raw_posted_at = extractor(structured_payload, url)
                if raw_posted_at is None and raw_markdown:
                    candidate_payload = None
                    stripped = raw_markdown.strip()
                    if stripped.startswith("{"):
                        candidate_payload = self._try_parse_json(stripped)
                    if candidate_payload is None and "```" in raw_markdown:
                        fence_match = re.search(
                            CODE_FENCE_JSON_OBJECT_PATTERN,
                            raw_markdown,
                            flags=re.DOTALL | re.IGNORECASE,
                        )
                        if fence_match:
                            candidate_payload = self._try_parse_json(fence_match.group(1))
                    if isinstance(candidate_payload, dict):
                        raw_posted_at = extractor(candidate_payload, url)
                if raw_posted_at is not None:
                    if not isinstance(raw_posted_at, str) or raw_posted_at.strip():
                        posted_at, posted_at_unknown = parse_posted_at_with_unknown(raw_posted_at)

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
            "posted_at": posted_at,
            "posted_at_unknown": posted_at_unknown,
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
        request_url = url
        if handler:
            api_url = handler.get_api_uri(url) if handler.name == "workday" else None
            if api_url and api_url != url:
                request_url = api_url
                logger.debug("SpiderCloud using api_url=%s original_url=%s", request_url, url)
            local_params.update(handler.get_spidercloud_config(request_url))

        try:
            async for chunk in self._iterate_scrape_response(
                scrape_fn(  # type: ignore[call-arg]
                    request_url,
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
                        request_url,
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
            self._emit_scrape_log(
                event="scrape.single_url.failed",
                level="error",
                site_url=url,
                api_url=request_url,
                data={"attempt": attempt},
                exc=exc,
                capture_exception=True,
            )
            raise ApplicationError(f"SpiderCloud scrape failed for {url}: {exc}") from exc

        logger.debug(
            "SpiderCloud stream parsed url=%s events=%s markdown_fragments=%s credit_candidates=%s",
            url,
            len(raw_events),
            len(markdown_parts),
            len(credit_candidates),
        )

        # Detect captcha walls early so the caller can decide whether to retry with a proxy.
        captcha_match = None
        if handler and handler.name == "greenhouse" and handler.is_api_detail_url(url):
            if not self._has_valid_greenhouse_job_payload(raw_events, markdown_parts):
                captcha_match = self._detect_captcha("\n\n".join(markdown_parts), raw_events)
        else:
            captcha_match = self._detect_captcha("\n\n".join(markdown_parts), raw_events)
        if captcha_match:
            logger.warning(
                "SpiderCloud captcha detected url=%s attempt=%s marker=%s",
                url,
                attempt,
                captcha_match.marker,
            )
            raise CaptchaDetectedError(
                captcha_match.marker,
                "\n\n".join(markdown_parts),
                raw_events,
                match_text=captcha_match.match_text,
            )

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
        if handler and handler.is_listing_url(url) and not listing_job_urls:
            try:
                listing_job_urls = self._extract_listing_job_urls_from_events(
                    handler,
                    raw_events,
                    markdown_text,
                    base_url=url,
                )
            except Exception:
                listing_job_urls = []
        if handler and handler.is_listing_url(url) and not listing_job_urls:
            try:
                listing_job_urls = self._extract_listing_job_urls_from_events(
                    handler,
                    raw_events,
                    markdown_text,
                    base_url=url,
                )
            except Exception:
                listing_job_urls = []
        if handler and handler.is_listing_url(url) and not listing_job_urls:
            try:
                listing_job_urls = self._extract_listing_links_from_html(
                    handler,
                    raw_events,
                    markdown_text,
                )
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
        posted_at_by_url: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        def _safe_json_size(payload: Any) -> Optional[int]:
            try:
                return len(json.dumps(payload, ensure_ascii=False))
            except Exception:
                return None

        def _sanitize_urls(values: Iterable[str]) -> list[str]:
            cleaned: list[str] = []
            seen: set[str] = set()
            for value in values:
                if not isinstance(value, str):
                    continue
                raw = value.strip()
                if not raw:
                    continue
                candidate = raw.replace("\\", "/")
                normalized = normalize_url(candidate)
                if normalized:
                    candidate = normalized
                if candidate in seen:
                    continue
                seen.add(candidate)
                cleaned.append(candidate)
            return cleaned

        urls = _sanitize_urls(urls)[:SPIDERCLOUD_BATCH_SIZE]
        posted_at_lookup: Dict[str, int] = {}
        if isinstance(posted_at_by_url, dict):
            for key, value in posted_at_by_url.items():
                if not isinstance(key, str):
                    continue
                if not isinstance(value, (int, float)):
                    continue
                normalized_key = normalize_url(key) or key
                posted_at_lookup[normalized_key] = int(value)
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

        batch_start_monotonic = time.monotonic()
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
        failed_items: List[Dict[str, Any]] = []
        listing_job_urls: List[str] = []
        total_cost_milli_cents = 0.0
        saw_cost_field = False
        max_markdown_len = 0

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
                    last_error: BaseException | None = None
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
                                self._emit_captcha_warn(
                                    url=url,
                                    marker=err.marker,
                                    match_text=getattr(err, "match_text", None),
                                    attempt=attempt,
                                    proxy=proxy,
                                    markdown_text=err.markdown,
                                    events=err.events,
                                )
                                self.deps.log_sync_response(
                                    self.provider,
                                    action="scrape",
                                    url=url,
                                    summary=f"captcha_failed marker={err.marker}",
                                    metadata={"attempts": attempt, "proxy": proxy},
                                )
                                return idx, url, {
                                    "failed": {
                                        "url": url,
                                        "reason": "captcha_failed",
                                        "marker": err.marker,
                                        "attempts": attempt,
                                        "proxy": proxy,
                                    }
                                }
                        except asyncio.TimeoutError as exc:
                            logger.warning(
                                "SpiderCloud scrape timed out url=%s timeout=%s",
                                url,
                                timeout_seconds,
                            )
                            last_error = exc
                            self._emit_scrape_log(
                                event="scrape.single_url.timeout",
                                level="error",
                                site_url=url,
                                data={"timeoutSeconds": timeout_seconds, "attempt": attempt},
                                exc=exc,
                                capture_exception=True,
                            )
                            break
                        except Exception:
                            # Bubble up unexpected errors
                            raise

                    if not result:
                        logger.warning("SpiderCloud skipping url after retries url=%s", url)
                        if last_error is None:
                            self._emit_scrape_log(
                                event="scrape.single_url.no_result",
                                level="error",
                                site_url=url,
                                data={"attempts": attempt},
                                exc=ValueError("SpiderCloud scrape returned empty result"),
                                capture_exception=True,
                            )
                        return idx, url, None

                    if marketing_url and isinstance(result, dict):
                        normalized_block = result.get("normalized")
                        if isinstance(normalized_block, dict) and not normalized_block.get("apply_url"):
                            normalized_block["apply_url"] = marketing_url
                    if posted_at_lookup and isinstance(result, dict):
                        normalized_block = result.get("normalized")
                        if isinstance(normalized_block, dict):
                            override = posted_at_lookup.get(url)
                            if override is None:
                                override = posted_at_lookup.get(normalize_url(url) or url)
                            if override is not None:
                                normalized_block["posted_at"] = int(override)
                                normalized_block["posted_at_unknown"] = False

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
                if result.get("failed"):
                    failed_items.append(result["failed"])
                if result.get("raw"):
                    raw_items.append(result["raw"])
                    markdown_len = len(result.get("raw", {}).get("markdown") or "")
                    if markdown_len > max_markdown_len:
                        max_markdown_len = markdown_len
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
        if failed_items:
            items_block["failed"] = failed_items
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

        raw_payload_bytes = _safe_json_size(scrape_payload)
        trimmed = self._trim_scrape_payload(scrape_payload)
        trimmed_payload_bytes = _safe_json_size(trimmed)
        trimmed_items = trimmed.get("items")
        if isinstance(trimmed_items, dict):
            trimmed_items.setdefault("seedUrls", urls)
            trimmed["items"] = trimmed_items

        cost_cents = (cost_milli_cents / 1000) if isinstance(cost_milli_cents, (int, float)) else None
        cost_usd = (cost_milli_cents / 100000) if isinstance(cost_milli_cents, (int, float)) else None
        cost_mc_display = str(cost_milli_cents) if cost_milli_cents is not None else "n/a"
        cost_cents_display = f"{float(cost_cents):.3f}" if cost_cents is not None else "n/a"
        cost_usd_display = f"{float(cost_usd):.5f}" if cost_usd is not None else "n/a"
        batch_elapsed = time.monotonic() - batch_start_monotonic
        logger.info(
            "SpiderCloud batch payload source=%s urls=%s normalized=%s raw=%s elapsed_s=%.2f raw_bytes=%s trimmed_bytes=%s max_markdown_len=%s",
            source_url,
            len(urls),
            len(normalized_items),
            len(raw_items),
            batch_elapsed,
            raw_payload_bytes,
            trimmed_payload_bytes,
            max_markdown_len,
        )
        if failed_items:
            logger.warning(
                "SpiderCloud batch failures source=%s failed=%s",
                source_url,
                len(failed_items),
            )
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
        force_seed = bool(handler and source_url and handler.is_listing_url(source_url))

        resolved_skip: Optional[List[str]] = skip_urls
        if resolved_skip is None and source_url:
            try:
                resolved_skip = await self.deps.fetch_seen_urls_for_site(source_url, site.get("pattern"))
            except Exception:
                resolved_skip = []

        skip_set = set(resolved_skip or [])
        urls = [u for u in urls if u not in skip_set]
        if (site.get("pattern") and source_url and source_url in skip_set) or force_seed:
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
            self._emit_scrape_log(
                event="scrape.greenhouse_listing.timeout",
                level="error",
                site_url=api_url,
                api_url=api_url,
                data={"timeoutSeconds": timeout_seconds},
                exc=exc,
                capture_exception=True,
            )
            raise ApplicationError(
                f"Failed to fetch Greenhouse board via SpiderCloud (timeout {timeout_seconds}s)."
            ) from exc
        except Exception as exc:  # noqa: BLE001
            logger.error("SpiderCloud greenhouse listing fetch failed url=%s error=%s", api_url, exc)
            self._emit_scrape_log(
                event="scrape.greenhouse_listing.fetch_failed",
                level="error",
                site_url=api_url,
                api_url=api_url,
                exc=exc,
                capture_exception=True,
            )
            raise ApplicationError(f"Failed to fetch Greenhouse board via SpiderCloud: {exc}") from exc

        def _extract_text(value: Any) -> str:
            if isinstance(value, dict):
                best = ""
                for key in ("content", "raw_html", "html", "text", "body", "result"):
                    candidate = value.get(key)
                    if isinstance(candidate, (bytes, bytearray)):
                        candidate = candidate.decode("utf-8", errors="replace")
                    if isinstance(candidate, str) and candidate.strip() and len(candidate) > len(best):
                        best = candidate
                for child in value.values():
                    found = _extract_text(child)
                    if found and len(found) > len(best):
                        best = found
                return best
            if isinstance(value, list):
                best = ""
                for child in value:
                    found = _extract_text(child)
                    if found and len(found) > len(best):
                        best = found
                return best
            if isinstance(value, (bytes, bytearray)):
                return value.decode("utf-8", errors="replace")
            if isinstance(value, str):
                return value
            return ""

        raw_text = ""
        for event in raw_events:
            candidate = _extract_text(event)
            if isinstance(candidate, str) and candidate.strip() and len(candidate) > len(raw_text):
                raw_text = candidate

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
        merged_text = self._merge_json_fragments(raw_events)
        if merged_text and (not raw_text or len(merged_text) > len(raw_text)):
            raw_text = merged_text
        if payload is None and merged_text:
            payload = self._extract_json_payload(merged_text)
        if not raw_text and payload is not None:
            try:
                raw_text = json.dumps(payload, ensure_ascii=False)
            except Exception:
                raw_text = str(payload)

        try:
            board = load_greenhouse_board(payload or raw_text or {})
            # Structured extraction first. Do not filter by required keywords here.
            required_keywords: tuple[str, ...] = ()
            job_urls = extract_greenhouse_job_urls(board, required_keywords=required_keywords)

            # Prefer API detail URLs when we know the board slug and job IDs.
            if slug and job_urls:
                api_urls: list[str] = []
                seen_api: set[str] = set()
                for job in board.jobs:
                    if not job.absolute_url or not title_matches_required_keywords(
                        job.title,
                        keywords=required_keywords,
                    ):
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
                regex_urls = self._regex_extract_job_urls_from_events(raw_text, raw_events)
                if regex_urls:
                    job_urls = regex_urls

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
        except Exception as exc:  # noqa: BLE001
            logger.error("SpiderCloud greenhouse listing parse error url=%s error=%s", api_url, exc)
            regex_urls = self._regex_extract_job_urls_from_events(raw_text, raw_events)
            if regex_urls:
                self._emit_scrape_log(
                    event="scrape.greenhouse_listing.regex_fallback",
                    level="warn",
                    site_url=url or api_url,
                    api_url=api_url,
                    data={
                        "urls": len(regex_urls),
                        "rawLength": len(raw_text) if isinstance(raw_text, str) else 0,
                    },
                    exc=exc,
                    capture_exception=False,
                )
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
            self._emit_scrape_log(
                event="scrape.greenhouse_listing.parse_failed",
                level="error",
                site_url=url or api_url,
                api_url=api_url,
                data={
                    "rawLength": len(raw_text) if isinstance(raw_text, str) else 0,
                    "hasPayload": payload is not None,
                },
                exc=exc,
                capture_exception=True,
            )
            completed_at = int(time.time() * 1000)
            logger.warning(
                "SpiderCloud greenhouse listing parse failed url=%s; returning empty result",
                api_url,
            )
            return {
                "raw": raw_text,
                "job_urls": [],
                "startedAt": started_at,
                "completedAt": completed_at,
                "parseFailed": True,
                "error": str(exc),
            }

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
            "posted_at_by_url": posted_at_by_url if posted_at_by_url else None,
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
        posted_at_by_url = payload.get("posted_at_by_url")
        if not isinstance(posted_at_by_url, dict):
            posted_at_by_url = None
        scrape_payload = await self._scrape_urls_batch(
            urls,
            source_url=source_url,
            pattern=None,
            posted_at_by_url=posted_at_by_url,
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
