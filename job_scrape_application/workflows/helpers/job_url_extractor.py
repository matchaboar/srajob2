"""Helper functions for extracting job URLs from scrape payloads.

This module contains pure functions that parse scrape payloads to extract job detail URLs.
No external API calls are made - this is deterministic processing only.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import parse_qs, urlparse, urljoin

from ..helpers.link_extractors import (
    extract_job_urls_from_json_payload,
    extract_links_from_payload,
    gather_strings,
    normalize_url,
    strip_wrapping_url,
)
from ..helpers.page_detection import is_invalid_job_url
from ..helpers.regex_patterns import (
    APPLY_WORD_PATTERN,
    ASHBY_JOB_SLUG_PATTERN,
    CODE_FENCE_CONTENT_PATTERN,
    CODE_FENCE_END_PATTERN,
    CODE_FENCE_START_PATTERN,
    CONFLUENT_JOB_PATH_PATTERN,
    DIGIT_PATTERN,
    GREENHOUSE_URL_PATTERN,
    INVALID_JSON_ESCAPE_PATTERN,
    LOCATION_LINE_PATTERN,
    MARKDOWN_LINK_PATTERN,
    TITLE_IN_BAR_PATTERN,
    TITLE_LOCATION_PAREN_PATTERN,
    URL_PATTERN,
)
from ..helpers.url_handling import _strip_ashby_application_url
from ..site_handlers import get_site_handler
from ..site_handlers.base import BaseSiteHandler
from ..activities.url_processing import _is_probable_listing_url
from ...constants import title_matches_required_keywords


def extract_job_urls_from_scrape(scrape: Dict[str, Any]) -> list[str]:
    """Heuristic extraction of job URLs (Greenhouse or plain HTML) from a scrape payload.

    This is a pure function that parses scrape payloads to extract job detail URLs.
    It handles multiple formats including markdown links, HTML anchors, JSON payloads,
    and various job board formats (Greenhouse, Ashby, Confluent, etc.).

    Args:
        scrape: A scrape payload dictionary containing items with raw content.

    Returns:
        A deduplicated list of job URLs extracted from the scrape.
    """
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

    def _looks_like_job_detail_url(url: str) -> bool:
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

    def _looks_like_auth_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").lower()
        segments = [seg for seg in path.split("/") if seg]
        return any(seg in {"login", "logout", "signin", "signout", "register", "signup"} for seg in segments)

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
            return json.loads(cleaned)
        except Exception:
            parsed_items: list[Any] = []
            for line in cleaned.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed_line = json.loads(line)
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
                decoder = json.JSONDecoder()
                parsed, _ = decoder.raw_decode(raw_text, brace_start)
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
                parsed = json.loads(text)
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
                        parsed = json.loads(raw_val)
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
                parsed_json = json.loads(
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
                parsed = json.loads(
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


# Alias for backward compatibility with tests using the underscore-prefixed name
_extract_job_urls_from_scrape = extract_job_urls_from_scrape
