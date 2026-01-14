"""URL processing utilities for filtering, classification, and extraction."""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from ..helpers.link_extractors import normalize_url, strip_wrapping_url
from ..helpers.regex_patterns import URL_PATTERN
from ..site_handlers import get_site_handler
from ..site_handlers.base import BaseSiteHandler


def _is_base_listing_page(url: str) -> bool:
    """Check if URL is a base listing page (page 1 or no pagination params)."""
    try:
        parsed = urlparse(url)
    except Exception:
        return True
    params = parse_qs(parsed.query)
    for key in ("page", "from", "start", "offset", "joboffset", "jobOffset"):
        raw_val = params.get(key, [None])[0]
        if raw_val is None:
            continue
        try:
            page_val = int(raw_val)
        except Exception:
            continue
        if page_val > 0:
            return False
    return True


def _looks_like_auth_url(url: str) -> bool:
    """Check if URL looks like an authentication page."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    path = (parsed.path or "").lower()
    if not path:
        return False
    segments = [seg for seg in path.split("/") if seg]
    auth_segments = {
        "login",
        "signin",
        "sign-in",
        "sign_in",
        "logout",
        "signout",
        "sign-out",
        "sign_out",
        "register",
        "signup",
        "sign-up",
    }
    return any(seg in auth_segments for seg in segments)


def _is_probable_listing_url(value: str) -> bool:
    """Check if URL is probably a job listing page."""
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    path = (parsed.path or "").lower()
    if not path:
        return False
    if "/api/" in path and any(token in path for token in ("jobs", "positions")):
        return True
    if path.endswith("/jobs") or path.endswith("/jobs/"):
        return True
    if "page=" in (parsed.query or "") and "/jobs" in path and "/jobs/job/" not in path:
        return True
    return False


_URL_PATTERN_CACHE: Dict[str, re.Pattern[str] | None] = {}


def _compile_url_pattern(pattern: str) -> re.Pattern[str] | None:
    """Compile a URL pattern with wildcard support, caching the result."""
    cached = _URL_PATTERN_CACHE.get(pattern)
    if pattern in _URL_PATTERN_CACHE:
        return cached
    escaped = re.escape(pattern)
    with_wildcards = escaped.replace("\\*\\*", ".*").replace("\\*", "[^/]*")
    try:
        compiled = re.compile(f"^{with_wildcards}$")
    except re.error:
        compiled = None
    _URL_PATTERN_CACHE[pattern] = compiled
    return compiled


def _matches_url_pattern(url: str, pattern: str | None) -> bool:
    """Check if URL matches a pattern (supports wildcards)."""
    if not pattern or not isinstance(pattern, str):
        return True
    trimmed = pattern.strip()
    if not trimmed:
        return True
    compiled = _compile_url_pattern(trimmed)
    if compiled:
        return bool(compiled.match(url))
    prefix = trimmed.replace("*", "")
    return url.startswith(prefix)


def _looks_like_job_detail_url(url: str) -> bool:
    """Check if URL looks like a job detail page."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    query = (parsed.query or "").lower()
    if "gh_jid=" in query:
        return True
    path = (parsed.path or "").lower()
    if path.rstrip("/") in {"/job", "/jobs"} and "id=" in query:
        return True
    host = (parsed.hostname or "").lower()
    if not host:
        segments = [seg for seg in path.split("/") if seg]
        if len(segments) >= 2 and segments[-1].isdigit():
            return True
    if host.endswith("confluent.io"):
        return "/jobs/job/" in path
    if host.endswith("ashbyhq.com"):
        segments = [seg for seg in path.split("/") if seg]
        return len(segments) >= 2
    if not any(token in path for token in ("/job", "/jobs", "/career", "/careers", "/position", "/positions")):
        return False
    segments = [seg for seg in path.split("/") if seg]
    for idx, seg in enumerate(segments):
        if seg in {"job", "jobs", "career", "careers", "position", "positions"}:
            return idx + 1 < len(segments)
    return False


def _handler_allows_url(handler: BaseSiteHandler, url: str) -> bool:
    """Check if a handler permits a given URL."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return True
    if handler.matches_url(url):
        return True
    path = (parsed.path or "").lower()
    if handler.name == "github_careers" and host.endswith("github.com"):
        return path.startswith("/collections")
    if handler.name == "netflix" and host.endswith("jobs.netflix.com"):
        return path.startswith("/locations")
    return False


def _classify_filtered_urls(
    extracted_urls: List[str],
    filtered_urls: List[str],
    handler: BaseSiteHandler | None,
    source_url: str | None = None,
) -> Tuple[List[str], List[str]]:
    """
    Classify URLs that were filtered out as either converted/transformed or truly invalid.

    Returns:
        (converted_urls, invalid_urls) where:
        - converted_urls: URLs that were successfully converted/transformed (e.g., job-boards -> boards-api)
        - invalid_urls: URLs that were actually invalid/filtered out
    """
    if not extracted_urls:
        return [], []

    converted_urls: List[str] = []
    invalid_urls: List[str] = []
    filtered_set = set(filtered_urls)

    for url in extracted_urls:
        if url in filtered_set:
            continue

        was_converted = False
        if handler:
            if handler.name == "greenhouse":
                api_url = handler.get_api_uri(url, source_url=source_url)
                if api_url and api_url in filtered_set:
                    was_converted = True
            elif handler.name in {"microsoft_careers", "workday"}:
                api_url = handler.get_api_uri(url)
                if api_url and api_url in filtered_set:
                    was_converted = True

        if was_converted:
            converted_urls.append(url)
        else:
            invalid_urls.append(url)

    return converted_urls, invalid_urls


def _filter_job_urls(
    urls: List[str],
    handler: BaseSiteHandler | None,
    listing_predicate: Callable[[str], bool] | None = None,
    pattern: str | None = None,
    source_url: str | None = None,
) -> List[str]:
    """Filter and normalize job URLs, applying handler-specific transformations."""
    if not urls:
        return []
    if handler and handler.name == "ashby":
        return [url.strip() for url in urls if isinstance(url, str) and url.strip()]
    if handler and handler.name == "greenhouse":
        cleaned: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            stripped = strip_wrapping_url(url)
            if not stripped:
                continue
            normalized = normalize_url(stripped) or stripped.strip()
            if not normalized or normalized in seen:
                continue
            if not _handler_allows_url(handler, normalized):
                continue
            seen.add(normalized)
            cleaned.append(normalized)
        filtered = [handler.get_api_uri(url, source_url=source_url) or url for url in cleaned]
    else:
        base_filtered = BaseSiteHandler.filter_job_urls_basic(urls)
        filtered = handler.filter_job_urls(base_filtered) if handler else base_filtered
        if handler:
            filtered = [url for url in filtered if _handler_allows_url(handler, url)]
    pattern_val = pattern.strip() if isinstance(pattern, str) else None
    if pattern_val:
        normalized_source = normalize_url(source_url) if source_url else None
        filtered = [
            url
            for url in filtered
            if _matches_url_pattern(url, pattern_val)
            or (
                normalized_source
                and _matches_url_pattern(
                    normalize_url(url, base_url=normalized_source) or "",
                    pattern_val,
                )
            )
        ]
    elif not handler:
        filtered = [url for url in filtered if _looks_like_job_detail_url(url)]
    if handler and handler.name in {"microsoft_careers", "workday"}:
        converted: List[str] = []
        seen_converted: set[str] = set()
        for url in filtered:
            api_url = handler.get_api_uri(url)
            candidate = api_url or url
            if candidate in seen_converted:
                continue
            seen_converted.add(candidate)
            converted.append(candidate)
        filtered = converted
    if source_url and any(url.startswith("/") for url in filtered):
        relative_urls = [url for url in filtered if url.startswith("/")]
        absolute_urls = [url for url in filtered if not url.startswith("/")]
        filtered = relative_urls + absolute_urls
    listing_candidates: List[str]
    if handler:
        listing_candidates = [
            url for url in urls if isinstance(url, str) and handler.is_listing_url(url)
        ]
    elif listing_predicate:
        listing_candidates = [
            url for url in urls if isinstance(url, str) and listing_predicate(url)
        ]
    else:
        listing_candidates = []
    if listing_candidates:
        listing_filtered = [
            strip_wrapping_url(url)
            for url in listing_candidates
            if isinstance(url, str) and url.strip()
        ]
    else:
        listing_filtered = []
    combined: List[str] = []
    seen: set[str] = set()
    for url in filtered + listing_filtered:
        if url in seen:
            continue
        seen.add(url)
        combined.append(url)
    return combined
