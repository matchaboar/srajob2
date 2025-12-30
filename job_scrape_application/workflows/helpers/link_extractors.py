from __future__ import annotations

import html as html_lib
import re
from typing import Any, Iterable, Sequence
from urllib.parse import urljoin, urlparse, urlunparse

from .regex_patterns import URL_PATTERN

def _is_nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def gather_strings(value: Any) -> list[str]:
    results: list[str] = []
    if isinstance(value, str):
        results.append(value)
        return results
    if isinstance(value, dict):
        for child in value.values():
            results.extend(gather_strings(child))
    elif isinstance(value, list):
        for child in value:
            results.extend(gather_strings(child))
    return results


_SLASH_RUN_RE = re.compile(r"/{2,}")
_WRAPPER_PAIRS = {
    '"': '"',
    "'": "'",
    "<": ">",
    "(": ")",
    "[": "]",
}


def strip_wrapping_url(candidate: str) -> str:
    cleaned = candidate.strip()
    while cleaned:
        closing = _WRAPPER_PAIRS.get(cleaned[0])
        if not closing or cleaned[-1] != closing:
            break
        cleaned = cleaned[1:-1].strip()
    return cleaned


def fix_scheme_slashes(candidate: str) -> str:
    lower = candidate.lower()
    if lower.startswith("http:/") and not lower.startswith("http://"):
        return "http://" + candidate[len("http:/") :]
    if lower.startswith("https:/") and not lower.startswith("https://"):
        return "https://" + candidate[len("https:/") :]
    return candidate


def _normalize_http_url(candidate: str) -> str:
    if not candidate.startswith(("http://", "https://")):
        return candidate
    try:
        parsed = urlparse(candidate)
    except Exception:
        return candidate
    if not parsed.scheme or not parsed.netloc:
        return candidate
    path = _SLASH_RUN_RE.sub("/", parsed.path or "")
    if path and path != "/":
        path = path.rstrip("/")
    return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, parsed.query, parsed.fragment))


def normalize_url(url: str | None, *, base_url: str | None = None) -> str | None:
    if not isinstance(url, str):
        return None
    candidate = url.strip()
    if not candidate:
        return None
    candidate = html_lib.unescape(candidate).strip()
    if not candidate:
        return None
    candidate = strip_wrapping_url(candidate)
    if not candidate:
        return None
    candidate = fix_scheme_slashes(candidate)
    candidate = candidate.replace("\\", "/")
    lower = candidate.lower()
    if lower.startswith(("mailto:", "tel:", "javascript:", "#")):
        return None
    if candidate.startswith(("http://", "https://")):
        return _normalize_http_url(candidate)
    if candidate.startswith("//"):
        if not base_url:
            return None
        scheme = urlparse(base_url).scheme or "https"
        return _normalize_http_url(f"{scheme}:{candidate}")
    if base_url:
        joined = urljoin(base_url, candidate)
        return _normalize_http_url(joined)
    return None


def normalize_url_list(urls: Iterable[str], *, base_url: str | None = None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in urls:
        normalized_url = normalize_url(candidate, base_url=base_url)
        if not normalized_url:
            continue
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        normalized.append(normalized_url)
    return normalized


def dedupe_str_list(values: Iterable[str], *, limit: int | None = None) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
        if limit is not None and len(deduped) >= limit:
            break
    return deduped


def extract_links_from_payload(
    value: Any,
    *,
    link_keys: Sequence[str] = ("links", "page_links"),
    collect_all: bool = False,
    scan_strings: bool = False,
) -> list[str]:
    """Extract link lists from nested payloads (e.g., SpiderCloud responses)."""

    links: list[str] = []

    def _walk(node: Any) -> bool:
        if isinstance(node, dict):
            for key in link_keys:
                raw_links = node.get(key)
                if isinstance(raw_links, list):
                    for link in raw_links:
                        if _is_nonempty_string(link):
                            links.append(str(link).strip())
                    if links and not collect_all:
                        return True
            for child in node.values():
                if _walk(child) and not collect_all:
                    return True
        elif isinstance(node, list):
            for child in node:
                if _walk(child) and not collect_all:
                    return True
        return False

    found_structured = _walk(value)

    if scan_strings and (collect_all or not links):
        url_re = re.compile(URL_PATTERN)
        job_hint_tokens = (
            "/job",
            "/jobs",
            "/career",
            "/careers",
            "/position",
            "/positions",
            "/opening",
            "/openings",
            "/opportunity",
            "/opportunities",
            "/role",
            "/roles",
            "/vacancy",
            "/vacancies",
            "gh_jid=",
            "://jobs.",
            "://careers.",
        )
        for text in gather_strings(value):
            if not _is_nonempty_string(text):
                continue
            if "http" not in text:
                continue
            for match in url_re.findall(text):
                if not _is_nonempty_string(match):
                    continue
                cleaned = str(match).strip()
                cleaned = cleaned.rstrip(").,]")
                cleaned = strip_wrapping_url(cleaned)
                if not cleaned:
                    continue
                match_lower = cleaned.lower()
                if not any(token in match_lower for token in job_hint_tokens):
                    continue
                links.append(cleaned)
            if links and not collect_all and not found_structured:
                break
    return links


def extract_job_urls_from_json_payload(value: Any) -> list[str]:
    """Extract job URLs from JSON payloads that include a jobs list."""

    if value is None:
        return []

    def _extract_from_jobs_payload(payload: dict[str, Any]) -> list[str]:
        jobs = payload.get("jobs") if isinstance(payload, dict) else None
        if not isinstance(jobs, list):
            jobs = None
        url_keys = ("jobUrl", "applyUrl", "jobPostingUrl", "postingUrl", "url")
        urls: list[str] = []
        seen_local: set[str] = set()
        if isinstance(jobs, list):
            for job in jobs:
                if not isinstance(job, dict):
                    continue
                for key in url_keys:
                    val = job.get(key)
                    if _is_nonempty_string(val):
                        url = str(val).strip()
                        if url not in seen_local:
                            seen_local.add(url)
                            urls.append(url)
        positions = payload.get("positions") if isinstance(payload, dict) else None
        if isinstance(positions, list):
            for position in positions:
                if not isinstance(position, dict):
                    continue
                url = position.get("canonicalPositionUrl")
                if _is_nonempty_string(url):
                    url = str(url).strip()
                    if url not in seen_local:
                        seen_local.add(url)
                        urls.append(url)
        return urls

    def _walk(node: Any) -> list[str]:
        urls: list[str] = []
        if isinstance(node, dict):
            urls = _extract_from_jobs_payload(node)
            if urls:
                return urls
            for child in node.values():
                urls.extend(_walk(child))
        elif isinstance(node, list):
            for child in node:
                urls.extend(_walk(child))
        return urls

    return _walk(value)
