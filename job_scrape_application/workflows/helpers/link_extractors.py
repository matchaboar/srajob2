from __future__ import annotations

from typing import Any, Iterable, Sequence
from urllib.parse import urljoin, urlparse


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


def normalize_url(url: str | None, *, base_url: str | None = None) -> str | None:
    if not isinstance(url, str):
        return None
    candidate = url.strip()
    if not candidate:
        return None
    lower = candidate.lower()
    if lower.startswith(("mailto:", "tel:", "javascript:", "#")):
        return None
    if candidate.startswith(("http://", "https://")):
        return candidate
    if candidate.startswith("//"):
        if not base_url:
            return None
        scheme = urlparse(base_url).scheme or "https"
        return f"{scheme}:{candidate}"
    if base_url:
        return urljoin(base_url, candidate)
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

    _walk(value)
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
