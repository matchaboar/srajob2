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
_CONTROL_ESCAPE_RE = re.compile(r"(?:\\[nrt]|[\r\n\t])")
_UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")
_WRAPPER_PAIRS = {
    '"': '"',
    "'": "'",
    "<": ">",
    "(": ")",
    "[": "]",
}


def _strip_table_tail(candidate: str) -> str:
    if not candidate:
        return candidate
    if "|" not in candidate:
        return candidate
    if not candidate.startswith(("http://", "https://", "//")):
        return candidate
    if ")|" in candidate or "]|" in candidate:
        return candidate.split("|", 1)[0].strip()
    return candidate


def _truncate_at_unbalanced_paren(candidate: str) -> str:
    """Truncate URL at the first unbalanced closing paren.

    Markdown links like [Title](https://example.com/job/123)Location cause the
    URL regex to capture text after the closing paren. This function finds where
    the paren balance goes negative and truncates there.
    """
    if ")" not in candidate:
        return candidate
    paren_depth = 0
    for i, char in enumerate(candidate):
        if char == "(":
            paren_depth += 1
        elif char == ")":
            paren_depth -= 1
            if paren_depth < 0:
                return candidate[:i]
    return candidate


def _strip_trailing_brackets(candidate: str) -> str:
    cleaned = candidate
    while cleaned:
        if cleaned.endswith(")") and cleaned.count("(") < cleaned.count(")"):
            cleaned = cleaned[:-1]
            continue
        if cleaned.endswith("]") and cleaned.count("[") < cleaned.count("]"):
            cleaned = cleaned[:-1]
            continue
        if cleaned.endswith("[") and cleaned.count("[") > cleaned.count("]"):
            cleaned = cleaned[:-1]
            continue
        if cleaned.endswith("(") and cleaned.count("(") > cleaned.count(")"):
            cleaned = cleaned[:-1]
            continue
        break
    return cleaned.rstrip(".,")


def _strip_html_tail(candidate: str) -> str:
    if "<" not in candidate:
        return candidate
    cut = candidate.find("<")
    if cut <= 0:
        return candidate
    return candidate[:cut].rstrip()


def _strip_embedded_url_quotes(candidate: str) -> str:
    if '"' not in candidate and "'" not in candidate:
        return candidate
    prefix = ""
    working = candidate
    if candidate.startswith("//"):
        prefix = "//"
        working = f"https:{candidate}"
    try:
        parsed = urlparse(working)
    except Exception:
        return candidate
    path = parsed.path or ""
    if '"' not in path and "'" not in path:
        return candidate
    cleaned_path = path.replace('"', "").replace("'", "")
    rebuilt = urlunparse(parsed._replace(path=cleaned_path))
    if prefix:
        return rebuilt.replace("https:", "", 1)
    return rebuilt


def strip_wrapping_url(candidate: str) -> str:
    cleaned = candidate.strip()
    if cleaned:
        match = _CONTROL_ESCAPE_RE.search(cleaned)
        if match:
            cleaned = cleaned[: match.start()].rstrip()
    while cleaned:
        closing = _WRAPPER_PAIRS.get(cleaned[0])
        if not closing or cleaned[-1] != closing:
            break
        cleaned = cleaned[1:-1].strip()
    cleaned = _strip_table_tail(cleaned)
    cleaned = _truncate_at_unbalanced_paren(cleaned)
    cleaned = _strip_trailing_brackets(cleaned)
    cleaned = _strip_html_tail(cleaned)
    return cleaned


def fix_scheme_slashes(candidate: str) -> str:
    lower = candidate.lower()
    if lower.startswith("http:/") and not lower.startswith("http://"):
        return "http://" + candidate[len("http:/") :]
    if lower.startswith("https:/") and not lower.startswith("https://"):
        return "https://" + candidate[len("https:/") :]
    return candidate


def _decode_unicode_escapes(candidate: str) -> str:
    if "\\u" not in candidate:
        return candidate

    def _replace(match: re.Match[str]) -> str:
        codepoint = int(match.group(1), 16)
        if codepoint > 0x7F:
            return match.group(0)
        return chr(codepoint)

    return _UNICODE_ESCAPE_RE.sub(_replace, candidate)


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
    candidate = _decode_unicode_escapes(candidate)
    candidate = candidate.replace("\\/", "/")
    candidate = candidate.replace("\\", "/")
    candidate = fix_scheme_slashes(candidate)
    candidate = _strip_embedded_url_quotes(candidate)
    if candidate.startswith(("http://", "https://")):
        try:
            parsed = urlparse(candidate)
        except Exception:
            parsed = None
        if parsed and not parsed.netloc and base_url:
            path = parsed.path or ""
            if path:
                path = f"/{path.lstrip('/')}"
                return _normalize_http_url(urljoin(base_url, path))
    lower = candidate.lower()
    if lower.startswith(("mailto:", "tel:", "javascript:", "#")):
        return None
    if candidate.startswith(("http://", "https://")):
        return _normalize_http_url(candidate)
    if candidate.startswith("//"):
        if candidate.startswith("///"):
            if not base_url:
                return None
            return _normalize_http_url(urljoin(base_url, f"/{candidate.lstrip('/')}"))
        if not base_url:
            return None
        scheme = urlparse(base_url).scheme or "https"
        normalized = _normalize_http_url(f"{scheme}:{candidate}")
        if normalized and urlparse(normalized).netloc:
            return normalized
        return _normalize_http_url(urljoin(base_url, f"/{candidate.lstrip('/')}"))
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
        url_keys = (
            "jobUrl",
            "applyUrl",
            "jobPostingUrl",
            "postingUrl",
            "url",
            "absolute_url",
            "absoluteUrl",
            "canonical_url",
            "canonicalUrl",
        )
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
