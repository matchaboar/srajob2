from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from .base import BaseSiteHandler, _normalize_relative_posted_label
from ..helpers.regex_patterns import (
    BASE_URL_META_PATTERNS,
    NON_ALNUM_PATTERN,
    WORKDAY_BASE_URL_RE,
    WORKDAY_JOB_DETAIL_PATH_RE,
    WORKDAY_JOB_DETAIL_URL_RE,
    WORKDAY_JOB_TITLE_ANCHOR_RE,
    WORKDAY_PAGE_RANGE_RE,
)

WORKDAY_HOST_SUFFIXES = ("myworkdayjobs.com", "myworkdaysite.com")
_LOCATION_LINE_RE = re.compile(r"^locations?\s*(?P<location>.+)$", flags=re.IGNORECASE)
_STOP_SECTION_RE = re.compile(r"^#+\s*about us\b", flags=re.IGNORECASE)
_IMAGE_LINE_RE = re.compile(r"^!\[[^\]]*\]\([^)]+\)$")
_EMPTY_LINK_RE = re.compile(r"^\[\s*\]\([^)]+\)$")
_WORKDAY_START_DATE_KEYS = ("startDate", "start_date")
_WORKDAY_POSTED_ON_KEYS = ("postedOn", "posted_on")


def _extract_workday_site_id(url: str) -> Optional[str]:
    """Extract the site_id (e.g., 'External_Career') from a Workday URL."""
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    segments = [s for s in (parsed.path or "").split("/") if s]
    # For API URLs like /wday/cxs/tenant/site_id/job/...
    if "cxs" in segments:
        idx = segments.index("cxs")
        if idx + 2 < len(segments):
            return segments[idx + 2]
    # For marketing URLs like /site_id/job/... or just /site_id
    if "job" in segments:
        idx = segments.index("job")
        if idx > 0:
            return segments[idx - 1]
    # For listing URLs without /job/, return the first path segment
    if segments:
        return segments[0]
    return None


def _build_workday_api_url(base_url: str, job_path: str) -> Optional[str]:
    """Build a Workday API URL from a base URL and job path.

    Args:
        base_url: The listing URL (e.g., https://tenant.wd1.myworkdayjobs.com/External_Career)
        job_path: The job path (e.g., /job/Location/Title_ID or job/Location/Title_ID)

    Returns:
        Full API URL or None if unable to build.
    """
    try:
        parsed = urlparse(base_url)
    except Exception:
        return None

    tenant = (parsed.hostname or "").split(".")[0]
    if not tenant:
        return None

    site_id = _extract_workday_site_id(base_url)
    if not site_id:
        return None

    # Normalize job_path - remove leading slash if present
    clean_path = job_path.lstrip("/")
    if not clean_path.startswith("job/"):
        return None

    # Build the API path
    api_path = f"/wday/cxs/{tenant}/{site_id}/{clean_path}"
    return urlunparse(parsed._replace(path=api_path, query="", fragment=""))


class WorkdayHandler(BaseSiteHandler):
    name = "workday"
    site_type = "workday"
    needs_page_links = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith(WORKDAY_HOST_SUFFIXES)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.hostname and not self.matches_url(url):
            return False
        path = (parsed.path or "").lower()
        return bool(path) and "/job/" not in path

    def is_api_detail_url(self, uri: str) -> bool:
        try:
            parsed = urlparse(uri)
        except Exception:
            return False
        return "/wday/cxs/" in (parsed.path or "").lower()

    def get_api_uri(self, uri: str) -> Optional[str]:
        if not self.matches_url(uri):
            return None
        if self.is_api_detail_url(uri):
            return None
        try:
            parsed = urlparse(uri)
        except Exception:
            return None
        segments = [segment for segment in (parsed.path or "").split("/") if segment]
        if "job" not in segments:
            return None
        job_index = segments.index("job")
        if job_index == 0:
            return None
        site_id = segments[job_index - 1]
        tenant = (parsed.hostname or "").split(".")[0]
        if not tenant or not site_id:
            return None
        job_path = "/".join(segments[job_index:])
        api_path = f"/wday/cxs/{tenant}/{site_id}/{job_path}"
        return urlunparse(parsed._replace(path=api_path, query="", fragment=""))

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        if self.is_listing_url(uri):
            return self._apply_page_links_config(
                {
                    "request": "chrome",
                    "return_format": ["raw_html"],
                    "follow_redirects": True,
                    "redirect_policy": "Loose",
                    "external_domains": ["*"],
                    "preserve_host": True,
                    "wait_for": {
                        "selector": {
                            "selector": "a[data-automation-id='jobTitle']",
                            # 90s timeout - Workday pages are JS-heavy and may load slowly
                            "timeout": {"secs": 90, "nanos": 0},
                        },
                        "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
                    },
                }
            )
        return {
            "request": "chrome",
            "return_format": ["raw_html"],
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []

        base_url = self._extract_base_url(html)
        if base_url:
            base_url = html_lib.unescape(base_url)
        urls: List[str] = []
        seen: set[str] = set()

        def _add(url_val: str | None, *, is_path_only: bool = False) -> None:
            if not url_val:
                return
            cleaned = html_lib.unescape(url_val.strip())
            if not cleaned or cleaned in seen:
                return
            if base_url and not cleaned.startswith(("http://", "https://")):
                # For paths starting with /job/, use URL builder to preserve site_id
                if is_path_only or (cleaned.startswith("/job/") or cleaned.startswith("job/")):
                    built = _build_workday_api_url(base_url, cleaned)
                    if built:
                        cleaned = built
                    else:
                        cleaned = urljoin(base_url, cleaned)
                else:
                    cleaned = urljoin(base_url, cleaned)
                if cleaned in seen:
                    return
            seen.add(cleaned)
            urls.append(cleaned)

        for match in WORKDAY_JOB_TITLE_ANCHOR_RE.finditer(html):
            _add(match.group("href"))

        for match in WORKDAY_JOB_DETAIL_URL_RE.findall(html):
            _add(match)

        if base_url:
            for match in WORKDAY_JOB_DETAIL_PATH_RE.findall(html):
                _add(match, is_path_only=True)

        if base_url and self.is_listing_url(base_url):
            urls.extend(self._augment_pagination_urls(base_url, html, urls))

        return self.filter_job_urls(urls)

    def _augment_pagination_urls(self, base_url: str, html: str, urls: List[str]) -> List[str]:
        def _with_offset(url_value: str, offset: int, limit: Optional[int]) -> str:
            parsed = urlparse(url_value)
            params = [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key.lower() not in {"offset", "limit"}
            ]
            if limit is not None:
                params.append(("limit", str(limit)))
            params.append(("offset", str(offset)))
            return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))

        def _query_hint(url_candidates: List[str]) -> Optional[str]:
            for candidate in url_candidates:
                try:
                    parsed = urlparse(candidate)
                except Exception:
                    continue
                if parsed.query:
                    return parsed.query
            return None

        parsed_base = urlparse(base_url)
        base_params = parse_qsl(parsed_base.query, keep_blank_values=True)
        base_offset = None
        base_limit = None
        for key, value in base_params:
            if key.lower() == "offset":
                try:
                    base_offset = int(value)
                except Exception:
                    base_offset = None
            if key.lower() == "limit":
                try:
                    base_limit = int(value)
                except Exception:
                    base_limit = None

        def _infer_page_data() -> tuple[int | None, int | None, int | None]:
            match = WORKDAY_PAGE_RANGE_RE.search(html)
            if match:
                start = int(match.group("start"))
                end = int(match.group("end"))
                total = int(match.group("total"))
                page_size = max(end - start + 1, 1)
                current_offset = max(start - 1, 0)
                return current_offset, page_size, total
            return base_offset or 0, base_limit, None

        current_offset, page_size, total = _infer_page_data()
        if base_offset is not None:
            current_offset = base_offset
        if base_limit is not None:
            page_size = base_limit

        if page_size is None:
            page_size = 20

        working_base = base_url
        if not parsed_base.query:
            hinted_query = _query_hint(urls)
            if hinted_query:
                working_base = urlunparse(parsed_base._replace(query=hinted_query))

        augmented: List[str] = []
        if current_offset is None:
            current_offset = 0

        max_pages = 10
        offsets: List[int] = []
        for idx in range(max_pages):
            offset = current_offset + (idx * page_size)
            if total is not None and offset >= total:
                break
            offsets.append(offset)

        for offset in offsets:
            token = f"offset={offset}"
            if any(token in url.lower() for url in urls + augmented):
                continue
            augmented.append(_with_offset(working_base, offset, page_size))

        return augmented

    def extract_company(self, payload: Any, url: str | None = None) -> Optional[str]:
        if not url:
            return None
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        segments = [segment for segment in (parsed.path or "").split("/") if segment]
        candidate = None
        if "recruiting" in segments:
            idx = segments.index("recruiting")
            if idx + 1 < len(segments):
                candidate = segments[idx + 1]
        elif "cxs" in segments:
            idx = segments.index("cxs")
            if idx + 1 < len(segments):
                candidate = segments[idx + 1]
        if not candidate:
            return None
        cleaned = re.sub(NON_ALNUM_PATTERN, " ", candidate).strip()
        return cleaned.title() if cleaned else None

    def extract_posted_at(self, payload: Any, url: str | None = None) -> Any | None:
        start_date = self._extract_workday_value(payload, _WORKDAY_START_DATE_KEYS)
        if start_date is not None:
            return start_date
        posted_on = self._extract_workday_value(payload, _WORKDAY_POSTED_ON_KEYS)
        if posted_on is not None:
            if isinstance(posted_on, str):
                return _normalize_relative_posted_label(posted_on)
            return posted_on
        return super().extract_posted_at(payload, url)

    def _extract_workday_value(self, payload: Any, keys: tuple[str, ...]) -> Any | None:
        if isinstance(payload, dict):
            for key in keys:
                if key in payload:
                    value = payload.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
                    if isinstance(value, (int, float)):
                        return value
            for value in payload.values():
                found = self._extract_workday_value(value, keys)
                if found is not None:
                    return found
        if isinstance(payload, list):
            for entry in payload:
                found = self._extract_workday_value(entry, keys)
                if found is not None:
                    return found
        return None

    def extract_location_hint(self, markdown: str) -> Optional[str]:
        if not markdown:
            return None
        for line in markdown.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            match = _LOCATION_LINE_RE.match(stripped)
            if match:
                return match.group("location").strip()
        return None

    def normalize_markdown(self, markdown: str) -> tuple[str, Optional[str]]:
        if not markdown:
            return "", None

        # Handle JSON in raw HTML (Workday API responses with <pre> tags)
        stripped_text = markdown.strip()
        if "<pre>{" in stripped_text:
            import json
            match = re.search(r"<pre>({.+})</pre>", stripped_text, re.DOTALL)
            if match:
                json_content = match.group(1)
                try:
                    parsed = json.loads(json_content)
                    posting_info = parsed.get("jobPostingInfo", {})
                    if isinstance(posting_info, dict):
                        title = posting_info.get("title")
                        description = posting_info.get("jobDescription", "")
                        # Convert HTML description to readable text
                        if description:
                            description = html_lib.unescape(description)
                            # Remove HTML tags
                            description = re.sub(r"<[^>]+>", " ", description)
                            description = re.sub(r"\s+", " ", description).strip()
                        return description or markdown, title
                except Exception:
                    pass

        # Handle JSON in code fences (Workday API responses)
        if stripped_text.startswith("```"):
            lines_raw = stripped_text.split("\n")
            # Find start and end of code fence content
            start_idx_json = 1 if len(lines_raw) > 1 else 0
            end_idx_json = len(lines_raw)
            for i in range(len(lines_raw) - 1, 0, -1):
                if lines_raw[i].strip() == "```":
                    end_idx_json = i
                    break
            json_content = "\n".join(lines_raw[start_idx_json:end_idx_json]).strip()
            if json_content.startswith("{"):
                try:
                    import json
                    parsed = json.loads(json_content)
                    # Extract from jobPostingInfo
                    posting_info = parsed.get("jobPostingInfo", {})
                    if isinstance(posting_info, dict):
                        title = posting_info.get("title")
                        description = posting_info.get("jobDescription", "")
                        # Convert HTML description to text
                        if description:
                            description = html_lib.unescape(description)
                            # Remove HTML tags
                            description = re.sub(r"<[^>]+>", " ", description)
                            description = re.sub(r"\s+", " ", description).strip()
                        return description or markdown, title
                except Exception:
                    pass

        lines = markdown.splitlines()
        title: Optional[str] = None
        start_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            heading_match = re.match(r"^#{1,6}\s*(.+)$", stripped)
            if not heading_match:
                continue
            candidate = heading_match.group(1).strip()
            if not candidate or candidate.lower() == "careers":
                continue
            title = candidate
            start_idx = idx
            break
        if start_idx is not None:
            lines = lines[start_idx:]

        stop_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            if _STOP_SECTION_RE.match(line.strip()):
                stop_idx = idx
                break
        if stop_idx is not None:
            lines = lines[:stop_idx]

        cleaned_lines: List[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                cleaned_lines.append(line)
                continue
            lower = stripped.lower()
            if lower in {"careers", "sign in", "decline", "accept cookies", "english"}:
                continue
            if lower.startswith("skip to main content"):
                continue
            if "this website uses cookies" in lower:
                continue
            if stripped.lower().startswith("[apply"):
                continue
            if _IMAGE_LINE_RE.match(stripped):
                continue
            if _EMPTY_LINK_RE.match(stripped):
                continue
            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned or markdown, title

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        filtered: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = url.strip()
            if not cleaned:
                continue
            lower = cleaned.lower()
            if lower.startswith(("mailto:", "tel:", "javascript:")):
                continue
            try:
                parsed = urlparse(cleaned)
            except Exception:
                parsed = None
            if parsed and parsed.hostname and not self.matches_url(cleaned):
                continue
            path = (parsed.path if parsed else cleaned).lower()
            if self.is_api_detail_url(cleaned):
                # Strip query string for deduplication - same job URL with different filters is the same job
                if parsed and parsed.query:
                    candidate = cleaned.split("?")[0]
                else:
                    candidate = cleaned
            elif "/job/" in path:
                candidate = self.get_api_uri(cleaned) or cleaned
            elif self.is_listing_url(cleaned):
                candidate = cleaned
            else:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            filtered.append(candidate)
        return filtered

    @staticmethod
    def _extract_base_url(html: str) -> Optional[str]:
        for pattern in BASE_URL_META_PATTERNS:
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                return match.group("url")
        match = WORKDAY_BASE_URL_RE.search(html)
        if match:
            return match.group(0)
        return None
