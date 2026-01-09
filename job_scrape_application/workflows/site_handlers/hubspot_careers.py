from __future__ import annotations

import html as html_lib
import math
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from .base import BaseSiteHandler

HUBSPOT_HOST_SUFFIX = "hubspot.com"
HUBSPOT_BASE_URL = "https://www.hubspot.com"
CAREERS_PATH = "/careers/jobs"
MAX_PAGINATION_PAGE = 4

_JOB_LINK_RE = re.compile(
    r"href=[\"'](?P<href>/careers/jobs/\d+[^\"']*)[\"']",
    flags=re.IGNORECASE,
)
_SHOWING_RANGE_RE = re.compile(
    r"Showing\s+(?P<start>\d{1,3}(?:,\d{3})*)\s*-\s*(?P<end>\d{1,3}(?:,\d{3})*)\s+of\s+(?P<total>\d{1,3}(?:,\d{3})*)",
    flags=re.IGNORECASE,
)
_HEADING_RE = re.compile(r"^#{2,4}\s+(?P<value>.+)$")


class HubspotCareersHandler(BaseSiteHandler):
    name = "hubspot_careers"
    site_type = "hubspot"
    supports_listing_api = False

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(HUBSPOT_HOST_SUFFIX):
            return False
        path = (parsed.path or "").lower()
        return path.startswith(CAREERS_PATH)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        return self._is_listing_path(parsed.path or "")

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        base_config: Dict[str, Any] = {
            "request": "chrome",
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }
        if self.is_listing_url(uri):
            base_config["return_format"] = ["raw_html"]
            base_config["wait_for"] = {
                "selector": {
                    "selector": "a.careers-apply",
                    "timeout": {"secs": 20, "nanos": 0},
                },
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

    def get_pagination_urls_from_listing(self, source_url: str | None = None) -> List[str]:
        if not source_url or not self.matches_url(source_url):
            return []
        if not self.is_listing_url(source_url):
            return []
        parsed = urlparse(source_url)
        current_page = self._extract_page_param(parsed.query) or 1
        if current_page >= MAX_PAGINATION_PAGE:
            return []
        return [
            self._set_page_param(source_url, page)
            for page in range(current_page + 1, MAX_PAGINATION_PAGE + 1)
        ]

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for match in _JOB_LINK_RE.finditer(html):
            href = html_lib.unescape(match.group("href")).strip()
            if not href:
                continue
            absolute = urljoin(HUBSPOT_BASE_URL, href)
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)

        for page_url in self._build_pagination_urls(html):
            if page_url in seen:
                continue
            seen.add(page_url)
            urls.append(page_url)

        return self.filter_job_urls(urls)

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        filtered: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = url.strip()
            if not cleaned or cleaned in seen:
                continue
            try:
                parsed = urlparse(cleaned)
            except Exception:
                continue
            host = (parsed.hostname or "").lower()
            if not host.endswith(HUBSPOT_HOST_SUFFIX):
                continue
            path = parsed.path or ""
            normalized_variants: List[str] = []
            if self._is_job_detail_path(path):
                canonical = urlunparse(
                    parsed._replace(path=path, params="", query="", fragment="")
                )
                normalized_variants.append(canonical)
            elif self._is_listing_path(path):
                normalized_variants.append(self._normalize_listing_url(parsed))
            else:
                continue
            for normalized in normalized_variants:
                if normalized in seen:
                    continue
                seen.add(normalized)
                filtered.append(normalized)
        return filtered

    def extract_company(self, payload: Any, url: str | None = None) -> Optional[str]:
        if url and self.matches_url(url):
            return "HubSpot"
        return None

    def extract_location_hint(self, markdown: str) -> Optional[str]:
        if not markdown:
            return None
        headings: List[str] = []
        title_index: Optional[int] = None
        for line in markdown.splitlines():
            match = _HEADING_RE.match(line.strip())
            if not match:
                continue
            value = match.group("value").strip()
            if not value:
                continue
            headings.append(value)
            if title_index is None:
                lowered = value.lower()
                if lowered in {"all open positions", "apply for this job"}:
                    continue
                if "recruiting process" in lowered:
                    continue
                title_index = len(headings) - 1

        if not headings:
            return None

        candidates = [
            (idx, value)
            for idx, value in enumerate(headings)
            if self._looks_like_location(value)
        ]
        if not candidates:
            return None
        if title_index is not None:
            for idx, value in candidates:
                if idx > title_index:
                    return value
        return candidates[0][1]

    def normalize_markdown(self, markdown: str) -> tuple[str, Optional[str]]:
        if not markdown:
            return "", None
        lines = markdown.splitlines()
        title: Optional[str] = None
        start_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            match = _HEADING_RE.match(line.strip())
            if not match:
                continue
            heading = match.group("value").strip()
            if not heading:
                continue
            lowered = heading.lower()
            if lowered in {"all open positions", "apply for this job"}:
                continue
            if "recruiting process" in lowered:
                continue
            title = heading
            start_idx = idx
            break
        if start_idx is not None:
            lines = lines[start_idx:]

        stop_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            lowered = line.strip().lower()
            if lowered.startswith("## apply for this job") or lowered == "apply for this job":
                stop_idx = idx
                break
            if "recruiting process like hubspot" in lowered:
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
            lowered = stripped.lower()
            if "back to all openings" in lowered:
                continue
            if lowered in {"careers menu", "logo - full (color)"}:
                continue
            if "headquartered" in lowered or "headquarters" in lowered:
                continue
            if "remote employee" in lowered and "work from the office" in lowered:
                continue
            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned or markdown, title

    def _build_pagination_urls(self, html: str) -> List[str]:
        range_info = self._extract_showing_range(html)
        if not range_info:
            return []
        start, end, total = range_info
        if total <= 0 or end < start:
            return []
        page_size = max(end - start + 1, 1)
        total_pages = max(1, math.ceil(total / page_size))
        current_page = max(1, math.ceil(end / page_size))
        if total_pages <= 1:
            return []
        base_url = f"{HUBSPOT_BASE_URL}{CAREERS_PATH}"
        urls: List[str] = []
        for page in range(1, total_pages + 1):
            if page == current_page:
                continue
            urls.append(self._set_page_param(base_url, page))
        return urls

    def _extract_showing_range(self, html: str) -> Optional[tuple[int, int, int]]:
        if not html:
            return None
        normalized = html_lib.unescape(html)
        normalized = normalized.replace("\u2013", "-").replace("\u2014", "-")
        match = _SHOWING_RANGE_RE.search(normalized)
        if not match:
            return None

        def _to_int(value: str) -> int:
            return int(value.replace(",", ""))

        try:
            start = _to_int(match.group("start"))
            end = _to_int(match.group("end"))
            total = _to_int(match.group("total"))
        except Exception:
            return None
        return start, end, total

    @staticmethod
    def _looks_like_location(value: str) -> bool:
        lowered = value.lower()
        if "remote" in lowered or "hybrid" in lowered or "flex" in lowered:
            return True
        if "," in value:
            return True
        if " - " in value:
            return True
        return False

    @staticmethod
    def _is_job_detail_path(path: str) -> bool:
        if not path:
            return False
        segments = [segment for segment in path.strip("/").split("/") if segment]
        if len(segments) != 3:
            return False
        if segments[0].lower() != "careers" or segments[1].lower() != "jobs":
            return False
        return segments[2].isdigit()

    @staticmethod
    def _is_listing_path(path: str) -> bool:
        if not path:
            return False
        segments = [segment for segment in path.strip("/").split("/") if segment]
        if len(segments) == 2 and segments[0].lower() == "careers" and segments[1].lower() == "jobs":
            return True
        if (
            len(segments) == 3
            and segments[0].lower() == "careers"
            and segments[1].lower() == "jobs"
            and segments[2].lower() == "all"
        ):
            return True
        return False

    @staticmethod
    def _extract_page_param(query: str) -> Optional[int]:
        for key, value in parse_qsl(query, keep_blank_values=True):
            if key.lower() != "page":
                continue
            try:
                parsed = int(value)
            except Exception:
                return None
            return parsed if parsed >= 1 else None
        return None

    @staticmethod
    def _set_page_param(url: str, page: int) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        params = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() != "page"
        ]
        params.append(("page", str(page)))
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=query, fragment=""))

    def _normalize_listing_url(self, parsed) -> str:
        page = self._extract_page_param(parsed.query)
        query = ""
        if page and page > 1:
            query = urlencode({"page": str(page)})
        return urlunparse(parsed._replace(path=CAREERS_PATH, params="", query=query, fragment=""))
