from __future__ import annotations

import html as html_lib
import math
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from .base import BaseSiteHandler

UBER_HOST_SUFFIX = "uber.com"
UBER_BASE_URL = "https://www.uber.com"
CAREERS_LIST_TOKEN = "/careers/list"
DEFAULT_PAGE_SIZE = 10

_JOB_LINK_RE = re.compile(
    r"href=[\"'](?P<href>/[^\"']*?/careers/list/\d+)[\"']",
    flags=re.IGNORECASE,
)
_LISTING_LINK_RE = re.compile(
    r"href=[\"'](?P<href>/[^\"']*/careers/list/\?[^\"']*)[\"']",
    flags=re.IGNORECASE,
)
_OPEN_ROLES_RE = re.compile(
    r"(?P<count>\d{1,3}(?:,\d{3})*)\s*open\s*(?:&nbsp;|\u00a0|\s)roles",
    flags=re.IGNORECASE,
)


class UberCareersHandler(BaseSiteHandler):
    name = "uber_careers"
    site_type = "uber"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(UBER_HOST_SUFFIX):
            return False
        return CAREERS_LIST_TOKEN in (parsed.path or "").lower()

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").strip("/")
        if not path:
            return False
        segments = [segment for segment in path.split("/") if segment]
        if not segments:
            return False
        return segments[-1].lower() == "list"

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
                    "selector": "a[href*='/careers/list/']",
                    "timeout": {"secs": 20, "nanos": 0},
                },
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []

        job_links = self._extract_job_links(html)
        page_links = self._build_pagination_urls(html, len(job_links))

        urls: List[str] = []
        seen: set[str] = set()
        for url in job_links + page_links:
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)

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
            lower = cleaned.lower()
            if CAREERS_LIST_TOKEN not in lower:
                continue
            if self.is_listing_url(cleaned) or re.search(r"/careers/list/\d+$", lower):
                seen.add(cleaned)
                filtered.append(cleaned)
        return filtered

    def _extract_job_links(self, html: str) -> List[str]:
        urls: List[str] = []
        seen: set[str] = set()
        for match in _JOB_LINK_RE.finditer(html):
            href = html_lib.unescape(match.group("href")).strip()
            if not href:
                continue
            absolute = urljoin(UBER_BASE_URL, href)
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
        if urls:
            return urls
        for match in re.findall(r"/careers/list/\d+", html, flags=re.IGNORECASE):
            absolute = urljoin(UBER_BASE_URL, match)
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
        return urls

    def _build_pagination_urls(self, html: str, page_size: int) -> List[str]:
        total = self._extract_open_roles(html)
        if total is None or total <= 0:
            return []
        if page_size <= 0:
            page_size = DEFAULT_PAGE_SIZE
        if total <= page_size:
            return []
        base_url = self._extract_listing_url(html)
        if not base_url:
            return []
        base_url = self._strip_page_param(base_url)
        total_pages = max(1, math.ceil(total / page_size))
        urls: List[str] = []
        for page in range(1, total_pages):
            urls.append(self._set_page_param(base_url, page))
        return urls

    def _extract_listing_url(self, html: str) -> Optional[str]:
        for match in _LISTING_LINK_RE.finditer(html):
            href = html_lib.unescape(match.group("href")).strip()
            if not href:
                continue
            absolute = urljoin(UBER_BASE_URL, href)
            return absolute
        return None

    def _strip_page_param(self, url: str) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        params = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() != "page"
        ]
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=query))

    def _set_page_param(self, url: str, page: int) -> str:
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
        return urlunparse(parsed._replace(query=query))

    def _extract_open_roles(self, html: str) -> Optional[int]:
        text = html_lib.unescape(html).replace("\xa0", " ")
        match = _OPEN_ROLES_RE.search(text)
        if not match:
            return None
        raw = match.group("count").replace(",", "")
        try:
            return int(raw)
        except Exception:
            return None
