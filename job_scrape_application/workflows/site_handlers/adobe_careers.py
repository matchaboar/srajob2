from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

from .base import BaseSiteHandler

ADOBE_HOST_SUFFIX = "careers.adobe.com"
ADOBE_BASE_URL = "https://careers.adobe.com"
LISTING_PATH_TOKEN = "/search-results"
JOB_PATH_TOKEN = "/job/"

_JOB_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://[^"\']+/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)
_JOB_LINK_REL_RE = re.compile(
    r'href=["\'](?P<href>/[^"\']+/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)
_JOB_URL_RE = re.compile(
    r'(?P<href>https?://(?:www\.)?careers\.adobe\.com[^"\']+/job/[^"\']+)',
    flags=re.IGNORECASE,
)
_PAGINATION_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://[^"\']+/search-results[^"\']*from=\d+[^"\']*)["\']',
    flags=re.IGNORECASE,
)
_PAGINATION_LINK_REL_RE = re.compile(
    r'href=["\'](?P<href>/[^"\']*/search-results[^"\']*from=\d+[^"\']*)["\']',
    flags=re.IGNORECASE,
)


class AdobeCareersHandler(BaseSiteHandler):
    name = "adobe_careers"
    site_type = "adobe"
    needs_page_links = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        return bool(host) and host.endswith(ADOBE_HOST_SUFFIX)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.hostname and not self.matches_url(url):
            return False
        path = (parsed.path or "").lower()
        return LISTING_PATH_TOKEN in path

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
                    "selector": "a[data-ph-at-id='job-link']",
                    "timeout": {"secs": 25, "nanos": 0},
                },
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []

        urls: List[str] = []
        seen: set[str] = set()

        def _add(url_val: str | None) -> None:
            if not url_val:
                return
            cleaned = html_lib.unescape(url_val).strip()
            if not cleaned:
                return
            if cleaned.startswith("/"):
                cleaned = urljoin(ADOBE_BASE_URL, cleaned)
            if cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for match in _JOB_LINK_RE.finditer(html):
            _add(match.group("href"))
        for match in _JOB_LINK_REL_RE.finditer(html):
            _add(match.group("href"))
        for match in _JOB_URL_RE.finditer(html):
            _add(match.group("href"))
        for match in _PAGINATION_LINK_RE.finditer(html):
            _add(match.group("href"))
        for match in _PAGINATION_LINK_REL_RE.finditer(html):
            _add(match.group("href"))

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
            if "hvhapply" in lower:
                continue
            if self.is_listing_url(cleaned) or JOB_PATH_TOKEN in lower:
                seen.add(cleaned)
                filtered.append(cleaned)
        return filtered
