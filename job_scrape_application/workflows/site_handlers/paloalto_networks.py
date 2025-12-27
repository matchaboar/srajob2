from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

from .base import BaseSiteHandler

PALOALTO_HOST = "jobs.paloaltonetworks.com"
PALOALTO_BASE_URL = f"https://{PALOALTO_HOST}"
LISTING_PATH_TOKEN = "/search-jobs"
JOB_PATH_TOKEN = "/job/"

_JOB_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://[^"\']+/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)
_JOB_LINK_REL_RE = re.compile(
    r'href=["\'](?P<href>/[^"\']+/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)


class PaloAltoNetworksHandler(BaseSiteHandler):
    name = "paloalto_networks"
    site_type = "paloalto"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        return host == PALOALTO_HOST

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
        return {
            "return_format": ["raw_html"],
            "request": "chrome",
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }

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
                cleaned = urljoin(PALOALTO_BASE_URL, cleaned)
            if cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for match in _JOB_LINK_RE.finditer(html):
            _add(match.group("href"))
        for match in _JOB_LINK_REL_RE.finditer(html):
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
            if PALOALTO_HOST not in lower:
                continue
            if JOB_PATH_TOKEN not in lower:
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered
