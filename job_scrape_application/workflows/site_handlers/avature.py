from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from .base import BaseSiteHandler

AVATURE_HOST_SUFFIXES = ("avature.net", "avature.com")
JOB_DETAIL_PATH_PATTERN = re.compile(r"/careers/JobDetail/[^\"'\s>]+", re.IGNORECASE)
JOB_DETAIL_URL_PATTERN = re.compile(
    r"https?://[^\"'\s>]+/careers/JobDetail/[^\"'\s>]+", re.IGNORECASE
)
PAGINATION_PATH_PATTERN = re.compile(
    r"/careers/SearchJobs/[^\"'\s>]*?jobOffset=\d+", re.IGNORECASE
)
PAGINATION_URL_PATTERN = re.compile(
    r"https?://[^\"'\s>]+/careers/SearchJobs/[^\"'\s>]*?jobOffset=\d+", re.IGNORECASE
)
BASE_URL_PATTERN = re.compile(r"https?://[^\"'\s>]+/careers/[^\"'\s>]*", re.IGNORECASE)


class AvatureHandler(BaseSiteHandler):
    name = "avature"
    site_type = "avature"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return any(host.endswith(suffix) for suffix in AVATURE_HOST_SUFFIXES)

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
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
        urls: List[str] = []
        seen: set[str] = set()

        def _add(url_val: str | None) -> None:
            if not url_val:
                return
            cleaned = url_val.strip()
            if not cleaned:
                return
            if cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for match in JOB_DETAIL_URL_PATTERN.findall(html):
            _add(match)
        for match in PAGINATION_URL_PATTERN.findall(html):
            _add(match)

        if base_url:
            for match in JOB_DETAIL_PATH_PATTERN.findall(html):
                _add(urljoin(base_url, match))
            for match in PAGINATION_PATH_PATTERN.findall(html):
                _add(urljoin(base_url, match))

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
            if "/savejob" in lower or "/login" in lower or "/register" in lower:
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    @staticmethod
    def _extract_base_url(html: str) -> Optional[str]:
        for pattern in (
            r"<base[^>]+href=\"(?P<url>[^\"]+)\"",
            r"property=\"og:url\"[^>]+content=\"(?P<url>[^\"]+)\"",
            r"rel=\"canonical\"[^>]+href=\"(?P<url>[^\"]+)\"",
        ):
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                return match.group("url")
        match = BASE_URL_PATTERN.search(html)
        if match:
            return match.group(0)
        return None
