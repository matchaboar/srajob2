from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urlparse

from .base import BaseSiteHandler
from ..helpers.regex_patterns import ASHBY_JOB_URL_PATTERN

NOTION_HOST_SUFFIX = "notion.com"
CAREERS_PATH = "/careers"
ASHBY_HOST = "jobs.ashbyhq.com"
ASHBY_SLUG = "notion"


class NotionCareersHandler(BaseSiteHandler):
    name = "notion_careers"
    site_type = "notion"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(NOTION_HOST_SUFFIX):
            return False
        path = (parsed.path or "").lower()
        return path.startswith(CAREERS_PATH)

    def is_listing_url(self, url: str) -> bool:
        return self.matches_url(url)

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        base_config: Dict[str, Any] = {
            "request": "basic",
            "return_format": ["commonmark"],
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }
        return self._apply_page_links_config(base_config)

    def get_links_from_markdown(self, markdown: str) -> List[str]:
        return self._extract_ashby_links(markdown)

    def get_links_from_raw_html(self, html: str) -> List[str]:
        return self._extract_ashby_links(html)

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
            if host != ASHBY_HOST:
                continue
            segments = [segment for segment in (parsed.path or "").split("/") if segment]
            if not segments or segments[0].lower() != ASHBY_SLUG:
                continue
            if len(segments) < 2:
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    def _extract_ashby_links(self, text: str) -> List[str]:
        if not text:
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for match in re.findall(ASHBY_JOB_URL_PATTERN, text):
            url = match.strip()
            if not url or url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return self.filter_job_urls(urls)
