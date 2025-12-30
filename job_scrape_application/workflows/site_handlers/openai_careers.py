from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

from .base import BaseSiteHandler

OPENAI_HOST_SUFFIX = "openai.com"
OPENAI_BASE_URL = "https://openai.com"
CAREERS_PATH = "/careers/"
SEARCH_PATH = "/careers/search"

_JOB_LINK_RE = re.compile(r"href=[\"'](?P<href>/careers/[^\"'>\s]+)", flags=re.IGNORECASE)


class OpenAICareersHandler(BaseSiteHandler):
    name = "openai_careers"
    site_type = "openai"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(OPENAI_HOST_SUFFIX):
            return False
        return CAREERS_PATH in (parsed.path or "").lower()

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").lower()
        return path.startswith(SEARCH_PATH)

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
                    "selector": "a[href*='/careers/']",
                    "timeout": {"secs": 15, "nanos": 0},
                }
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for match in _JOB_LINK_RE.finditer(html):
            href = html_lib.unescape(match.group("href")).strip()
            if not href:
                continue
            absolute = urljoin(OPENAI_BASE_URL, href)
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
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
            path = parsed.path or ""
            if not host.endswith(OPENAI_HOST_SUFFIX):
                continue
            if not self._is_job_detail_path(path):
                continue
            normalized = urljoin(OPENAI_BASE_URL, path)
            if normalized in seen:
                continue
            seen.add(normalized)
            filtered.append(normalized)
        return filtered

    def _is_job_detail_path(self, path: str) -> bool:
        if not path:
            return False
        lowered = path.lower()
        if not lowered.startswith(CAREERS_PATH):
            return False
        if lowered.startswith(SEARCH_PATH):
            return False
        if lowered.endswith(".js") or lowered.endswith(".css"):
            return False
        segments = [segment for segment in lowered.strip("/").split("/") if segment]
        if len(segments) != 2:
            return False
        if segments[0] != "careers":
            return False
        if not segments[1] or segments[1] == "search":
            return False
        return True
