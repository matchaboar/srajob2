from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from ..helpers.regex_patterns import CONFLUENT_JOB_PATH_PATTERN
from .base import BaseSiteHandler


class ConfluentHandler(BaseSiteHandler):
    name = "confluent"
    _job_path_re = re.compile(CONFLUENT_JOB_PATH_PATTERN, re.IGNORECASE)
    _page_re = re.compile(r"/jobs[^\"'\\s<>]*\\bpage=\\d+", re.IGNORECASE)

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return False
        path = (parsed.path or "").lower()
        return path.startswith("/jobs")

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return False
        path = (parsed.path or "").lower()
        if not path.startswith("/jobs"):
            return False
        return "/jobs/job/" not in path

    def get_spidercloud_config(self, uri: str) -> dict[str, Any]:
        if self.is_listing_url(uri):
            return {"return_format": ["raw_html"]}
        return {}

    def get_links_from_raw_html(self, html: str) -> list[str]:
        if not isinstance(html, str) or not html:
            return []
        links: list[str] = []
        seen: set[str] = set()
        for value in super().get_links_from_raw_html(html):
            if value and value not in seen:
                seen.add(value)
                links.append(value)
        for match in self._job_path_re.findall(html):
            if match and match not in seen:
                seen.add(match)
                links.append(match)
        for match in self._page_re.findall(html):
            if match and match not in seen:
                seen.add(match)
                links.append(match)
        return links

    def get_links_from_markdown(self, markdown: str) -> list[str]:
        if not isinstance(markdown, str) or not markdown:
            return []
        links: list[str] = []
        seen: set[str] = set()
        for match in self._job_path_re.findall(markdown):
            if match and match not in seen:
                seen.add(match)
                links.append(match)
        for match in self._page_re.findall(markdown):
            if match and match not in seen:
                seen.add(match)
                links.append(match)
        return links
