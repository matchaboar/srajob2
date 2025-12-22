from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from .base import BaseSiteHandler
from ..helpers.regex_patterns import (
    BASE_URL_META_PATTERNS,
    WORKDAY_BASE_URL_RE,
    WORKDAY_JOB_DETAIL_PATH_RE,
    WORKDAY_JOB_DETAIL_URL_RE,
    WORKDAY_JOB_TITLE_ANCHOR_RE,
    WORKDAY_PAGE_RANGE_RE,
)

WORKDAY_HOST_SUFFIX = "myworkdayjobs.com"


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
        return host.endswith(WORKDAY_HOST_SUFFIX)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.hostname and not self.matches_url(url):
            return False
        path = (parsed.path or "").lower()
        return bool(path) and "/job/" not in path

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
                            "timeout": {"secs": 40, "nanos": 0},
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

        def _add(url_val: str | None) -> None:
            if not url_val:
                return
            cleaned = html_lib.unescape(url_val.strip())
            if not cleaned or cleaned in seen:
                return
            if base_url and not cleaned.startswith(("http://", "https://")):
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
                _add(urljoin(base_url, match))

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
            if lower.startswith(("mailto:", "tel:", "javascript:")):
                continue
            try:
                parsed = urlparse(cleaned)
            except Exception:
                parsed = None
            if parsed and parsed.hostname and not self.matches_url(cleaned):
                continue
            path = (parsed.path if parsed else cleaned).lower()
            if "/job/" in path or self.is_listing_url(cleaned):
                seen.add(cleaned)
                filtered.append(cleaned)
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
