from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

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
PAGE_RANGE_PATTERN = re.compile(
    r"(?P<start>\d+)\s*-\s*(?P<end>\d+)\s*of\s*(?P<total>\d+)",
    re.IGNORECASE,
)
JOB_RECORDS_PER_PAGE_PATTERN = re.compile(
    r"jobRecordsPerPage\"?\s*[:=]\s*\"?(?P<count>\d+)\"?",
    re.IGNORECASE,
)
RESULTS_ARIA_PATTERN = re.compile(r"aria-label=\"\s*(?P<count>\d+)\s+results", re.IGNORECASE)


class AvatureHandler(BaseSiteHandler):
    name = "avature"
    site_type = "avature"
    needs_page_links = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return any(host.endswith(suffix) for suffix in AVATURE_HOST_SUFFIXES)

    def is_listing_url(self, url: str) -> bool:
        try:
            path = (urlparse(url).path or "").lower()
        except Exception:
            return False
        return "/careers/searchjobs" in path or "/careers/searchjobsdata" in path

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
                            "selector": "a[href*='/careers/JobDetail/']",
                            "timeout": {"secs": 15, "nanos": 0},
                        },
                        "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
                    },
                }
            )
        return {
            "request": "chrome",
            "return_format": ["commonmark"],
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

        if base_url and "/careers/searchjobs" in base_url.lower():
            urls.extend(self._augment_pagination_urls(base_url, html, urls))

        return self.filter_job_urls(urls)

    def _augment_pagination_urls(self, base_url: str, html: str, urls: List[str]) -> List[str]:
        def _with_job_offset(url_value: str, offset: int) -> str:
            parsed = urlparse(url_value)
            params = [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key.lower() != "joboffset"
            ]
            params.append(("jobOffset", str(offset)))
            return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))

        base_offset = None
        base_has_offset = False
        for key, value in parse_qsl(urlparse(base_url).query, keep_blank_values=True):
            if key.lower() == "joboffset":
                base_has_offset = True
                try:
                    base_offset = int(value)
                except Exception:
                    base_offset = None

        def _infer_page_data() -> tuple[int | None, int | None, int | None]:
            match = PAGE_RANGE_PATTERN.search(html)
            if match:
                start = int(match.group("start"))
                end = int(match.group("end"))
                total = int(match.group("total"))
                page_size = max(end - start + 1, 1)
                current_offset = max(start - 1, 0)
                return current_offset, page_size, total

            page_size = None
            total = None
            match = JOB_RECORDS_PER_PAGE_PATTERN.search(html)
            if match:
                page_size = int(match.group("count"))
            match = RESULTS_ARIA_PATTERN.search(html)
            if match:
                total = int(match.group("count"))
            default_offset = base_offset if base_offset is not None else 0
            return default_offset, page_size, total

        augmented: List[str] = []
        current_offset, page_size, total = _infer_page_data()
        if base_offset is not None:
            current_offset = base_offset
        should_add_zero = (not base_has_offset) or (current_offset == 0)
        if should_add_zero and not any("joboffset=0" in url.lower() for url in urls):
            augmented.append(_with_job_offset(base_url, 0))

        if page_size and current_offset is not None:
            next_offset = current_offset + page_size
            if total is None or next_offset < total:
                next_token = f"joboffset={next_offset}"
                if not any(next_token in url.lower() for url in urls + augmented):
                    augmented.append(_with_job_offset(base_url, next_offset))

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
            if "/savejob" in lower or "/login" in lower or "/register" in lower:
                continue
            if "/careers/" not in lower:
                continue
            if not any(
                token in lower
                for token in (
                    "/careers/jobdetail/",
                    "/careers/searchjobs",
                    "/careers/searchjobsdata",
                )
            ):
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
