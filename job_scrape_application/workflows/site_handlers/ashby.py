from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .base import BaseSiteHandler
from ..helpers.link_extractors import fix_scheme_slashes, strip_wrapping_url
from ..helpers.regex_patterns import ASHBY_JOB_URL_PATTERN


class AshbyHqHandler(BaseSiteHandler):
    name = "ashby"
    supports_listing_api = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith("ashbyhq.com")

    def _job_board_slug(self, url: str) -> Optional[str]:
        if not self.matches_url(url):
            return None
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        path = parsed.path.strip("/")
        if not path:
            return None
        segments = [seg for seg in path.split("/") if seg]
        if not segments:
            return None
        if len(segments) >= 3 and segments[0] == "posting-api" and segments[1] == "job-board":
            return segments[2]
        return segments[0]

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        slug = self._job_board_slug(uri)
        if not slug:
            return None
        return f"https://api.ashbyhq.com/posting-api/job-board/{slug}"

    def get_api_uri(self, uri: str) -> Optional[str]:
        return self.get_listing_api_uri(uri)

    def get_company_uri(self, uri: str) -> Optional[str]:
        slug = self._job_board_slug(uri)
        if not slug:
            return None
        return f"https://jobs.ashbyhq.com/{slug}"

    def get_links_from_json(self, payload: Any) -> List[str]:
        jobs = payload.get("jobs") if isinstance(payload, dict) else None
        if not isinstance(jobs, list):
            return []
        url_keys = ("jobUrl", "applyUrl", "jobPostingUrl", "postingUrl", "url")
        urls: List[str] = []
        seen: set[str] = set()
        for job in jobs:
            if not isinstance(job, dict):
                continue
            for key in url_keys:
                value = job.get(key)
                if isinstance(value, str) and value.strip():
                    url = value.strip()
                    if url not in seen:
                        seen.add(url)
                        urls.append(url)
        return urls

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for match in re.findall(ASHBY_JOB_URL_PATTERN, html):
            url = match.strip()
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        filtered: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = strip_wrapping_url(url)
            if not cleaned or cleaned in seen:
                continue
            cleaned = fix_scheme_slashes(cleaned)
            lower = cleaned.lower()
            if lower.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            if self._looks_like_non_job_detail_url(cleaned):
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    @staticmethod
    def _looks_like_non_job_detail_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.scheme and parsed.scheme not in {"http", "https"}:
            return True
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
        if not host or not path:
            return False
        if any(token in path for token in ("http://", "https://", "http:/", "https:/")):
            return True
        segments = [seg for seg in path.split("/") if seg]
        if not host.endswith("ashbyhq.com"):
            if any(seg in {"apply", "application", "hvhapply"} for seg in segments):
                return True
        if host.endswith("linkedin.com") and path.startswith("/company/"):
            return True
        return False

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        try:
            parsed = urlparse(uri)
        except Exception:
            parsed = None
        host = (parsed.hostname or "").lower() if parsed else ""
        path = (parsed.path or "").strip("/") if parsed else ""
        segments = [seg for seg in path.split("/") if seg]
        is_api = host.startswith("api.ashbyhq.com") or (
            len(segments) >= 2 and segments[0] == "posting-api" and segments[1] == "job-board"
        )
        is_job_detail = not is_api and len(segments) >= 2
        return_format = ["commonmark"] if is_job_detail else ["raw_html"]
        base_config = {
            "request": "chrome",
            "return_format": return_format,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }
        return self._apply_page_links_config(base_config)
