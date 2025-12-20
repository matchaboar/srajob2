from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .base import BaseSiteHandler

ASHBY_JOB_URL_PATTERN = r"https?://jobs\\.ashbyhq\\.com/[^\\s\"'>]+"


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
