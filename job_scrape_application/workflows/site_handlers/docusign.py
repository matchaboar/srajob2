from __future__ import annotations

import math
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .base import BaseSiteHandler

DOCUSIGN_HOST = "careers.docusign.com"
LISTING_PATH = "/api/jobs"
JOB_DETAIL_PATH = "/jobs/"


class DocusignHandler(BaseSiteHandler):
    name = "docusign"
    site_type = "docusign"
    supports_listing_api = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith(DOCUSIGN_HOST):
            return False
        path = (parsed.path or "").lower()
        return path.startswith(LISTING_PATH) or JOB_DETAIL_PATH in path

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").lower()
        return path.startswith(LISTING_PATH)

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        if not self.matches_url(uri):
            return None
        if self.is_listing_url(uri):
            return uri
        return None

    def get_links_from_json(self, payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for job in jobs:
            if not isinstance(job, dict):
                continue
            data = job.get("data")
            if isinstance(data, dict):
                job_data = data
            else:
                job_data = job
            if not isinstance(job_data, dict):
                continue
            url = self._extract_job_url(job_data)
            if not url:
                continue
            cleaned = url.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            urls.append(cleaned)
        return urls

    def get_pagination_urls_from_json(self, payload: Any, source_url: str | None = None) -> List[str]:
        if not isinstance(payload, dict):
            return []
        total = payload.get("totalCount")
        if not isinstance(total, int):
            total = payload.get("count")
        if not isinstance(total, int) or total <= 0:
            return []
        jobs = payload.get("jobs")
        page_size = len(jobs) if isinstance(jobs, list) else 0
        if page_size <= 0:
            page_size = self._extract_display_limit(payload)
        if page_size <= 0:
            return []
        total_pages = max(1, math.ceil(total / page_size))
        current_page = self._extract_page_from_url(source_url) or 1
        if current_page >= total_pages:
            return []
        if not source_url:
            return []
        urls: List[str] = []
        for page in range(current_page + 1, total_pages + 1):
            urls.append(self._set_page_param(source_url, page))
        return urls

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
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

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
            path = (parsed.path or "").lower()
            if not host.endswith(DOCUSIGN_HOST):
                continue
            if JOB_DETAIL_PATH not in path:
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    def _extract_job_url(self, data: Dict[str, Any]) -> Optional[str]:
        meta = data.get("meta_data")
        if isinstance(meta, dict):
            canonical = meta.get("canonical_url")
            if isinstance(canonical, str) and canonical.strip():
                return canonical
        for key in ("canonical_url", "jobUrl", "postingUrl", "url"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value
        slug = data.get("slug") or data.get("req_id")
        if isinstance(slug, str) and slug.strip():
            language = data.get("language")
            base = f"https://{DOCUSIGN_HOST}/jobs/{slug.strip()}"
            if isinstance(language, str) and language.strip():
                return f"{base}?lang={language.strip()}"
            return base
        return None

    def _extract_display_limit(self, payload: Dict[str, Any]) -> int:
        filter_data = payload.get("filter")
        if not isinstance(filter_data, dict):
            return 0
        display_limit = filter_data.get("displayLimit")
        if not isinstance(display_limit, int):
            return 0
        return display_limit if display_limit > 0 else 0

    def _extract_page_from_url(self, url: str | None) -> Optional[int]:
        if not url:
            return None
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower() != "page":
                continue
            try:
                page_val = int(value)
            except Exception:
                return None
            return page_val if page_val >= 1 else None
        return None

    def _set_page_param(self, url: str, page: int) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        params = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() != "page"
        ]
        params.append(("page", str(page)))
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=query))
