from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from .base import BaseSiteHandler

KULA_HOST = "careers.kula.ai"
KULA_API_PATH = "/api/internal/ats_job_posts"
KULA_SCOPE = "public"
KULA_TYPE = "ats_job_post.index"
KULA_ITEMS = 99


class KulaCareersHandler(BaseSiteHandler):
    name = "kula"
    site_type = "kula"
    supports_listing_api = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        return host.endswith(KULA_HOST)

    def is_listing_url(self, url: str) -> bool:
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = parsed.path or ""
        if path.startswith(KULA_API_PATH):
            return True
        segments = [segment for segment in path.split("/") if segment]
        if not segments:
            return False
        if segments[0] == "api":
            return True
        if len(segments) == 1:
            return True
        if len(segments) >= 2 and segments[1].isdigit():
            return False
        return True

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        if not self.matches_url(uri):
            return None
        try:
            parsed = urlparse(uri)
        except Exception:
            return None
        if parsed.path.startswith(KULA_API_PATH):
            return uri
        account_name = self._extract_account_name(uri)
        if not account_name:
            return None
        query = urlencode(
            {
                "accountName": account_name,
                "scope": KULA_SCOPE,
                "page": "1",
                "type": KULA_TYPE,
                "items": str(KULA_ITEMS),
            }
        )
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc or KULA_HOST
        return urlunparse((scheme, netloc, KULA_API_PATH, "", query, ""))

    def get_api_uri(self, uri: str) -> Optional[str]:
        return self.get_listing_api_uri(uri)

    def get_links_from_json(self, payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if not isinstance(data, list):
            return []
        ids: List[str] = []
        seen: set[str] = set()
        for job in data:
            if not isinstance(job, dict):
                continue
            job_id = job.get("id")
            if job_id is None:
                continue
            job_id_str = str(job_id).strip()
            if not job_id_str or job_id_str in seen:
                continue
            seen.add(job_id_str)
            ids.append(job_id_str)
        return ids

    def get_pagination_urls_from_json(self, payload: Any, source_url: str | None = None) -> List[str]:
        if not isinstance(payload, dict):
            return []
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            return []
        total_pages = meta.get("pages")
        if not isinstance(total_pages, int) or total_pages <= 1:
            return []
        current_page = meta.get("page")
        if not isinstance(current_page, int):
            current_page = self._extract_page_param(source_url) or 1
        if current_page >= total_pages or not source_url:
            return []
        return [self._set_page_param(source_url, page) for page in range(current_page + 1, total_pages + 1)]

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

    def filter_job_urls_for_site(self, urls: List[str], source_url: str | None) -> List[str]:
        account_name = self._extract_account_name(source_url or "")
        normalized: List[str] = []
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = url.strip()
            if not cleaned:
                continue
            if cleaned.isdigit():
                if not account_name:
                    continue
                cleaned = f"https://{KULA_HOST}/{account_name}/{cleaned}"
            elif cleaned.startswith("/"):
                cleaned = f"https://{KULA_HOST}{cleaned}"
            normalized.append(cleaned)
        return self.filter_job_urls(normalized)

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
            if not host.endswith(KULA_HOST):
                continue
            if not self._is_job_detail_path(parsed.path or ""):
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    def _extract_account_name(self, url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        query = parse_qs(parsed.query)
        account_name = query.get("accountName", [None])[0]
        if isinstance(account_name, str) and account_name.strip():
            return account_name.strip()
        segments = [segment for segment in (parsed.path or "").split("/") if segment]
        if not segments:
            return None
        if segments[0] == "api":
            return None
        return segments[0]

    def _is_job_detail_path(self, path: str) -> bool:
        segments = [segment for segment in path.split("/") if segment]
        if len(segments) < 2:
            return False
        if segments[0] == "api":
            return False
        return segments[1].isdigit()

    def _extract_page_param(self, url: str | None) -> Optional[int]:
        if not url:
            return None
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        for key, values in parse_qs(parsed.query).items():
            if key.lower() != "page" or not values:
                continue
            try:
                page_val = int(values[0])
            except Exception:
                return None
            return page_val if page_val >= 1 else None
        return None

    def _set_page_param(self, url: str, page: int) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        query = parse_qs(parsed.query)
        query["page"] = [str(page)]
        new_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=new_query))
