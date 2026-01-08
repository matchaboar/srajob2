from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from .base import BaseSiteHandler

MICROSOFT_HOST_SUFFIX = "apply.careers.microsoft.com"
MICROSOFT_BASE_URL = "https://apply.careers.microsoft.com"
CAREERS_PATH = "/careers"
JOB_DETAIL_PATH = "/careers/job/"
API_SEARCH_PATH = "/api/pcsx/search"
API_DETAIL_PATH = "/api/pcsx/position_details"
DEFAULT_PAGE_SIZE = 10

_JOB_ID_RE = re.compile(r"/careers/job/(?P<job_id>\d+)", flags=re.IGNORECASE)


class MicrosoftCareersHandler(BaseSiteHandler):
    name = "microsoft_careers"
    site_type = "microsoft"
    supports_listing_api = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(MICROSOFT_HOST_SUFFIX):
            return False
        path = (parsed.path or "").lower()
        return (
            path.startswith(CAREERS_PATH)
            or path.startswith(API_SEARCH_PATH)
            or path.startswith(API_DETAIL_PATH)
        )

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").lower()
        if path.startswith(API_SEARCH_PATH):
            return True
        if path.rstrip("/") == CAREERS_PATH:
            return True
        return False

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        if not self.matches_url(uri):
            return None
        try:
            parsed = urlparse(uri)
        except Exception:
            return None

        params = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() != "pid"
        ]
        params = self._ensure_domain_param(params)
        params = self._ensure_start_param(params)

        api_path = API_SEARCH_PATH if not (parsed.path or "").lower().startswith(API_SEARCH_PATH) else parsed.path
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(path=api_path, query=query, scheme="https"))

    def get_api_uri(self, uri: str) -> Optional[str]:
        if not self.matches_url(uri):
            return None
        try:
            parsed = urlparse(uri)
        except Exception:
            return None

        path = (parsed.path or "").lower()
        if path.startswith(API_DETAIL_PATH):
            return urlunparse(parsed._replace(scheme="https"))

        job_id = self._extract_job_id(uri)
        if not job_id:
            return None

        params = [
            ("position_id", job_id),
            ("domain", "microsoft.com"),
            ("hl", "en"),
        ]
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(path=API_DETAIL_PATH, query=query, scheme="https"))

    def get_links_from_json(self, payload: Any) -> List[str]:
        urls: List[str] = []
        seen: set[str] = set()

        def _add(url: str) -> None:
            cleaned = url.strip()
            if not cleaned or cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for url in super().get_links_from_json(payload):
            _add(url)

        for position in self._extract_positions(payload):
            position_url = position.get("positionUrl") or position.get("url")
            if isinstance(position_url, str) and position_url.strip():
                absolute = urljoin(MICROSOFT_BASE_URL, position_url.strip())
                _add(absolute)
                continue
            job_id = position.get("id") or position.get("positionId")
            if job_id is None:
                continue
            job_id_str = str(job_id).strip()
            if not job_id_str:
                continue
            _add(urljoin(MICROSOFT_BASE_URL, f"{JOB_DETAIL_PATH}{job_id_str}"))

        return urls

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for match in _JOB_ID_RE.finditer(html):
            job_id = match.group("job_id")
            if not job_id:
                continue
            url = urljoin(MICROSOFT_BASE_URL, f"{JOB_DETAIL_PATH}{job_id}")
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return self.filter_job_urls(urls)

    def get_pagination_urls_from_json(
        self,
        payload: Any,
        source_url: str | None = None,
    ) -> List[str]:
        if not isinstance(payload, dict):
            return []
        count = payload.get("count")
        positions = self._extract_positions(payload)
        page_size = len(positions) or DEFAULT_PAGE_SIZE
        if not isinstance(count, int) or count <= page_size:
            return []

        base_url, base_params = self._build_api_base(source_url)
        if not base_url:
            return []

        start_current = self._extract_start_param(source_url) or 0
        urls: List[str] = []
        for start in range(start_current + page_size, count, page_size):
            params = [(key, value) for key, value in base_params if key.lower() != "start"]
            params.append(("start", str(start)))
            query = urlencode(params, doseq=True)
            urls.append(f"{base_url}?{query}" if query else base_url)
        return urls

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        try:
            parsed = urlparse(uri)
        except Exception:
            parsed = None
        path = (parsed.path or "").lower() if parsed else ""
        base_config: Dict[str, Any] = {
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }
        if path.startswith(API_SEARCH_PATH) or path.startswith(API_DETAIL_PATH):
            base_config.update({"request": "standard", "return_format": ["raw_html"]})
            return self._apply_page_links_config(base_config)
        if self.is_listing_url(uri):
            base_config.update({"request": "chrome", "return_format": ["raw_html"]})
            return self._apply_page_links_config(base_config)
        base_config.update({"request": "chrome", "return_format": ["raw_html"]})
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
            if not host.endswith(MICROSOFT_HOST_SUFFIX):
                continue
            path = (parsed.path or "").lower()
            if path.startswith(API_SEARCH_PATH) or path.startswith(JOB_DETAIL_PATH):
                seen.add(cleaned)
                filtered.append(cleaned)
        return filtered

    def extract_posted_at(self, payload: Any, url: str | None = None) -> Any | None:
        if isinstance(payload, dict):
            if "postedTs" in payload:
                return payload.get("postedTs")
            positions = self._extract_positions(payload)
            job_id = self._extract_job_id(url)
            if job_id is not None:
                for position in positions:
                    if str(position.get("id")) == job_id:
                        if "postedTs" in position:
                            return position.get("postedTs")
        return super().extract_posted_at(payload, url)

    def extract_company(self, payload: Any, url: str | None = None) -> Optional[str]:
        if url and self.matches_url(url):
            return "Microsoft"
        return None

    @staticmethod
    def _extract_positions(payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        positions = payload.get("positions")
        if not isinstance(positions, list):
            data = payload.get("data") if isinstance(payload.get("data"), dict) else None
            positions = data.get("positions") if isinstance(data, dict) else None
        if not isinstance(positions, list):
            return []
        return [pos for pos in positions if isinstance(pos, dict)]

    @staticmethod
    def _ensure_domain_param(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
        if any(key.lower() == "domain" for key, _ in params):
            return params
        return params + [("domain", "microsoft.com")]

    @staticmethod
    def _ensure_start_param(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
        if any(key.lower() == "start" for key, _ in params):
            return params
        return params + [("start", "0")]

    @staticmethod
    def _extract_start_param(source_url: str | None) -> Optional[int]:
        if not source_url:
            return None
        try:
            parsed = urlparse(source_url)
        except Exception:
            return None
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower() != "start":
                continue
            try:
                parsed_val = int(str(value))
            except Exception:
                return None
            return parsed_val if parsed_val >= 0 else None
        return None

    def _build_api_base(self, source_url: str | None) -> tuple[str | None, list[tuple[str, str]]]:
        if not source_url:
            return None, []
        try:
            parsed = urlparse(source_url)
        except Exception:
            return None, []
        params = list(parse_qsl(parsed.query, keep_blank_values=True))
        params = self._ensure_domain_param(params)
        params = self._ensure_start_param(params)
        path = parsed.path
        if not (path or "").lower().startswith(API_SEARCH_PATH):
            path = API_SEARCH_PATH
        base_url = urlunparse(parsed._replace(path=path, query=""))
        return base_url, params

    @staticmethod
    def _extract_job_id(url: str | None) -> Optional[str]:
        if not url:
            return None
        try:
            parsed = urlparse(url)
        except Exception:
            parsed = None
        if parsed:
            for key, value in parse_qsl(parsed.query, keep_blank_values=True):
                if key.lower() == "position_id":
                    job_id = str(value).strip()
                    return job_id if job_id else None
        match = _JOB_ID_RE.search(url)
        if match:
            return match.group("job_id")
        return None

    @staticmethod
    def _build_detail_execution_script() -> str:
        return """
(function() {
  try {
    const match = window.location.pathname.match(/\\/careers\\/job\\/(\\d+)/i);
    if (!match) return;
    const jobId = match[1];
    const params = new URLSearchParams({
      position_id: jobId,
      domain: "microsoft.com",
      hl: "en"
    });
    const url = `/api/pcsx/position_details?${params.toString()}`;
    fetch(url, { credentials: "include" })
      .then((response) => response.json())
      .then((data) => {
        const pre = document.createElement("pre");
        pre.id = "ms-job-json";
        pre.textContent = JSON.stringify(data);
        document.body.appendChild(pre);
      })
      .catch(() => {});
  } catch (err) {}
})();
"""
