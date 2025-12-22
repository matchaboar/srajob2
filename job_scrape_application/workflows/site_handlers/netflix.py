from __future__ import annotations

import html as html_lib
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .base import BaseSiteHandler

NETFLIX_HOST_SUFFIX = "jobs.netflix.net"
LISTING_PATH = "/careers"
JOB_DETAIL_PATH_TOKEN = "/careers/job/"
API_PATH = "/api/apply/v2/jobs"
DEFAULT_PAGE_SIZE = 10

SMART_APPLY_PATTERN = re.compile(
    r"<code[^>]*id=\"smartApplyData\"[^>]*>(?P<content>.*?)</code>",
    flags=re.IGNORECASE | re.DOTALL,
)
PRE_PATTERN = re.compile(r"<pre[^>]*>(?P<content>.*?)</pre>", flags=re.IGNORECASE | re.DOTALL)


class NetflixHandler(BaseSiteHandler):
    name = "netflix"
    site_type = "netflix"
    supports_listing_api = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith(NETFLIX_HOST_SUFFIX)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").lower()
        if API_PATH in path:
            return True
        return path.rstrip("/") == LISTING_PATH

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        try:
            parsed = urlparse(uri)
        except Exception:
            return None
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(NETFLIX_HOST_SUFFIX):
            return None
        if API_PATH in (parsed.path or ""):
            return self._with_default_pagination(uri)
        params = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)]
        params = self._ensure_domain_param(params, domain="netflix.com")
        params = self._ensure_pagination_params(params, start=0, num=DEFAULT_PAGE_SIZE)
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(path=API_PATH, query=query, scheme="https"))

    def get_links_from_json(self, payload: Any) -> List[str]:
        positions = self._extract_positions(payload)
        return self._extract_position_urls(positions)

    def get_pagination_urls_from_json(self, payload: Any, source_url: str | None = None) -> List[str]:
        if not isinstance(payload, dict):
            return []
        count = payload.get("count")
        if not isinstance(count, int) or count <= 0:
            return []
        positions = self._extract_positions(payload)
        page_size = len(positions)
        start_current = self._extract_start_from_url(source_url)
        num_param = self._extract_num_from_url(source_url)
        if page_size <= 0:
            page_size = num_param or DEFAULT_PAGE_SIZE
        if count <= page_size:
            return []
        api_base, base_params = self._build_api_base(source_url, payload)
        if not api_base:
            return []
        if start_current is None:
            start_current = 0
        urls: list[str] = []
        for start in range(start_current + page_size, count, page_size):
            params = self._ensure_pagination_params(base_params, start=start, num=page_size)
            query = urlencode(params, doseq=True)
            urls.append(f"{api_base}?{query}")
        return urls

    def get_links_from_raw_html(self, html: str) -> List[str]:
        payload = self._extract_payload(html)
        if not payload:
            return []
        positions = self._extract_positions(payload)
        urls: List[str] = []
        seen: set[str] = set()
        for url in self._extract_position_urls(positions):
            if url not in seen:
                seen.add(url)
                urls.append(url)

        pagination_urls = self._build_pagination_urls(payload, html)
        for url in pagination_urls:
            if url not in seen:
                seen.add(url)
                urls.append(url)

        return urls

    def _extract_start_from_url(self, uri: str | None) -> Optional[int]:
        return self._extract_int_param(uri, "start")

    def _extract_num_from_url(self, uri: str | None) -> Optional[int]:
        return self._extract_int_param(uri, "num")

    def _extract_int_param(self, uri: str | None, key: str) -> Optional[int]:
        if not uri:
            return None
        try:
            parsed = urlparse(uri)
        except Exception:
            return None
        for param_key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if param_key.lower() != key.lower():
                continue
            try:
                parsed_val = int(str(value))
            except Exception:
                return None
            return parsed_val if parsed_val >= 0 else None
        return None

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        try:
            parsed = urlparse(uri)
        except Exception:
            parsed = None
        path = (parsed.path or "").lower() if parsed else ""
        base_config = {
            "request": "chrome",
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }
        if API_PATH in uri:
            base_config["return_format"] = ["raw_html"]
            return self._apply_page_links_config(base_config)
        if JOB_DETAIL_PATH_TOKEN in path:
            base_config["request"] = "chrome"
            base_config["return_format"] = ["commonmark"]
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        base_config["wait_for"] = {
            "selector": {
                "selector": "#smartApplyData",
                "timeout": {"secs": 15, "nanos": 0},
            }
        }
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
            lower = cleaned.lower()
            if "/careers/job/" not in lower and API_PATH not in lower:
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    def _extract_payload(self, html: str) -> Optional[Dict[str, Any]]:
        if not html:
            return None
        match = SMART_APPLY_PATTERN.search(html) or PRE_PATTERN.search(html)
        if not match:
            return None
        content = html_lib.unescape(match.group("content")).strip()
        if not content:
            return None
        parsed = self._parse_json_blob(content)
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and ("positions" in item or "jobs" in item):
                    return item
        return None

    def _parse_json_blob(self, text: str) -> Any | None:
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if parsed is not None:
            return parsed
        match = re.search(r"{.*}", text, flags=re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                return None
        return None

    def _extract_positions(self, payload: Any) -> List[Dict[str, Any]]:
        positions = payload.get("positions") if isinstance(payload, dict) else None
        if not isinstance(positions, list):
            return []
        return [pos for pos in positions if isinstance(pos, dict)]

    def _extract_position_urls(self, positions: List[Dict[str, Any]]) -> List[str]:
        urls: List[str] = []
        seen: set[str] = set()
        for position in positions:
            url = position.get("canonicalPositionUrl")
            if isinstance(url, str) and url.strip():
                cleaned = url.strip()
                if cleaned not in seen:
                    seen.add(cleaned)
                    urls.append(cleaned)
        return urls

    def _build_pagination_urls(self, payload: Dict[str, Any], html: str) -> List[str]:
        count = payload.get("count") if isinstance(payload, dict) else None
        if not isinstance(count, int) or count <= 0:
            return []
        positions = self._extract_positions(payload)
        page_size = len(positions) or DEFAULT_PAGE_SIZE
        if count <= page_size:
            return []

        listing_url = self._extract_listing_url(html)
        api_base, base_params = self._build_api_base(listing_url, payload)
        if not api_base:
            return []

        urls: List[str] = []
        for start in range(page_size, count, page_size):
            params = self._ensure_pagination_params(base_params, start=start, num=page_size)
            query = urlencode(params, doseq=True)
            urls.append(f"{api_base}?{query}")
        return urls

    def _extract_listing_url(self, html: str) -> Optional[str]:
        patterns = (
            r"rel=\"canonical\"[^>]+href=\"(?P<url>[^\"]+)\"",
            r"property=\"og:url\"[^>]+content=\"(?P<url>[^\"]+)\"",
            r"name=\"og:url\"[^>]+content=\"(?P<url>[^\"]+)\"",
        )
        for pattern in patterns:
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                return html_lib.unescape(match.group("url"))
        return None

    def _build_api_base(
        self, listing_url: Optional[str], payload: Dict[str, Any]
    ) -> tuple[Optional[str], list[tuple[str, str]]]:
        host = None
        params: list[tuple[str, str]] = []
        if listing_url:
            try:
                parsed = urlparse(listing_url)
            except Exception:
                parsed = None
            if parsed:
                host = parsed.hostname or None
                params = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)]
        params = self._strip_pagination_params(params)
        query_block = payload.get("query") if isinstance(payload, dict) else None
        params = self._ensure_domain_param(params, payload.get("domain") if isinstance(payload, dict) else None)
        params = self._ensure_query_param(params, query_block)
        params = self._ensure_query_block_param(params, query_block, "Region")
        params = self._ensure_query_block_param(params, query_block, "pid")
        params = self._ensure_query_block_param(params, query_block, "location")
        if not host:
            host = f"explore.{NETFLIX_HOST_SUFFIX}"
        return f"https://{host}{API_PATH}", params

    def _strip_pagination_params(self, params: list[tuple[str, str]]) -> list[tuple[str, str]]:
        return [
            (key, value)
            for key, value in params
            if key.lower() not in {"start", "num", "page", "offset"}
        ]

    def _ensure_domain_param(
        self, params: list[tuple[str, str]], domain: Optional[str]
    ) -> list[tuple[str, str]]:
        if any(key.lower() == "domain" for key, _ in params):
            return params
        if isinstance(domain, str) and domain.strip():
            return params + [("domain", domain.strip())]
        return params

    def _ensure_query_param(
        self, params: list[tuple[str, str]], query_block: Optional[Dict[str, Any]]
    ) -> list[tuple[str, str]]:
        if any(key.lower() == "query" for key, _ in params):
            return params
        if not isinstance(query_block, dict):
            return params
        query_val = query_block.get("query")
        if isinstance(query_val, str) and query_val.strip():
            return params + [("query", query_val.strip())]
        return params

    def _ensure_query_block_param(
        self,
        params: list[tuple[str, str]],
        query_block: Optional[Dict[str, Any]],
        key: str,
    ) -> list[tuple[str, str]]:
        if any(param_key.lower() == key.lower() for param_key, _ in params):
            return params
        if not isinstance(query_block, dict):
            return params
        value = query_block.get(key)
        if isinstance(value, list):
            additions = [(key, str(item)) for item in value if isinstance(item, (str, int, float))]
            return params + additions if additions else params
        if isinstance(value, (str, int, float)) and str(value).strip():
            return params + [(key, str(value).strip())]
        return params

    def _ensure_pagination_params(
        self, params: list[tuple[str, str]], *, start: int, num: int
    ) -> list[tuple[str, str]]:
        stripped = self._strip_pagination_params(params)
        stripped.append(("start", str(start)))
        stripped.append(("num", str(num)))
        return stripped

    def _with_default_pagination(self, uri: str) -> str:
        try:
            parsed = urlparse(uri)
        except Exception:
            return uri
        params = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)]
        params = self._ensure_pagination_params(params, start=0, num=DEFAULT_PAGE_SIZE)
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=query))
