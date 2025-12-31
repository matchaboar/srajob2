from __future__ import annotations

import html as html_lib
import json
import math
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from .base import BaseSiteHandler

UBER_HOST_SUFFIX = "uber.com"
UBER_BASE_URL = "https://www.uber.com"
CAREERS_LIST_TOKEN = "/careers/list"
DEFAULT_PAGE_SIZE = 10

_JOB_LINK_RE = re.compile(
    r"href=[\"'](?P<href>/[^\"']*?/careers/list/\d+)[\"']",
    flags=re.IGNORECASE,
)
_LISTING_LINK_RE = re.compile(
    r"href=[\"'](?P<href>/[^\"']*/careers/list/\?[^\"']*)[\"']",
    flags=re.IGNORECASE,
)
_OPEN_ROLES_RE = re.compile(
    r"(?P<count>\d{1,3}(?:,\d{3})*)\s*open\s*(?:&nbsp;|\u00a0|\s)roles",
    flags=re.IGNORECASE,
)


class UberCareersHandler(BaseSiteHandler):
    name = "uber_careers"
    site_type = "uber"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(UBER_HOST_SUFFIX):
            return False
        return CAREERS_LIST_TOKEN in (parsed.path or "").lower()

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").strip("/")
        if not path:
            return False
        segments = [segment for segment in path.split("/") if segment]
        if not segments:
            return False
        return segments[-1].lower() == "list"

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
            payload = self._build_listing_payload(uri)
            base_config["return_format"] = ["raw_html"]
            base_config["execution_scripts"] = {"*": self._build_execution_script(payload)}
            base_config["wait_for"] = {
                "selector": {
                    "selector": "#uber-jobs",
                    "timeout": {"secs": 20, "nanos": 0},
                },
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []

        payload = self._extract_results_payload(html)
        if payload:
            urls = self.get_links_from_json(payload)
            urls.extend(self.get_pagination_urls_from_json(payload))
            return self.filter_job_urls(urls)

        job_links = self._extract_job_links(html)
        page_links = self._build_pagination_urls(html, len(job_links))

        urls: List[str] = []
        seen: set[str] = set()
        for url in job_links + page_links:
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)

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
            lower = cleaned.lower()
            if CAREERS_LIST_TOKEN not in lower:
                continue
            if self.is_listing_url(cleaned) or re.search(r"/careers/list/\d+$", lower):
                seen.add(cleaned)
                filtered.append(cleaned)
        return filtered

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

        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                for item in results:
                    if not isinstance(item, dict):
                        continue
                    job_id = (
                        item.get("id")
                        or item.get("jobId")
                        or item.get("job_id")
                        or item.get("jobReqId")
                    )
                    if job_id is None:
                        continue
                    job_id_str = str(job_id).strip()
                    if not job_id_str:
                        continue
                    _add(urljoin(UBER_BASE_URL, f"{CAREERS_LIST_TOKEN}/{job_id_str}"))
        return urls

    def get_pagination_urls_from_json(
        self,
        payload: Any,
        source_url: str | None = None,
    ) -> List[str]:
        if not isinstance(payload, dict):
            return []
        total = self._extract_total_results(payload)
        results = payload.get("results") if isinstance(payload.get("results"), list) else []
        page_size = len(results) if results else payload.get("limit") or payload.get("__limit") or DEFAULT_PAGE_SIZE
        if not isinstance(page_size, int) or page_size <= 0:
            page_size = DEFAULT_PAGE_SIZE
        if not isinstance(total, int) or total <= page_size:
            return []

        base_url = source_url or payload.get("__source_url")
        if not isinstance(base_url, str) or not base_url.strip():
            return []
        base_url = self._strip_page_param(base_url)

        current_page = payload.get("page") if isinstance(payload.get("page"), int) else None
        if current_page is None:
            try:
                parsed = urlparse(base_url)
                params = dict(parse_qsl(parsed.query, keep_blank_values=True))
                current_page = int(params.get("page") or 0)
            except Exception:
                current_page = 0

        total_pages = max(1, math.ceil(total / page_size))
        urls: List[str] = []
        for page in range(total_pages):
            if page == current_page:
                continue
            if page == 0:
                urls.append(base_url)
            else:
                urls.append(self._set_page_param(base_url, page))
        return urls

    def _extract_job_links(self, html: str) -> List[str]:
        urls: List[str] = []
        seen: set[str] = set()
        for match in _JOB_LINK_RE.finditer(html):
            href = html_lib.unescape(match.group("href")).strip()
            if not href:
                continue
            absolute = urljoin(UBER_BASE_URL, href)
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
        if urls:
            return urls
        for match in re.findall(r"/careers/list/\d+", html, flags=re.IGNORECASE):
            absolute = urljoin(UBER_BASE_URL, match)
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
        return urls

    def _extract_results_payload(self, html: str) -> Optional[Dict[str, Any]]:
        match = re.search(r"<pre[^>]*>(?P<content>.*?)</pre>", html, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        content = html_lib.unescape(match.group("content")).strip()
        if not content:
            return None
        try:
            payload = json.loads(content)
        except Exception:
            return None
        if isinstance(payload, dict) and isinstance(payload.get("results"), list):
            return payload
        return None

    def _extract_total_results(self, payload: Dict[str, Any]) -> Optional[int]:
        total = payload.get("totalResults")
        if isinstance(total, dict):
            low = total.get("low")
            if isinstance(low, int):
                return low
            if isinstance(total.get("value"), int):
                return total.get("value")
        if isinstance(total, int):
            return total
        for key in ("total", "count", "totalCount"):
            value = payload.get(key)
            if isinstance(value, int):
                return value
        return None

    def _build_listing_payload(self, uri: str) -> Dict[str, Any]:
        try:
            parsed = urlparse(uri)
        except Exception:
            return {"limit": DEFAULT_PAGE_SIZE, "page": 0, "params": {}}
        query_params = parse_qsl(parsed.query, keep_blank_values=True)
        query = ""
        locations: List[Dict[str, str]] = []
        page = 0
        for key, value in query_params:
            if key == "query":
                query = value.strip()
            elif key == "location":
                parsed_loc = self._parse_location(value)
                if parsed_loc:
                    locations.append(parsed_loc)
            elif key == "page":
                try:
                    page = int(value)
                except Exception:
                    page = 0
        params: Dict[str, Any] = {}
        if query:
            params["query"] = query
        if locations:
            params["location"] = locations
        return {"limit": DEFAULT_PAGE_SIZE, "page": page, "params": params}

    def _parse_location(self, value: str) -> Optional[Dict[str, str]]:
        if not value:
            return None
        parts = [part.strip() for part in value.split("-") if part.strip()]
        if not parts:
            return None
        location: Dict[str, str] = {}
        if len(parts) >= 1:
            location["country"] = parts[0]
        if len(parts) >= 2:
            location["region"] = parts[1]
        if len(parts) >= 3:
            location["city"] = "-".join(parts[2:])
        return location

    def _build_execution_script(self, payload: Dict[str, Any]) -> str:
        payload_json = json.dumps(payload, separators=(",", ":"))
        return f"""
(function() {{
  const payload = {payload_json};
  fetch("/api/loadSearchJobsResults", {{
    method: "POST",
    headers: {{
      "content-type": "application/json",
      "accept": "application/json",
      "x-csrf-token": "x"
    }},
    credentials: "include",
    body: JSON.stringify(payload)
  }})
    .then((res) => res.json())
    .then((data) => {{
      data.__source_url = window.location.href;
      data.__page = payload.page;
      data.__limit = payload.limit;
      const pre = document.createElement("pre");
      pre.id = "uber-jobs";
      pre.textContent = JSON.stringify(data);
      document.body.innerHTML = "";
      document.body.appendChild(pre);
    }})
    .catch((err) => {{
      const pre = document.createElement("pre");
      pre.id = "uber-jobs";
      pre.textContent = JSON.stringify({{error: String(err)}});
      document.body.innerHTML = "";
      document.body.appendChild(pre);
    }});
}})();
"""

    def _build_pagination_urls(self, html: str, page_size: int) -> List[str]:
        total = self._extract_open_roles(html)
        if total is None or total <= 0:
            return []
        if page_size <= 0:
            page_size = DEFAULT_PAGE_SIZE
        if total <= page_size:
            return []
        base_url = self._extract_listing_url(html)
        if not base_url:
            return []
        base_url = self._strip_page_param(base_url)
        total_pages = max(1, math.ceil(total / page_size))
        urls: List[str] = []
        for page in range(1, total_pages):
            urls.append(self._set_page_param(base_url, page))
        return urls

    def _extract_listing_url(self, html: str) -> Optional[str]:
        for match in _LISTING_LINK_RE.finditer(html):
            href = html_lib.unescape(match.group("href")).strip()
            if not href:
                continue
            absolute = urljoin(UBER_BASE_URL, href)
            return absolute
        return None

    def _strip_page_param(self, url: str) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        params = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() != "page"
        ]
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=query))

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

    def _extract_open_roles(self, html: str) -> Optional[int]:
        text = html_lib.unescape(html).replace("\xa0", " ")
        match = _OPEN_ROLES_RE.search(text)
        if not match:
            return None
        raw = match.group("count").replace(",", "")
        try:
            return int(raw)
        except Exception:
            return None
