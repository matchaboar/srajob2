from __future__ import annotations

import html as html_lib
import json
import math
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from ..helpers.regex_patterns import PRE_PATTERN
from .base import BaseSiteHandler

META_HOST_SUFFIX = "metacareers.com"
META_BASE_URL = "https://www.metacareers.com"
JOBSEARCH_PATH = "/jobsearch"
JOB_DETAIL_PATH = "/jobs/"
GRAPHQL_ENDPOINT = "/api/graphql/"
RESULTS_PER_PAGE = 20
GRAPHQL_DOC_ID = "24330890369943030"

_JOB_ID_RE = re.compile(r"^\d+$")


class MetaCareersHandler(BaseSiteHandler):
    name = "meta_careers"
    site_type = "meta"
    supports_listing_api = True
    needs_page_links = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(META_HOST_SUFFIX):
            return False
        path = (parsed.path or "").lower()
        return path.startswith(JOBSEARCH_PATH) or path.startswith(JOB_DETAIL_PATH)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        return (parsed.path or "").lower().startswith(JOBSEARCH_PATH)

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
            base_config["execution_scripts"] = {"*": self._build_execution_script()}
            base_config["wait_for"] = {
                "selector": {
                    "selector": "#meta-jobs",
                    "timeout": {"secs": 30, "nanos": 0},
                },
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

    def get_links_from_raw_html(self, html: str) -> List[str]:
        payload = self._extract_results_payload(html)
        if not payload:
            return []
        urls = self.get_links_from_json(payload)
        urls.extend(self.get_pagination_urls_from_json(payload))
        return self.filter_job_urls(urls)

    def get_links_from_json(self, payload: Any) -> List[str]:
        urls: List[str] = []
        seen: set[str] = set()

        def _add(url: str) -> None:
            cleaned = url.strip()
            if not cleaned or cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for job in self._extract_jobs(payload):
            job_id = job.get("id") if isinstance(job, dict) else None
            if job_id is None:
                continue
            job_id_str = str(job_id).strip()
            if not job_id_str or not _JOB_ID_RE.match(job_id_str):
                continue
            _add(urljoin(META_BASE_URL, f"/jobs/{job_id_str}/"))
        return urls

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
            if not host.endswith(META_HOST_SUFFIX):
                continue
            if not path.startswith(JOB_DETAIL_PATH):
                continue
            job_id = path.strip("/").split("/")[-1]
            if not job_id or not _JOB_ID_RE.match(job_id):
                continue
            normalized = urljoin(META_BASE_URL, f"/jobs/{job_id}/")
            if normalized in seen:
                continue
            seen.add(normalized)
            filtered.append(normalized)
        return filtered

    def get_pagination_urls_from_json(
        self,
        payload: Any,
        source_url: str | None = None,
    ) -> List[str]:
        total = self._extract_total_jobs(payload)
        if not total:
            return []
        if total <= RESULTS_PER_PAGE:
            return []
        base_url = source_url or self._extract_source_url(payload)
        if not base_url:
            return []
        base_url = self._strip_page_param(base_url)
        current_page = self._extract_page(payload) or self._extract_page_from_url(base_url)
        if current_page < 1:
            current_page = 1
        total_pages = max(1, math.ceil(total / RESULTS_PER_PAGE))
        urls: List[str] = []
        for page in range(1, total_pages + 1):
            if page == current_page:
                continue
            if page == 1:
                urls.append(base_url)
            else:
                urls.append(self._set_page_param(base_url, page))
        return urls

    def _extract_results_payload(self, html: str) -> Optional[Dict[str, Any]]:
        if not isinstance(html, str) or not html:
            return None
        match = PRE_PATTERN.search(html)
        if not match:
            return None
        content = html_lib.unescape(match.group("content")).strip()
        if not content:
            return None
        try:
            parsed = json.loads(content)
        except Exception:
            parsed = None
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                parsed = None
        return parsed if isinstance(parsed, dict) else None

    def _extract_jobs(self, payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        if not isinstance(data, dict):
            return []
        search = data.get("job_search_with_featured_jobs")
        if not isinstance(search, dict):
            return []
        jobs: List[Dict[str, Any]] = []
        for key in ("featured_jobs", "all_jobs"):
            items = search.get(key)
            if isinstance(items, list):
                jobs.extend([item for item in items if isinstance(item, dict)])
        return jobs

    def _extract_total_jobs(self, payload: Any) -> int:
        if not isinstance(payload, dict):
            return 0
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        if not isinstance(data, dict):
            return 0
        search = data.get("job_search_with_featured_jobs")
        if not isinstance(search, dict):
            return 0
        all_jobs = search.get("all_jobs")
        if isinstance(all_jobs, list):
            return len(all_jobs)
        return 0

    def _extract_source_url(self, payload: Any) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        for key in ("__source_url", "source_url", "url"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _extract_page(self, payload: Any) -> Optional[int]:
        if not isinstance(payload, dict):
            return None
        page = payload.get("__page")
        if isinstance(page, int):
            return page
        if isinstance(page, str):
            try:
                return int(page)
            except Exception:
                return None
        return None

    def _extract_page_from_url(self, url: str) -> int:
        try:
            parsed = urlparse(url)
        except Exception:
            return 1
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower() == "page":
                try:
                    return max(1, int(value))
                except Exception:
                    return 1
        return 1

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

    def _build_execution_script(self) -> str:
        return f"""
(function() {{
  const listKeys = new Set([
    "divisions",
    "offices",
    "roles",
    "leadership_levels",
    "saved_jobs",
    "saved_searches",
    "sub_teams",
    "teams"
  ]);

  const filters = {{
    q: null,
    divisions: [],
    offices: [],
    roles: [],
    leadership_levels: [],
    saved_jobs: [],
    saved_searches: [],
    sub_teams: [],
    teams: [],
    is_leadership: false,
    is_remote_only: false,
    sort_by_new: false
  }};

  let page = 1;
  const params = new URLSearchParams(window.location.search);
  params.forEach((value, key) => {{
    const baseKey = key.includes("[") ? key.slice(0, key.indexOf("[")) : key;
    if (baseKey === "page") {{
      const parsed = parseInt(value, 10);
      page = Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
      return;
    }}
    if (baseKey === "q") {{
      filters.q = value ? value : null;
      return;
    }}
    if (baseKey === "is_leadership") {{
      filters.is_leadership = value === "1" || value === "true";
      return;
    }}
    if (baseKey === "is_remote_only") {{
      filters.is_remote_only = value === "true";
      return;
    }}
    if (baseKey === "sort_by_new") {{
      filters.sort_by_new = value === "true";
      return;
    }}
    if (listKeys.has(baseKey)) {{
      filters[baseKey].push(value);
    }}
  }});

  function readToken(moduleName, field) {{
    try {{
      if (window.require) {{
        const mod = window.require(moduleName);
        if (mod) {{
          if (field && mod[field]) return mod[field];
          if (typeof mod.getToken === "function") return mod.getToken();
        }}
      }}
    }} catch (err) {{}}
    return "";
  }}

  const dtsg = readToken("DTSG", "token") || readToken("DTSGInitData", "token");
  const lsd = readToken("LSD", "token");
  const siteData = (() => {{
    try {{
      return window.require ? window.require("SiteData") : null;
    }} catch (err) {{
      return null;
    }}
  }})();

  const variables = {{
    search_input: Object.assign({{}}, filters, {{ results_per_page: null }})
  }};

  const payload = new URLSearchParams();
  payload.set("doc_id", "{GRAPHQL_DOC_ID}");
  payload.set("variables", JSON.stringify(variables));
  payload.set("server_timestamps", "true");
  payload.set("fb_api_caller_class", "RelayModern");
  payload.set("fb_api_req_friendly_name", "CareersJobSearchResultsV3DataQuery");
  payload.set("__a", "1");
  payload.set("__comet_req", "1");
  payload.set("__user", "0");
  if (siteData && siteData.hsi) {{
    payload.set("__hsi", siteData.hsi);
  }}
  if (siteData && siteData.__spin_r) {{
    payload.set("__spin_r", siteData.__spin_r);
  }}
  if (siteData && siteData.__spin_b) {{
    payload.set("__spin_b", siteData.__spin_b);
  }}
  if (siteData && siteData.__spin_t) {{
    payload.set("__spin_t", siteData.__spin_t);
  }}
  if (dtsg) {{
    payload.set("fb_dtsg", dtsg);
  }}
  if (lsd) {{
    payload.set("lsd", lsd);
  }}

  const timeout = new Promise((_, reject) => {{
    window.setTimeout(() => reject(new Error("GraphQL timeout")), 20000);
  }});

  Promise.race([
    fetch("{GRAPHQL_ENDPOINT}", {{
      method: "POST",
      headers: {{
        "content-type": "application/x-www-form-urlencoded",
        "accept": "application/json"
      }},
      credentials: "include",
      body: payload.toString()
    }}).then((res) => res.text()),
    timeout
  ])
    .then((text) => {{
      let parsed;
      try {{
        parsed = JSON.parse(text);
      }} catch (err) {{
        parsed = {{ raw: text }};
      }}
      parsed.__source_url = window.location.href;
      parsed.__page = page;
      const pre = document.createElement("pre");
      pre.id = "meta-jobs";
      pre.textContent = JSON.stringify(parsed);
      document.body.innerHTML = "";
      document.body.appendChild(pre);
    }})
    .catch((err) => {{
      const pre = document.createElement("pre");
      pre.id = "meta-jobs";
      pre.textContent = JSON.stringify({{ error: String(err) }});
      document.body.innerHTML = "";
      document.body.appendChild(pre);
    }});
}})();
"""
