from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

from .base import BaseSiteHandler

SPOTIFY_HOST_SUFFIX = "lifeatspotify.com"
SPOTIFY_BASE_URL = "https://www.lifeatspotify.com"
JOBS_PATH = "/jobs"
SPOTIFY_JOBS_API_URL = "https://api.lifeatspotify.com/wp-json/animal/v1/job/search"

_DATA_INFO_RE = re.compile(r"data-info=\"(?P<slug>[^\"]+)\"")


class LifeAtSpotifyHandler(BaseSiteHandler):
    name = "lifeatspotify"
    site_type = "spotify"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host or not host.endswith(SPOTIFY_HOST_SUFFIX):
            return False
        path = (parsed.path or "").lower()
        return path.startswith(JOBS_PATH)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").rstrip("/")
        return path == JOBS_PATH

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        """Return the Spotify Jobs API URL for listing pages."""
        if not self.matches_url(uri) or not self.is_listing_url(uri):
            return None
        return SPOTIFY_JOBS_API_URL

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
            script = self._build_execution_script()
            base_config["execution_scripts"] = {"*": script}
            base_config["exuecution_scripts"] = {"*": script}
            base_config["wait_for"] = {
                "selector": {
                    "selector": "#spotify-jobs",
                    "timeout": {"secs": 20, "nanos": 0},
                },
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark"]
        return self._apply_page_links_config(base_config)

    def get_links_from_raw_html(self, html: str) -> List[str]:
        payload = self._extract_spotify_json_payload(html)
        if payload:
            urls = self.get_links_from_json(payload)
            if urls:
                return self.filter_job_urls(urls)
        if not html:
            return []
        urls = []
        seen: set[str] = set()
        for match in _DATA_INFO_RE.finditer(html):
            slug = match.group("slug").strip()
            if not slug:
                continue
            url = urljoin(SPOTIFY_BASE_URL, f"{JOBS_PATH}/{slug}")
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return self.filter_job_urls(urls)

    def _extract_spotify_json_payload(self, html: str) -> Optional[Dict[str, Any]]:
        """Extract JSON payload from Spotify API response.

        The Spotify API returns JSON with 'result' key containing jobs,
        which differs from the standard 'jobs' or 'positions' keys.
        """
        import orjson
        from ..helpers.regex_patterns import PRE_PATTERN

        if not isinstance(html, str) or not html:
            return None
        match = PRE_PATTERN.search(html)
        if not match:
            return None
        content = html_lib.unescape(match.group("content")).strip()
        if not content:
            return None
        try:
            parsed = orjson.loads(content)
        except Exception:
            return None
        if not isinstance(parsed, dict):
            return None
        # Handle Spotify API format with 'result' key
        if "result" in parsed:
            return {"jobs": parsed.get("result", []), "__source_url": None}
        # Fallback to standard format
        if "jobs" in parsed or "positions" in parsed:
            return parsed
        return None

    def get_links_from_json(self, payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            return []
        location_filters, category_filters = self._extract_filters(payload.get("__source_url"))
        urls: List[str] = []
        seen: set[str] = set()
        for job in jobs:
            if not isinstance(job, dict):
                continue
            if not self._matches_filters(job, location_filters, category_filters):
                continue
            # New API format: job has 'id' field which is the slug
            slug = job.get("id")
            if not isinstance(slug, str) or not slug.strip():
                # Fallback to old format: slugify the 'text' field
                title = job.get("text")
                if not isinstance(title, str) or not title.strip():
                    continue
                slug = self._slugify(title)
            if not slug:
                continue
            url = urljoin(SPOTIFY_BASE_URL, f"{JOBS_PATH}/{slug}")
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
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
            if not host.endswith(SPOTIFY_HOST_SUFFIX):
                continue
            path = (parsed.path or "").rstrip("/")
            if path == JOBS_PATH:
                normalized = urljoin(SPOTIFY_BASE_URL, JOBS_PATH)
            elif path.startswith(f"{JOBS_PATH}/"):
                slug = path.split("/")[-1]
                if not slug:
                    continue
                normalized = urljoin(SPOTIFY_BASE_URL, f"{JOBS_PATH}/{slug}")
            else:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            filtered.append(normalized)
        return filtered

    def extract_company(self, payload: Any, url: str | None = None) -> Optional[str]:
        if url and self.matches_url(url):
            return "Spotify"
        return None

    def normalize_markdown(self, markdown: str) -> tuple[str, Optional[str]]:
        if not markdown:
            return "", None
        lines = markdown.splitlines()
        title: Optional[str] = None
        start_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            heading = self._extract_heading(line)
            if heading:
                title = heading
                start_idx = idx
                break
        if start_idx is not None:
            lines = lines[start_idx:]

        stop_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            normalized = self._normalize_line(line)
            if normalized.startswith("similar jobs"):
                stop_idx = idx
                break
            if normalized.startswith("quick clicks"):
                stop_idx = idx
                break
            if normalized.startswith("application "):
                stop_idx = idx
                break
            if normalized.startswith("demographic survey"):
                stop_idx = idx
                break
            if normalized.startswith("well take it from here"):
                stop_idx = idx
                break
        if stop_idx is not None:
            lines = lines[:stop_idx]

        cleaned_lines: List[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                cleaned_lines.append(line)
                continue
            normalized = self._normalize_line(line)
            if normalized in {"link copied to clipboard", "apply", "apply now"}:
                continue
            if normalized.startswith("apply now"):
                continue
            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned or markdown, title

    @staticmethod
    def _extract_heading(line: str) -> Optional[str]:
        match = re.match(r"^#+\s*(.+)$", line.strip())
        if not match:
            return None
        heading = match.group(1).strip()
        return heading if heading else None

    @staticmethod
    def _normalize_line(value: str) -> str:
        cleaned = value.strip()
        cleaned = cleaned.encode("ascii", "ignore").decode()
        cleaned = re.sub(r"^[#*\-\u2022]+", "", cleaned).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.lower()

    def _build_execution_script(self) -> str:
        return f"""
(function() {{
  const apiUrl = "{SPOTIFY_JOBS_API_URL}";
  const render = (payload) => {{
    const pre = document.createElement("pre");
    pre.id = "spotify-jobs";
    pre.textContent = JSON.stringify(payload);
    document.body.innerHTML = "";
    document.body.appendChild(pre);
  }};
  fetch(apiUrl)
    .then((res) => res.json())
    .then((data) => {{
      render({{ jobs: data.result || [], __source_url: window.location.href }});
    }})
    .catch((err) => {{
      render({{ jobs: [], error: String(err), __source_url: window.location.href }});
    }});
}})();
"""

    @staticmethod
    def _slugify(value: str) -> str:
        if not value:
            return ""
        cleaned = html_lib.unescape(value)
        cleaned = cleaned.replace("&", "")
        cleaned = re.sub(r"[^A-Za-z0-9]+", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
        return cleaned.replace(" ", "-")

    def _extract_filters(self, source_url: Any) -> Tuple[Set[str], Set[str]]:
        if not isinstance(source_url, str) or not source_url.strip():
            return set(), set()
        try:
            parsed = urlparse(source_url)
        except Exception:
            return set(), set()
        query = parse_qs(parsed.query)
        locations = {value.strip() for value in query.get("l", []) if value.strip()}
        categories = {value.strip() for value in query.get("c", []) if value.strip()}
        return locations, categories

    def _matches_filters(
        self,
        job: Dict[str, Any],
        location_filters: Set[str],
        category_filters: Set[str],
    ) -> bool:
        if location_filters:
            job_locations = self._extract_location_slugs(job)
            if not job_locations.intersection(location_filters):
                return False
        if category_filters:
            allowed_departments = self._allowed_departments(category_filters)
            if allowed_departments:
                department_slug = ""
                # New API format: main_category is a dict with slug
                main_category = job.get("main_category")
                if isinstance(main_category, dict):
                    slug = main_category.get("slug")
                    if isinstance(slug, str):
                        department_slug = slug.strip()
                # Fallback: old Lever API format with categories.department
                if not department_slug:
                    categories = job.get("categories") if isinstance(job.get("categories"), dict) else {}
                    department = categories.get("department") if isinstance(categories, dict) else None
                    department_slug = self._slugify(department) if isinstance(department, str) else ""
                if department_slug and department_slug not in allowed_departments:
                    return False
        return True

    def _extract_location_slugs(self, job: Dict[str, Any]) -> Set[str]:
        slugs: Set[str] = set()
        # New API format: locations is an array of {location, slug} objects
        locations = job.get("locations")
        if isinstance(locations, list):
            for loc in locations:
                if isinstance(loc, dict):
                    loc_slug = loc.get("slug")
                    if isinstance(loc_slug, str) and loc_slug.strip():
                        slugs.add(loc_slug.strip())
                    loc_name = loc.get("location")
                    if isinstance(loc_name, str) and loc_name.strip():
                        slugs.add(self._slugify(loc_name))
                        city = loc_name.split(",")[0].strip()
                        city_slug = self._slugify(city)
                        if city_slug:
                            slugs.add(city_slug)
        # Fallback: old Lever API format with categories.location
        categories = job.get("categories") if isinstance(job.get("categories"), dict) else {}
        if isinstance(categories, dict):
            location = categories.get("location")
            if isinstance(location, str):
                slugs.add(self._slugify(location))
                city = location.split(",")[0].strip()
                city_slug = self._slugify(city)
                if city_slug:
                    slugs.add(city_slug)
            all_locations = categories.get("allLocations")
            if isinstance(all_locations, list):
                for loc in all_locations:
                    if isinstance(loc, str):
                        slugs.add(self._slugify(loc))
                        city = loc.split(",")[0].strip()
                        city_slug = self._slugify(city)
                        if city_slug:
                            slugs.add(city_slug)
        return slugs

    def _allowed_departments(self, category_filters: Set[str]) -> Set[str]:
        if not category_filters:
            return set()
        engineering_filters = {
            "backend",
            "client-c",
            "data",
            "developer-tools-infrastructure",
            "engineering-leadership",
            "machine-learning",
            "mobile",
            "network-engineering-it",
            "security",
            "tech-research",
            "web",
        }
        data_filters = {
            "data-insights-leadership",
            "data-science",
            "machine-learning-data-research-insights",
            "tech-research-data-research-insights",
            "user-research",
        }
        design_filters = {
            "design-ops",
            "editorial-design",
            "internal-tools-design",
            "product-design",
            "ux-writing",
        }
        product_filters = {"product"}
        allowed: Set[str] = set()
        if category_filters.intersection(engineering_filters):
            # Old Lever format
            allowed.add("engineering")
        if category_filters.intersection(data_filters):
            # Old Lever format
            allowed.add("data-and-analytics")
            # New API format
            allowed.add("data-research-insights")
        if category_filters.intersection(design_filters):
            # Old Lever format
            allowed.add("design-and-user-experience")
            # New API format
            allowed.add("design")
        if category_filters.intersection(product_filters):
            allowed.add("product")
        # New API: also add engineering for engineering-related URL filters
        if category_filters.intersection(engineering_filters):
            allowed.add("engineering")
        return allowed
