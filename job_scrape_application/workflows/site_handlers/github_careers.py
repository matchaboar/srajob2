from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse

from .base import BaseSiteHandler


class GithubCareersHandler(BaseSiteHandler):
    name = "github_careers"
    supports_listing_api = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith("github.careers")

    def is_listing_url(self, url: str) -> bool:
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        parts = [segment for segment in (parsed.path or "").split("/") if segment]
        if len(parts) < 2:
            return False
        if parts[0] != "careers-home" or parts[1] != "jobs":
            return False
        if len(parts) == 2:
            return True
        return parts[2] == "categories"

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        if not self.matches_url(uri):
            return None
        try:
            parsed = urlparse(uri)
        except Exception:
            return None
        if parsed.path.rstrip("/") == "/api/jobs":
            return uri

        query = parse_qs(parsed.query)
        query.pop("page", None)
        scheme = parsed.scheme or "https"
        host = parsed.hostname or "www.github.careers"
        base = f"{scheme}://{host}/api/jobs"
        return f"{base}?{urlencode(query, doseq=True)}" if query else base

    def _is_listing_api_url(self, uri: str) -> bool:
        """Check if URL is the GitHub Careers API endpoint (/api/jobs)."""
        if not self.matches_url(uri):
            return False
        try:
            parsed = urlparse(uri)
        except Exception:
            return False
        return parsed.path.rstrip("/") == "/api/jobs"

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        # API URLs return raw JSON - use basic request (no browser rendering)
        if self._is_listing_api_url(uri):
            return {
                "request": "basic",
                "return_format": ["raw"],
                "follow_redirects": True,
                "redirect_policy": "Loose",
                "external_domains": ["*"],
                "preserve_host": True,
            }
        # Regular pages need browser rendering
        return {"return_format": ["raw_html"]}

    def _is_valid_job_detail_url(self, url: str) -> bool:
        """Check if a URL is a valid GitHub Careers job detail page.

        Valid job URLs follow the pattern /careers-home/jobs/{job_slug}
        where job_slug is not 'categories' and is a specific job identifier.
        """
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        parts = [segment for segment in (parsed.path or "").split("/") if segment]
        # Valid job detail URL: /careers-home/jobs/{job_slug}
        if len(parts) != 3:
            return False
        if parts[0] != "careers-home" or parts[1] != "jobs":
            return False
        # Exclude navigation pages like /careers-home/jobs/categories
        if parts[2] in {"categories", "locations", "teams"}:
            return False
        return True

    def _looks_like_navigation_url(self, url: str) -> bool:
        """Check if a URL looks like a navigation/filter page rather than a job detail.

        Navigation URLs include:
        - /jobs with query params (filter pages)
        - /life-at-github, /benefits, /experienced-professionals, etc.
        - Any path ending in /categories, /locations, /teams
        """
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").lower().rstrip("/")
        parts = [segment for segment in path.split("/") if segment]

        # Filter pages: /jobs or /jobs?...
        if path == "/jobs" or (len(parts) == 1 and parts[0] == "jobs"):
            return True

        # Navigation pages ending in common navigation segments
        navigation_segments = {"categories", "locations", "teams"}
        if parts and parts[-1] in navigation_segments:
            return True

        # Top-level navigation pages
        navigation_prefixes = {
            "life-at-github",
            "benefits",
            "experienced-professionals",
            "early-in-profession",
            "careers-home",
        }
        # Check if path is just a top-level page without job detail
        if len(parts) == 1 and parts[0] in navigation_prefixes:
            return True
        if len(parts) == 2 and parts[0] in navigation_prefixes and parts[1] not in {"jobs"}:
            return True

        return False

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        """Filter URLs to only include valid GitHub Careers job detail pages."""
        filtered: List[str] = []
        for url in urls:
            # Skip if not a github.careers URL
            if not self.matches_url(url):
                continue
            # Skip navigation/filter pages
            if self._looks_like_navigation_url(url):
                continue
            # Only include valid job detail URLs
            if self._is_valid_job_detail_url(url):
                filtered.append(url)
        return filtered

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
            data = job.get("data") if isinstance(job.get("data"), dict) else job
            slug = data.get("slug") if isinstance(data, dict) else None
            if not isinstance(slug, str) or not slug.strip():
                continue
            language = data.get("language") if isinstance(data, dict) else None
            if not isinstance(language, str) or not language.strip():
                languages = data.get("languages") if isinstance(data, dict) else None
                if isinstance(languages, list) and languages and isinstance(languages[0], str):
                    language = languages[0]
            lang = (language or "en-us").lower()
            url = f"https://www.github.careers/careers-home/jobs/{slug.strip()}?lang={lang}"
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls
