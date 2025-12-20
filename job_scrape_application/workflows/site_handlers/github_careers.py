from __future__ import annotations

from typing import Any, List, Optional
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
