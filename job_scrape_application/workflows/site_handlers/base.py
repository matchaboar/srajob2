from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


class BaseSiteHandler(ABC):
    """Base class for site-specific scraping helpers."""

    name: str = "base"
    site_type: str | None = None
    supports_listing_api: bool = False

    @classmethod
    @abstractmethod
    def matches_url(cls, url: str) -> bool:
        """Return True when this handler is appropriate for the supplied URL."""

    def matches_site(self, site_type: str | None, url: str | None = None) -> bool:
        if site_type and self.site_type and site_type == self.site_type:
            return True
        if url and self.matches_url(url):
            return True
        return False

    def get_api_uri(self, uri: str) -> Optional[str]:
        return None

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        return None

    def get_company_uri(self, uri: str) -> Optional[str]:
        return None

    def get_links_from_markdown(self, markdown: str) -> List[str]:
        return []

    def get_links_from_raw_html(self, html: str) -> List[str]:
        return []

    def get_links_from_json(self, payload: Any) -> List[str]:
        return []

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        return {}

    def get_firecrawl_config(self, uri: str) -> Dict[str, Any]:
        return {}

    def normalize_markdown(self, markdown: str) -> tuple[str, Optional[str]]:
        return markdown, None

    def is_api_detail_url(self, uri: str) -> bool:
        return False

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        return urls

    @staticmethod
    def _title_from_url(url: str) -> str:
        """Return a title-ish slug from a URL path (best-effort)."""

        try:
            parsed = urlparse(url)
        except Exception:
            parsed = None
        path = parsed.path if parsed else url
        slug = path.split("/")[-1] if "/" in path else path
        slug = slug.split("?")[0]
        slug = slug.replace("-", " ").replace("_", " ").strip()
        return slug
