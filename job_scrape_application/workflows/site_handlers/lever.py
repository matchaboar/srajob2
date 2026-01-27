from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .base import BaseSiteHandler
from ..helpers.link_extractors import fix_scheme_slashes, strip_wrapping_url

# Lever job detail URLs have UUID in path: /company/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
_LEVER_UUID_PATTERN = re.compile(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")

# Pattern to extract job URLs from HTML
_LEVER_JOB_URL_PATTERN = re.compile(
    r'https://jobs\.lever\.co/[a-zA-Z0-9_-]+/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
)


class LeverHandler(BaseSiteHandler):
    """Handler for Lever job boards (jobs.lever.co)."""

    name = "lever"
    needs_page_links = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith("lever.co")

    def _get_company_slug(self, url: str) -> Optional[str]:
        """Extract company slug from Lever URL."""
        if not self.matches_url(url):
            return None
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        path = parsed.path.strip("/")
        if not path:
            return None
        segments = [seg for seg in path.split("/") if seg]
        if not segments:
            return None
        # First segment is company slug (e.g., "zoox" from jobs.lever.co/zoox)
        # Skip if it's a non-company path like "img"
        slug = segments[0]
        if slug.lower() in {"img", "api", "static", "assets", "css", "js"}:
            return None
        return slug

    def get_company_uri(self, uri: str) -> Optional[str]:
        """Get the company's base listing URL."""
        slug = self._get_company_slug(uri)
        if not slug:
            return None
        return f"https://jobs.lever.co/{slug}"

    def is_listing_url(self, url: str) -> bool:
        """Check if URL is a listing page (not a job detail page)."""
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False

        # If URL has query params, it's a filter/listing page
        if parsed.query:
            return True

        segments = [seg for seg in (parsed.path or "").split("/") if seg]
        if not segments:
            return False

        # Skip non-company paths
        if segments[0].lower() in {"img", "api", "static", "assets", "css", "js"}:
            return False

        # Listing page has exactly 1 segment (company slug)
        # Detail page has 2 segments (company slug + job UUID)
        if len(segments) == 1:
            return True

        if len(segments) >= 2:
            # Check if second segment is a valid UUID (job detail)
            return not _LEVER_UUID_PATTERN.match(segments[1])

        return False

    def _is_valid_job_detail_url(self, url: str) -> bool:
        """Check if URL is a valid job detail URL."""
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False

        # Job detail URLs should not have query params
        if parsed.query:
            return False

        segments = [seg for seg in (parsed.path or "").split("/") if seg]

        # Must have exactly 2 segments: company slug and job UUID
        if len(segments) != 2:
            return False

        # First segment is company slug
        company_slug = segments[0]
        if company_slug.lower() in {"img", "api", "static", "assets", "css", "js"}:
            return False

        # Second segment must be a valid UUID
        job_id = segments[1]
        return bool(_LEVER_UUID_PATTERN.match(job_id))

    def get_links_from_raw_html(self, html: str) -> List[str]:
        """Extract job URLs from raw HTML content."""
        if not html:
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for match in _LEVER_JOB_URL_PATTERN.findall(html):
            url = match.strip()
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        """Filter URLs to only include valid job detail URLs."""
        filtered: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = strip_wrapping_url(url)
            if not cleaned:
                continue
            cleaned = fix_scheme_slashes(cleaned)

            # Normalize: remove trailing slashes and fragments
            try:
                parsed = urlparse(cleaned)
                # Remove fragment and reconstruct
                cleaned = parsed._replace(fragment="").geturl().rstrip("/")
            except Exception:
                continue

            if cleaned in seen:
                continue

            # Only include valid job detail URLs
            if not self._is_valid_job_detail_url(cleaned):
                continue

            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        """Get SpiderCloud configuration for Lever pages."""
        if not self.matches_url(uri):
            return {}
        base_config = {
            "request": "chrome",
            "return_format": ["commonmark", "raw_html"],
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "return_page_links": True,
        }
        return self._apply_page_links_config(base_config)

    def normalize_markdown(self, markdown: str) -> Tuple[str, Optional[str]]:
        """Clean up Lever page content."""
        if not markdown:
            return markdown, None

        # Remove script and style blocks
        cleaned = re.sub(
            r"<script\b[^>]*>.*?</script>",
            "",
            markdown,
            flags=re.IGNORECASE | re.DOTALL,
        )
        cleaned = re.sub(
            r"<style\b[^>]*>.*?</style>",
            "",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )

        return cleaned, None
