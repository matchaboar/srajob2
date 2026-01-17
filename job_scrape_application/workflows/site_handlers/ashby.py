from __future__ import annotations

import html as html_lib
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .base import BaseSiteHandler
from ..helpers.link_extractors import fix_scheme_slashes, strip_wrapping_url
from ..helpers.regex_patterns import ASHBY_JOB_URL_PATTERN

# Pattern to find Ashby's embedded JavaScript data blob
_ASHBY_JS_DATA_PATTERN = re.compile(
    r';\s*fetch\s*\(\s*"https://cdn\.ashbyprd\.com/.*?</script>',
    flags=re.DOTALL,
)
# Pattern to extract secondaryLocationNames from Ashby's embedded data
_SECONDARY_LOCATIONS_PATTERN = re.compile(
    r'"secondaryLocationNames"\s*:\s*\[([^\]]*)\]'
)
# Pattern to extract title from <title> tag
_TITLE_TAG_PATTERN = re.compile(
    r"<title[^>]*>([^<]+)</title>",
    flags=re.IGNORECASE,
)
# Pattern to extract og:title from meta tag
_OG_TITLE_PATTERN = re.compile(
    r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']',
    flags=re.IGNORECASE,
)
# Alternative og:title pattern (content before property)
_OG_TITLE_PATTERN_ALT = re.compile(
    r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:title["\']',
    flags=re.IGNORECASE,
)
# Pattern to detect "About {Company}" titles that should be rejected
_ABOUT_COMPANY_PATTERN = re.compile(
    r"^about\s+\w+",
    flags=re.IGNORECASE,
)


class AshbyHqHandler(BaseSiteHandler):
    name = "ashby"
    supports_listing_api = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith("ashbyhq.com")

    def _job_board_slug(self, url: str) -> Optional[str]:
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
        if len(segments) >= 3 and segments[0] == "posting-api" and segments[1] == "job-board":
            return segments[2]
        return segments[0]

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        slug = self._job_board_slug(uri)
        if not slug:
            return None
        return f"https://api.ashbyhq.com/posting-api/job-board/{slug}"

    def get_api_uri(self, uri: str) -> Optional[str]:
        # Only transform listing URLs to API; detail URLs should stay as-is
        if self.is_listing_url(uri):
            return self.get_listing_api_uri(uri)
        return None

    def get_company_uri(self, uri: str) -> Optional[str]:
        slug = self._job_board_slug(uri)
        if not slug:
            return None
        return f"https://jobs.ashbyhq.com/{slug}"

    def is_listing_url(self, url: str) -> bool:
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        segments = [seg for seg in (parsed.path or "").split("/") if seg]
        if not segments:
            return False
        if len(segments) >= 3 and segments[0] == "posting-api" and segments[1] == "job-board":
            return len(segments) == 3
        return len(segments) == 1

    def get_links_from_json(self, payload: Any) -> List[str]:
        jobs = payload.get("jobs") if isinstance(payload, dict) else None
        if not isinstance(jobs, list):
            return []
        url_keys = ("jobUrl", "applyUrl", "jobPostingUrl", "postingUrl", "url")
        urls: List[str] = []
        seen: set[str] = set()
        for job in jobs:
            if not isinstance(job, dict):
                continue
            for key in url_keys:
                value = job.get(key)
                if isinstance(value, str) and value.strip():
                    url = value.strip()
                    if url not in seen:
                        seen.add(url)
                        urls.append(url)
        return urls

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for match in re.findall(ASHBY_JOB_URL_PATTERN, html):
            url = match.strip()
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        filtered: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = strip_wrapping_url(url)
            if not cleaned:
                continue
            cleaned = fix_scheme_slashes(cleaned)
            cleaned = self._strip_application_suffix(cleaned)
            if cleaned in seen:
                continue
            try:
                parsed = urlparse(cleaned)
            except Exception:
                parsed = None
            host = (parsed.hostname or "").lower() if parsed else ""
            path = parsed.path or "" if parsed else ""
            if host:
                if not host.endswith("ashbyhq.com"):
                    continue
                if host.startswith("api."):
                    continue
            segments = [seg for seg in path.split("/") if seg]
            if segments and segments[0].lower() == "posting-api":
                continue
            if len(segments) < 2:
                continue
            lower = cleaned.lower()
            if lower.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            if self._looks_like_non_job_detail_url(cleaned):
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    @staticmethod
    def _strip_application_suffix(url: str) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        host = (parsed.hostname or "").lower()
        if not host.endswith("ashbyhq.com"):
            return url
        path = parsed.path or ""
        stripped_path = path.rstrip("/")
        if not stripped_path.endswith("/application"):
            return url
        trimmed = stripped_path[: -len("/application")] or "/"
        return parsed._replace(path=trimmed).geturl()

    @staticmethod
    def _looks_like_non_job_detail_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.scheme and parsed.scheme not in {"http", "https"}:
            return True
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
        if not host or not path:
            return False
        if any(token in path for token in ("http://", "https://", "http:/", "https:/")):
            return True
        segments = [seg for seg in path.split("/") if seg]
        if not host.endswith("ashbyhq.com"):
            if any(seg in {"apply", "application", "hvhapply"} for seg in segments):
                return True
        if host.endswith("linkedin.com") and path.startswith("/company/"):
            return True
        return False

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        # Ashby job detail pages can render sparse commonmark; raw HTML preserves
        # meta/JSON-LD descriptions for normalization.
        return_format = ["raw_html"]
        base_config = {
            "request": "chrome",
            "return_format": return_format,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }
        return self._apply_page_links_config(base_config)

    def normalize_markdown(self, markdown: str) -> Tuple[str, Optional[str]]:
        """
        Clean Ashby raw HTML by removing embedded JavaScript/JSON blocks
        that would otherwise pollute the job description.

        Also extracts title from HTML when JSON-LD is not available.
        Priority: <title> tag > og:title meta tag.
        Rejects "About {Company}" patterns as they are not job titles.
        """
        if not markdown:
            return markdown, None

        # Extract title before cleaning (need original HTML for meta tags)
        title = self._extract_title_from_html(markdown)

        # Remove the large JavaScript data blob that Ashby embeds
        # This contains internal form fields, feature flags, etc.
        cleaned = _ASHBY_JS_DATA_PATTERN.sub("</script>", markdown)

        # Remove inline script blocks entirely
        cleaned = re.sub(
            r"<script\b[^>]*>.*?</script>",
            "",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )

        # Remove style blocks
        cleaned = re.sub(
            r"<style\b[^>]*>.*?</style>",
            "",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )

        # Remove <head> section entirely if present
        cleaned = re.sub(
            r"<head\b[^>]*>.*?</head>",
            "",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )

        # Remove stray <title>, <meta>, and <link> tags that might be outside <head>
        cleaned = re.sub(
            r"<title\b[^>]*>.*?</title>",
            "",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )
        cleaned = re.sub(
            r"<meta\b[^>]*>",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"<link\b[^>]*>",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )

        return cleaned, title

    def _extract_title_from_html(self, html: str) -> Optional[str]:
        """
        Extract job title from HTML when JSON-LD structured data is not available.

        Priority:
        1. <title> tag (often contains "Job Title @ Company")
        2. og:title meta tag

        Returns None if title looks like "About {Company}" pattern,
        which indicates the description heading was incorrectly used as title.
        """
        if not html:
            return None

        title: Optional[str] = None

        # Try <title> tag first
        title_match = _TITLE_TAG_PATTERN.search(html)
        if title_match:
            raw_title = html_lib.unescape(title_match.group(1)).strip()
            # Ashby titles often have format "Job Title @ Company"
            if " @ " in raw_title:
                title = raw_title.split(" @ ", 1)[0].strip()
            elif " | " in raw_title:
                title = raw_title.split(" | ", 1)[0].strip()
            else:
                title = raw_title

        # Fall back to og:title if no title found
        if not title:
            og_match = _OG_TITLE_PATTERN.search(html) or _OG_TITLE_PATTERN_ALT.search(html)
            if og_match:
                title = html_lib.unescape(og_match.group(1)).strip()

        # Reject "About {Company}" patterns - these are description headings, not job titles
        if title and _ABOUT_COMPANY_PATTERN.match(title):
            return "Unknown Engineer"

        return title if title else None

    def extract_location_hint(self, markdown: str) -> Optional[str]:
        """
        Extract location from Ashby page content.

        Priority order:
        1. JSON-LD jobLocation (Schema.org structured data) - most authoritative
        2. secondaryLocationNames from Ashby's embedded JavaScript data

        If the job has both remote and physical locations, returns a combined string
        like "Remote; San Francisco, CA" so that remote detection works properly.
        """
        if not markdown:
            return None

        # First, try to extract from JSON-LD (most authoritative)
        json_ld_location = self._extract_location_from_json_ld(markdown)
        if json_ld_location:
            return json_ld_location

        # Fall back to secondaryLocationNames from embedded data
        match = _SECONDARY_LOCATIONS_PATTERN.search(markdown)
        if not match:
            return None

        try:
            # Parse the array contents - they're JSON string values
            array_content = match.group(1).strip()
            if not array_content:
                return None

            # Parse as JSON array
            locations = json.loads(f"[{array_content}]")
            if not isinstance(locations, list) or not locations:
                return None

            # Filter and format locations
            cleaned_locations: List[str] = []
            for loc in locations:
                if isinstance(loc, str) and loc.strip():
                    cleaned_locations.append(loc.strip())

            if not cleaned_locations:
                return None

            # Check if any location is remote
            has_remote = any("remote" in loc.lower() for loc in cleaned_locations)
            non_remote = [
                loc for loc in cleaned_locations if "remote" not in loc.lower()
            ]

            # If remote and has physical locations, combine them for proper detection
            if has_remote and non_remote:
                return f"Remote; {non_remote[0]}"
            elif has_remote:
                return "Remote"
            elif non_remote:
                return non_remote[0]
            return cleaned_locations[0]
        except Exception:
            return None

    def extract_company(self, payload: Any, url: str) -> Optional[str]:
        """
        Extract company name from Ashby page content.

        Priority order:
        1. JSON-LD hiringOrganization.name (from structured data dict)
        2. JSON-LD hiringOrganization.name (from raw HTML string)
        3. Page title pattern "Title @ Company"
        """
        # Handle dict payload (structured data)
        if isinstance(payload, dict):
            # Check for hiringOrganization.name in structured data
            hiring_org = payload.get("hiringOrganization")
            if isinstance(hiring_org, dict):
                name = hiring_org.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()
            return None

        # Handle string payload (raw HTML)
        if not isinstance(payload, str):
            return None

        html = payload

        # Try JSON-LD hiringOrganization first
        company = self._extract_company_from_json_ld(html)
        if company:
            return company

        # Fall back to page title pattern "Title @ Company"
        title_match = _TITLE_TAG_PATTERN.search(html)
        if title_match:
            raw_title = html_lib.unescape(title_match.group(1)).strip()
            if " @ " in raw_title:
                # Extract company after the @ symbol
                company = raw_title.split(" @ ", 1)[1].strip()
                if company:
                    return company

        return None

    def _extract_company_from_json_ld(self, html: str) -> Optional[str]:
        """
        Extract company from JSON-LD JobPosting structured data.

        Looks for <script type="application/ld+json"> blocks containing JobPosting
        with hiringOrganization.name field.
        """
        if not html:
            return None

        try:
            # Find JSON-LD script blocks
            json_ld_pattern = re.compile(
                r'<script[^>]*application/ld\+json[^>]*>(.+?)</script>',
                re.DOTALL | re.IGNORECASE,
            )
            for match in json_ld_pattern.finditer(html):
                try:
                    data = json.loads(match.group(1))
                    if not isinstance(data, dict):
                        continue
                    if data.get("@type") != "JobPosting":
                        continue

                    # Extract company from hiringOrganization.name
                    hiring_org = data.get("hiringOrganization")
                    if isinstance(hiring_org, dict):
                        name = hiring_org.get("name")
                        if isinstance(name, str) and name.strip():
                            return name.strip()
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue
        except Exception:
            pass

        return None

    def _extract_location_from_json_ld(self, html: str) -> Optional[str]:
        """
        Extract location from JSON-LD JobPosting structured data.

        Looks for <script type="application/ld+json"> blocks containing JobPosting
        with jobLocation.address fields.
        """
        if not html:
            return None

        try:
            # Find JSON-LD script blocks
            json_ld_pattern = re.compile(
                r'<script[^>]*application/ld\+json[^>]*>(.+?)</script>',
                re.DOTALL | re.IGNORECASE,
            )
            for match in json_ld_pattern.finditer(html):
                try:
                    data = json.loads(match.group(1))
                    if not isinstance(data, dict):
                        continue
                    if data.get("@type") != "JobPosting":
                        continue

                    # Extract location from jobLocation.address
                    job_location = data.get("jobLocation")
                    if not isinstance(job_location, dict):
                        continue

                    address = job_location.get("address")
                    if not isinstance(address, dict):
                        continue

                    # Build location string from addressLocality + addressRegion
                    locality = address.get("addressLocality")
                    region = address.get("addressRegion")

                    if locality and region:
                        return f"{locality}, {region}"
                    elif locality:
                        return locality
                    elif region:
                        return region
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue
        except Exception:
            pass

        return None
