from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from .base import BaseSiteHandler

CISCO_HOST_SUFFIX = "careers.cisco.com"
CISCO_BASE_URL = "https://careers.cisco.com"
LISTING_PATH_TOKEN = "/search-results"
JOB_PATH_TOKEN = "/job/"

_JOB_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://[^"\']+/global/en/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)
_JOB_LINK_REL_RE = re.compile(
    r'href=["\'](?P<href>/global/en/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)
_PAGINATION_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://[^"\']+/global/en/search-results[^"\']*from=\d+[^"\']*)["\']',
    flags=re.IGNORECASE,
)
_PAGINATION_LINK_REL_RE = re.compile(
    r'href=["\'](?P<href>/global/en/search-results[^"\']*from=\d+[^"\']*)["\']',
    flags=re.IGNORECASE,
)
_JOB_DESCRIPTION_HEADER_RE = re.compile(r"^#+\s*job description\b", flags=re.IGNORECASE)
_DROP_LINE_RE = re.compile(
    r"^(save job|apply now|share(?: via.*)?|job id\b|category\b|available in \d+ locations|job)$",
    flags=re.IGNORECASE,
)
_IMAGE_RE = re.compile(r"^!\[[^\]]*]\([^\)]*\)$")


class CiscoCareersHandler(BaseSiteHandler):
    name = "cisco_careers"
    site_type = "cisco"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        return bool(host) and host.endswith(CISCO_HOST_SUFFIX)

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.hostname and not self.matches_url(url):
            return False
        path = (parsed.path or "").lower()
        return LISTING_PATH_TOKEN in path

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
            base_config["wait_for"] = {
                "selector": {
                    "selector": "a[data-ph-at-id='job-link']",
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

        urls: List[str] = []
        seen: set[str] = set()

        def _add(url_val: str | None) -> None:
            if not url_val:
                return
            cleaned = html_lib.unescape(url_val).strip()
            if not cleaned:
                return
            if cleaned.startswith("/"):
                cleaned = urljoin(CISCO_BASE_URL, cleaned)
            if cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for match in _JOB_LINK_RE.finditer(html):
            _add(match.group("href"))
        for match in _JOB_LINK_REL_RE.finditer(html):
            _add(match.group("href"))
        for match in _PAGINATION_LINK_RE.finditer(html):
            _add(match.group("href"))
        for match in _PAGINATION_LINK_REL_RE.finditer(html):
            _add(match.group("href"))

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
            if "hvhapply" in lower:
                continue
            if self.is_listing_url(cleaned) or JOB_PATH_TOKEN in lower:
                seen.add(cleaned)
                filtered.append(cleaned)
        return filtered

    def extract_location_hint(self, markdown: str) -> Optional[str]:
        if not markdown:
            return None
        for line in markdown.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if _JOB_DESCRIPTION_HEADER_RE.match(stripped):
                break
            lower = stripped.lower()
            if "|" in stripped or "career" in lower:
                continue
            if "available in" in lower:
                continue
            if lower in {"job", "save job"}:
                continue
            if lower.startswith(("apply", "share")):
                continue
            if "job id" in lower or "category" in lower:
                continue
            if "http" in lower:
                continue
            if "," not in stripped:
                continue
            if stripped.endswith("."):
                continue
            return stripped
        return None

    def normalize_markdown(self, markdown: str) -> tuple[str, Optional[str]]:
        if not markdown:
            return "", None

        lines = markdown.splitlines()
        title: Optional[str] = None
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            heading_match = re.match(r"^#+\s*(.+)$", stripped)
            if not heading_match:
                continue
            if _JOB_DESCRIPTION_HEADER_RE.match(stripped):
                continue
            candidate = heading_match.group(1).strip()
            if not candidate:
                continue
            if candidate.lower() in {"job", "job description"}:
                continue
            title = candidate
            break
        start_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            if _JOB_DESCRIPTION_HEADER_RE.match(line.strip()):
                start_idx = idx
                break
        if start_idx is not None:
            lines = lines[start_idx:]

        cleaned_lines: List[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                cleaned_lines.append(line)
                continue
            if stripped == "-":
                continue
            if _DROP_LINE_RE.match(stripped):
                continue
            if _IMAGE_RE.match(stripped):
                continue
            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned or markdown, title
