from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

from .base import BaseSiteHandler

ADOBE_HOST_SUFFIX = "careers.adobe.com"
ADOBE_BASE_URL = "https://careers.adobe.com"
LISTING_PATH_TOKEN = "/search-results"
JOB_PATH_TOKEN = "/job/"

_JOB_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://[^"\']+/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)
_JOB_LINK_REL_RE = re.compile(
    r'href=["\'](?P<href>/[^"\']+/job/[^"\']+)["\']',
    flags=re.IGNORECASE,
)
_JOB_URL_RE = re.compile(
    r'(?P<href>https?://(?:www\.)?careers\.adobe\.com[^"\']+/job/[^"\']+)',
    flags=re.IGNORECASE,
)
_PAGINATION_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://[^"\']+/search-results[^"\']*from=\d+[^"\']*)["\']',
    flags=re.IGNORECASE,
)
_PAGINATION_LINK_REL_RE = re.compile(
    r'href=["\'](?P<href>/[^"\']*/search-results[^"\']*from=\d+[^"\']*)["\']',
    flags=re.IGNORECASE,
)
_JOB_DESCRIPTION_HEADER_RE = re.compile(r"^#+\s*job description\b|^job description\b", flags=re.IGNORECASE)
_DROP_LINE_RE = re.compile(
    r"^(save job|apply now|category|job id|posted date|location|close the popup|profile icon)$",
    flags=re.IGNORECASE,
)
_COOKIE_LINE_RE = re.compile(r"\bcookie\b", flags=re.IGNORECASE)
_STOP_SECTION_RE = re.compile(
    r"^(explore location|get notified for similar jobs|similar jobs|profile recommendations|get tailored job)$",
    flags=re.IGNORECASE,
)
_IMAGE_RE = re.compile(r"^!\[[^\]]*]\([^\)]*\)$")


class AdobeCareersHandler(BaseSiteHandler):
    name = "adobe_careers"
    site_type = "adobe"
    needs_page_links = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        return bool(host) and host.endswith(ADOBE_HOST_SUFFIX)

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
                    "timeout": {"secs": 25, "nanos": 0},
                },
                "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
            }
            return self._apply_page_links_config(base_config)
        base_config["return_format"] = ["commonmark", "raw_html"]
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
                cleaned = urljoin(ADOBE_BASE_URL, cleaned)
            if cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for match in _JOB_LINK_RE.finditer(html):
            _add(match.group("href"))
        for match in _JOB_LINK_REL_RE.finditer(html):
            _add(match.group("href"))
        for match in _JOB_URL_RE.finditer(html):
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

    def extract_location_hint(self, markdown: str) -> str | None:
        if not markdown:
            return None
        lines = markdown.splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.lower() != "location":
                continue
            for offset in range(1, 4):
                if idx + offset >= len(lines):
                    break
                candidate = self._clean_markdown_token(lines[idx + offset])
                if not candidate:
                    continue
                if candidate.lower() in {"category", "job id", "posted date"}:
                    break
                lowered = candidate.lower()
                if lowered.startswith("remote,"):
                    candidate = candidate.split(",", 1)[-1].strip()
                return candidate

        for line in lines[:5]:
            candidate = self._extract_location_from_title_line(line)
            if candidate:
                return candidate
        return None

    def normalize_markdown(self, markdown: str) -> tuple[str, str | None]:
        if not markdown:
            return "", None

        lines = markdown.splitlines()
        title: str | None = None
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            heading_match = re.match(r"^#+\s*(.+)$", stripped)
            if not heading_match:
                continue
            candidate = heading_match.group(1).strip()
            if not candidate:
                continue
            if _JOB_DESCRIPTION_HEADER_RE.match(stripped):
                continue
            title = candidate
            break

        start_idx: int | None = None
        for idx, line in enumerate(lines):
            if _JOB_DESCRIPTION_HEADER_RE.match(line.strip()):
                start_idx = idx
                break
        if start_idx is not None:
            lines = lines[start_idx:]

        stop_idx: int | None = None
        for idx, line in enumerate(lines):
            cleaned_stop = self._clean_markdown_token(line)
            if _STOP_SECTION_RE.match(cleaned_stop):
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
            if stripped == "-":
                continue
            if _IMAGE_RE.match(stripped):
                continue
            if _COOKIE_LINE_RE.search(stripped):
                continue
            lower = stripped.lower()
            if "apply now" in lower or "jobseqno" in lower:
                continue
            cleaned = self._clean_markdown_token(stripped)
            if _DROP_LINE_RE.match(cleaned):
                continue
            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned or markdown, title

    @staticmethod
    def _clean_markdown_token(value: str) -> str:
        cleaned = value.strip()
        cleaned = re.sub(r"^[#*\-\u2022]+", "", cleaned).strip()
        cleaned = cleaned.strip("*` ")
        cleaned = cleaned.strip("[]()")
        cleaned = cleaned.strip(" ,:;")
        return cleaned

    @staticmethod
    def _extract_location_from_title_line(value: str) -> str | None:
        if not value:
            return None
        match = re.search(r"\bin\s+(?P<location>[^|]+)\|", value, flags=re.IGNORECASE)
        if not match:
            return None
        location = match.group("location").strip()
        return location or None
