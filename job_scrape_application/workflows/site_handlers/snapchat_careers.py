from __future__ import annotations

import re
from typing import List, Optional
from urllib.parse import parse_qs, urlparse

from .base import BaseSiteHandler

_HOST_SUFFIX = "careers.snap.com"
_JOB_DETAIL_PATH_RE = re.compile(r"^/job/?$")
_STOP_SECTION_MARKERS = {"ready to join team snap", "life at snap", "life at"}
_IMAGE_LINE_RE = re.compile(r"^!\[[^\]]*\]\([^)]+\)$")
_EMPTY_LINK_RE = re.compile(r"^\[\s*\]\([^)]+\)$")
_JOB_ID_RE = re.compile(r"^R\d{4,}$", flags=re.IGNORECASE)


def _normalize_line(value: str) -> str:
    cleaned = value.strip()
    cleaned = re.sub(r"^[#*\-\u2022]+", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.lower()


class SnapchatCareersHandler(BaseSiteHandler):
    name = "snapchat"
    site_type = "snapchat"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.endswith(_HOST_SUFFIX)

    def is_listing_url(self, url: str) -> bool:
        """Check if URL is a listing page (not a job detail page)."""
        if not self.matches_url(url):
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        path = (parsed.path or "").rstrip("/")
        # Job detail pages have /job?id=... format
        if _JOB_DETAIL_PATH_RE.match(path + "/"):
            query = parse_qs(parsed.query)
            if "id" in query:
                return False  # This is a job detail page
        # Everything else is a listing page
        return True

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        """Filter URLs to only include valid job detail pages.

        Valid job URLs: https://careers.snap.com/job?id=R0043117
        Invalid URLs:
        - https://careers.snap.com/university
        - https://careers.snap.com/belonging
        - https://www.snap.com/...
        """
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
            # Must be on careers.snap.com
            if host != _HOST_SUFFIX:
                continue
            path = (parsed.path or "").rstrip("/")
            # Must be /job path
            if not _JOB_DETAIL_PATH_RE.match(path + "/"):
                continue
            # Must have ?id=... query param
            query = parse_qs(parsed.query)
            if "id" not in query or not query["id"]:
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

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
            candidate = heading_match.group(1).strip()
            if candidate:
                title = candidate
                break

        start_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            if re.match(r"^#+\s*(.+)$", line.strip()):
                start_idx = idx
                break
        if start_idx is not None:
            lines = lines[start_idx:]

        stop_idx: Optional[int] = None
        for idx, line in enumerate(lines):
            if _normalize_line(line) in _STOP_SECTION_MARKERS:
                stop_idx = idx
                break
        if stop_idx is not None:
            lines = lines[:stop_idx]

        cleaned_lines: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                cleaned_lines.append(line)
                continue
            lower = stripped.lower()
            if "view openings" in lower or "apply now" in lower:
                continue
            if _IMAGE_LINE_RE.match(stripped):
                continue
            if _EMPTY_LINK_RE.match(stripped):
                continue
            if _JOB_ID_RE.match(stripped):
                continue
            cleaned_lines.append(line)

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned or markdown, title
