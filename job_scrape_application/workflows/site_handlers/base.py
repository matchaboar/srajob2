from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import html as html_lib
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ..helpers.link_extractors import fix_scheme_slashes, normalize_url, strip_wrapping_url
from ..helpers.regex_patterns import JSON_ARRAY_PATTERN, JSON_OBJECT_PATTERN, PRE_PATTERN

_POSTED_DATE_LABEL_RE = re.compile(
    r"^(?:#+\s*)?(?:posted\s+date|date\s+posted|posted\s+on|updated\s+on|updated\s+at|last\s+updated)\b",
    flags=re.IGNORECASE,
)
_RELATIVE_POSTED_LINE_RE = re.compile(r"\bposted\b.{0,40}\bago\b", flags=re.IGNORECASE)
_ISO_DATE_RE = re.compile(r"\b(?P<date>\d{4}-\d{2}-\d{2})\b")
_SLASH_DATE_RE = re.compile(
    r"\b(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<year>\d{2,4})\b"
)
_MONTH_DATE_RE = re.compile(
    r"\b(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"[.,]?\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?(?:,)?\s+(?P<year>\d{4})\b",
    flags=re.IGNORECASE,
)
_RELATIVE_POSTED_RE = re.compile(
    r"\bposted\b[^0-9]{0,20}(?P<value>\d+)\s+(?P<unit>minute|hour|day|week|month|year)s?\s+ago\b",
    flags=re.IGNORECASE,
)
_MONTH_NAME_TO_NUMBER = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

class BaseSiteHandler(ABC):
    """Base class for site-specific scraping helpers."""

    name: str = "base"
    site_type: str | None = None
    supports_listing_api: bool = False
    needs_page_links: bool = False

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
        payload = self._extract_json_payload_from_html(html)
        if not payload:
            return []
        return self.get_links_from_json(payload)

    def get_links_from_json(self, payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        urls: List[str] = []
        seen: set[str] = set()

        def _add(value: Any) -> None:
            if not isinstance(value, str):
                return
            cleaned = value.strip()
            if not cleaned or cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        jobs = payload.get("jobs")
        if isinstance(jobs, list):
            for job in jobs:
                if not isinstance(job, dict):
                    continue
                for key in ("jobUrl", "applyUrl", "jobPostingUrl", "postingUrl", "url", "absolute_url"):
                    _add(job.get(key))

        positions = payload.get("positions")
        if isinstance(positions, list):
            for position in positions:
                if not isinstance(position, dict):
                    continue
                for key in ("canonicalPositionUrl", "url", "jobUrl"):
                    _add(position.get(key))

        return urls

    def extract_posted_at(self, payload: Any, url: str | None = None) -> Any | None:
        return None

    def extract_company(self, payload: Any, url: str | None = None) -> Optional[str]:
        return None

    def extract_posted_at_from_markdown(self, markdown: str, url: str | None = None) -> Any | None:
        if not markdown:
            return None

        relative_match = _RELATIVE_POSTED_RE.search(markdown)
        if relative_match:
            return relative_match.group(0)

        lines = markdown.splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            cleaned = stripped.strip("*` ").strip()
            if not cleaned:
                continue
            if _POSTED_DATE_LABEL_RE.match(cleaned):
                for offset in range(0, 4):
                    if idx + offset >= len(lines):
                        break
                    candidate = lines[idx + offset].strip()
                    if not candidate:
                        continue
                    parsed = self._extract_iso_date_from_text(candidate)
                    if parsed:
                        return parsed
                continue

            lowered = cleaned.lower()
            if "posted" in lowered or "updated" in lowered:
                parsed = self._extract_iso_date_from_text(cleaned)
                if parsed:
                    return parsed
                if "ago" in lowered or "today" in lowered or "yesterday" in lowered:
                    return cleaned
        return None

    @staticmethod
    def _extract_iso_date_from_text(text: str) -> Optional[str]:
        if not text:
            return None
        iso_match = _ISO_DATE_RE.search(text)
        if iso_match:
            return f"{iso_match.group('date')}T00:00:00+00:00"

        slash_match = _SLASH_DATE_RE.search(text)
        if slash_match:
            try:
                month = int(slash_match.group("month"))
                day = int(slash_match.group("day"))
                year = int(slash_match.group("year"))
                if year < 100:
                    year += 2000
                dt = datetime(year, month, day, tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                return None

        month_match = _MONTH_DATE_RE.search(text)
        if month_match:
            month_name = month_match.group("month").lower().strip(".")
            month = _MONTH_NAME_TO_NUMBER.get(month_name)
            if not month:
                return None
            try:
                day = int(month_match.group("day"))
                year = int(month_match.group("year"))
                dt = datetime(year, month, day, tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                return None

        return None

    def get_pagination_urls_from_json(self, payload: Any, source_url: str | None = None) -> List[str]:
        return []

    def get_pagination_urls_from_listing(self, source_url: str | None = None) -> List[str]:
        return []

    def is_listing_url(self, url: str) -> bool:
        return False

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        return {}

    def normalize_spidercloud_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        if not config:
            return {}
        normalized = dict(config)
        if "execution_scripts" in normalized and "exuecution_scripts" not in normalized:
            normalized["exuecution_scripts"] = normalized["execution_scripts"]
        elif "exuecution_scripts" in normalized and "execution_scripts" not in normalized:
            normalized["execution_scripts"] = normalized["exuecution_scripts"]
        return normalized

    @staticmethod
    def drop_source_listing_url(urls: List[str], source_url: str | None) -> List[str]:
        if not source_url or not urls:
            return urls
        normalized_source = normalize_url(source_url, base_url=source_url)
        if not normalized_source:
            return urls
        cleaned: List[str] = []
        for url in urls:
            if not isinstance(url, str):
                continue
            if url == normalized_source:
                continue
            normalized = normalize_url(url, base_url=source_url)
            if normalized == normalized_source:
                continue
            cleaned.append(url)
        return cleaned

    def get_firecrawl_config(self, uri: str) -> Dict[str, Any]:
        return {}

    def normalize_markdown(self, markdown: str) -> tuple[str, Optional[str]]:
        return markdown, None

    def extract_location_hint(self, markdown: str) -> Optional[str]:
        return None

    def should_use_structured_description(self, markdown: str) -> Optional[bool]:
        return None

    def build_structured_description(self, payload: Dict[str, Any]) -> Optional[str]:
        return None

    def is_api_detail_url(self, uri: str) -> bool:
        return False

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        filtered: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = strip_wrapping_url(url)
            if not cleaned or cleaned in seen:
                continue
            cleaned = fix_scheme_slashes(cleaned)
            lower = cleaned.lower()
            if lower.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            if self._looks_like_non_job_detail_url(cleaned):
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

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
        if host.endswith("meta.com") and not host.endswith("metacareers.com"):
            return True
        if host.endswith(("facebook.com", "instagram.com", "twitter.com", "x.com")):
            return True
        if host.endswith("metacareers.com"):
            segments = [seg for seg in path.split("/") if seg]
            if not segments:
                return True
            if segments[0] == "jobsearch":
                return False
            if segments[0] == "jobs":
                return not (len(segments) >= 2 and segments[1].isdigit())
            if (
                len(segments) >= 3
                and segments[0] == "profile"
                and segments[1] == "job_details"
                and segments[2].isdigit()
            ):
                return False
            return True
        if any(token in path for token in ("http://", "https://", "http:/", "https:/")):
            return True
        segments = [seg for seg in path.split("/") if seg]
        if any(seg in {"apply", "application", "hvhapply"} for seg in segments):
            return True
        if host.endswith("linkedin.com"):
            if path.startswith("/company/"):
                return True
            if (
                path.startswith(("/checkpoint/", "/login", "/m/login", "/uas/", "/sharearticle", "/share/"))
                or "request-password-reset" in path
            ):
                return True
        if host.endswith("careers.adobe.com"):
            if "/job/" in path:
                return False
            if "c" in segments or "teams" in segments:
                return True
        if host.endswith("avature.net") and "savejob" in path:
            return True
        if host.endswith("adobe.com") and not host.endswith("careers.adobe.com"):
            if "/job/" in path:
                return False
            if path.startswith("/creativecloud/buy/"):
                return True
        return False

    def _apply_page_links_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        if not self.needs_page_links:
            return config
        if config.get("return_page_links"):
            return config
        merged = dict(config)
        merged["return_page_links"] = True
        return merged

    @staticmethod
    def _extract_json_payload_from_html(html: str) -> Optional[Dict[str, Any]]:
        if not isinstance(html, str) or not html:
            return None
        match = PRE_PATTERN.search(html)
        if not match:
            return None
        content = html_lib.unescape(match.group("content")).strip()
        if not content:
            return None

        def _parse_json_blob(text: str) -> Any | None:
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
            if parsed is not None:
                if isinstance(parsed, str):
                    try:
                        return json.loads(parsed)
                    except Exception:
                        return parsed
                return parsed
            try:
                unescaped = text.encode("utf-8", errors="ignore").decode("unicode_escape")
            except Exception:
                unescaped = ""
            if unescaped:
                try:
                    return json.loads(unescaped)
                except Exception:
                    pass
            for pattern in (JSON_OBJECT_PATTERN, JSON_ARRAY_PATTERN):
                match = re.search(pattern, text, flags=re.DOTALL)
                if not match:
                    continue
                try:
                    return json.loads(match.group(0))
                except Exception:
                    continue
            return None

        def _find_jobs_payload(node: Any) -> Optional[Dict[str, Any]]:
            if isinstance(node, dict):
                jobs = node.get("jobs")
                if isinstance(jobs, list):
                    return node
                positions = node.get("positions")
                if isinstance(positions, list):
                    return node
                for child in node.values():
                    found = _find_jobs_payload(child)
                    if found:
                        return found
            elif isinstance(node, list):
                for child in node:
                    found = _find_jobs_payload(child)
                    if found:
                        return found
            return None

        parsed = _parse_json_blob(content)
        return _find_jobs_payload(parsed)

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
