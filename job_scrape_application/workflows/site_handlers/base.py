from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import html as html_lib
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ..helpers.link_extractors import fix_scheme_slashes, normalize_url, strip_wrapping_url
from ..helpers.regex_patterns import (
    JSON_ARRAY_PATTERN,
    JSON_LD_SCRIPT_PATTERN,
    JSON_OBJECT_PATTERN,
    PRE_PATTERN,
)

_POSTED_DATE_LABEL_RE = re.compile(
    r"^(?:#+\s*)?(?:posted\s+date|date\s+posted|posting\s+date|date\s+of\s+posting|posted\s+on|updated\s+on|updated\s+at|last\s+updated)\b",
    flags=re.IGNORECASE,
)
_POSTED_DATE_INLINE_RE = re.compile(
    r"(?:posted\s+date|date\s+posted|posting\s+date|date\s+of\s+posting|posted\s+on|updated\s+on|updated\s+at|last\s+updated)\b",
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
_DAY_MONTH_DATE_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"[.,]?\s+(?P<year>\d{4})\b",
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
_POSTED_AT_KEYS = (
    "postedTs",
    "posted_ts",
    "postedAt",
    "posted_at",
    "datePosted",
    "date_posted",
    "postedDate",
    "postingDate",
    "postDate",
    "publishedAt",
    "published_at",
    "publishDate",
    "publicationDate",
    "publication_date",
)
_POSTED_AT_FALLBACK_KEYS = (
    "createdAt",
    "created_at",
    "creationTs",
    "createdTs",
    "created_ts",
    "dateCreated",
    "date_created",
    "dateModified",
    "date_modified",
    "lastUpdated",
    "last_updated",
    "validThrough",
    "valid_through",
    "validUntil",
    "valid_until",
)
_POSTED_AT_CONTAINER_KEYS = (
    "data",
    "job",
    "jobPosting",
    "job_posting",
    "jobPostingInfo",
    "job_detail",
    "jobDetail",
    "jobDetails",
    "position",
    "posting",
)
_POSTED_AT_PLACEHOLDER_RE = re.compile(
    r"^(?:0+|0000-00-00(?:[T\s]00:00:00(?:Z|[+-]00:00)?)?|"
    r"1970-01-01(?:[T\s]00:00:00(?:Z|[+-]00:00)?)?)$"
)

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
        return self._extract_posted_at_from_payload(payload, url)

    def extract_company(self, payload: Any, url: str | None = None) -> Optional[str]:
        return None

    def extract_posted_at_from_markdown(self, markdown: str, url: str | None = None) -> Any | None:
        if not markdown:
            return None
        def _normalize_markdown_candidates(value: str) -> List[str]:
            candidates = [value]
            if "<" in value and ">" in value:
                cleaned = re.sub(r"<script\b[^>]*>.*?</script>", " ", value, flags=re.IGNORECASE | re.DOTALL)
                cleaned = re.sub(r"<style\b[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
                cleaned = re.sub(r"<[^>]+>", "\n", cleaned)
                cleaned = html_lib.unescape(cleaned)
                cleaned = re.sub(r"[ \t\r\f\v]+", " ", cleaned)
                cleaned = re.sub(r"\n{2,}", "\n", cleaned)
                cleaned = cleaned.strip()
                if cleaned and cleaned != value:
                    candidates.append(cleaned)
            return candidates

        for candidate_text in _normalize_markdown_candidates(markdown):
            relative_match = _RELATIVE_POSTED_RE.search(candidate_text)
            if relative_match:
                return relative_match.group(0)

            inline_match = _POSTED_DATE_INLINE_RE.search(candidate_text)
            if inline_match:
                tail = candidate_text[inline_match.end() : inline_match.end() + 200]
                parsed = self._extract_iso_date_from_text(tail)
                if parsed:
                    return parsed

            lines = candidate_text.splitlines()
            for idx, line in enumerate(lines):
                stripped = line.strip()
                cleaned = stripped.strip("*`-• ").strip()
                if not cleaned:
                    continue
                if _POSTED_DATE_LABEL_RE.match(cleaned):
                    for offset in range(0, 4):
                        if idx + offset >= len(lines):
                            break
                        line_candidate = lines[idx + offset].strip()
                        if not line_candidate:
                            continue
                        parsed = self._extract_iso_date_from_text(line_candidate)
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
            candidate = f"{iso_match.group('date')}T00:00:00+00:00"
            if _POSTED_AT_PLACEHOLDER_RE.match(candidate):
                return None
            return candidate

        slash_match = _SLASH_DATE_RE.search(text)
        if slash_match:
            try:
                month = int(slash_match.group("month"))
                day = int(slash_match.group("day"))
                year = int(slash_match.group("year"))
                if year < 100:
                    year += 2000
                dt = datetime(year, month, day, tzinfo=timezone.utc)
                candidate = dt.isoformat()
                if _POSTED_AT_PLACEHOLDER_RE.match(candidate):
                    return None
                return candidate
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
                candidate = dt.isoformat()
                if _POSTED_AT_PLACEHOLDER_RE.match(candidate):
                    return None
                return candidate
            except Exception:
                return None

        day_month_match = _DAY_MONTH_DATE_RE.search(text)
        if day_month_match:
            month_name = day_month_match.group("month").lower().strip(".")
            month = _MONTH_NAME_TO_NUMBER.get(month_name)
            if not month:
                return None
            try:
                day = int(day_month_match.group("day"))
                year = int(day_month_match.group("year"))
                dt = datetime(year, month, day, tzinfo=timezone.utc)
                candidate = dt.isoformat()
                if _POSTED_AT_PLACEHOLDER_RE.match(candidate):
                    return None
                return candidate
            except Exception:
                return None

        return None

    @classmethod
    def _extract_posted_at_from_payload(
        cls,
        payload: Any,
        url: str | None = None,
        *,
        depth: int = 0,
        seen: set[int] | None = None,
    ) -> Any | None:
        if depth > 6:
            return None
        if isinstance(payload, str):
            return cls._extract_posted_at_from_text(
                payload,
                url,
                depth=depth + 1,
                seen=seen,
            )
        if not isinstance(payload, (dict, list)):
            return None
        if seen is None:
            seen = set()
        payload_id = id(payload)
        if payload_id in seen:
            return None
        seen.add(payload_id)

        def _coerce(value: Any) -> Any | None:
            if value is None:
                return None
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned or _POSTED_AT_PLACEHOLDER_RE.match(cleaned):
                    return None
                return cleaned
            if isinstance(value, (int, float)):
                return value if value > 0 else None
            return None

        def _job_id_tokens() -> list[str]:
            if not url:
                return []
            try:
                parsed = urlparse(url)
            except Exception:
                parsed = None
            path = parsed.path if parsed else url
            parts = [part for part in path.split("/") if part]
            if not parts:
                return []
            last = parts[-1].split("?")[0].strip()
            return [last] if last else []

        if isinstance(payload, dict):
            for key in _POSTED_AT_KEYS:
                candidate = _coerce(payload.get(key))
                if candidate is not None:
                    return candidate

            for key in _POSTED_AT_CONTAINER_KEYS:
                if key in payload:
                    candidate = cls._extract_posted_at_from_payload(
                        payload.get(key),
                        url,
                        depth=depth + 1,
                        seen=seen,
                    )
                    if candidate is not None:
                        return candidate

            job_tokens = _job_id_tokens()
            if job_tokens:
                for key, value in payload.items():
                    if not isinstance(value, list) or not value:
                        continue
                    for item in value:
                        if not isinstance(item, dict):
                            continue
                        for id_key in ("id", "jobId", "job_id", "positionId", "postingId", "reqId"):
                            if id_key not in item:
                                continue
                            item_id = str(item.get(id_key))
                            if item_id in job_tokens:
                                candidate = cls._extract_posted_at_from_payload(
                                    item,
                                    url,
                                    depth=depth + 1,
                                    seen=seen,
                                )
                                if candidate is not None:
                                    return candidate

            for value in payload.values():
                if isinstance(value, (dict, list)):
                    candidate = cls._extract_posted_at_from_payload(
                        value,
                        url,
                        depth=depth + 1,
                        seen=seen,
                    )
                    if candidate is not None:
                        return candidate

            for key in _POSTED_AT_FALLBACK_KEYS:
                candidate = _coerce(payload.get(key))
                if candidate is not None:
                    return candidate

        if isinstance(payload, list):
            if len(payload) == 1:
                return cls._extract_posted_at_from_payload(
                    payload[0],
                    url,
                    depth=depth + 1,
                    seen=seen,
                )
            job_tokens = _job_id_tokens()
            if job_tokens:
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    for id_key in ("id", "jobId", "job_id", "positionId", "postingId", "reqId"):
                        if id_key not in item:
                            continue
                        item_id = str(item.get(id_key))
                        if item_id in job_tokens:
                            candidate = cls._extract_posted_at_from_payload(
                                item,
                                url,
                                depth=depth + 1,
                                seen=seen,
                            )
                            if candidate is not None:
                                return candidate

            for item in payload:
                candidate = cls._extract_posted_at_from_payload(
                    item,
                    url,
                    depth=depth + 1,
                    seen=seen,
                )
                if candidate is not None:
                    return candidate

        return None

    @classmethod
    def _extract_posted_at_from_text(
        cls,
        text: str,
        url: str | None = None,
        *,
        depth: int,
        seen: set[int] | None,
    ) -> Any | None:
        cleaned = text.strip()
        if not cleaned:
            return None

        def _parse_json_blob(value: str) -> Any | None:
            try:
                parsed = json.loads(value)
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
                unescaped = value.encode("utf-8", errors="ignore").decode("unicode_escape")
            except Exception:
                unescaped = ""
            if unescaped:
                try:
                    return json.loads(unescaped)
                except Exception:
                    pass
            for pattern in (JSON_OBJECT_PATTERN, JSON_ARRAY_PATTERN):
                match = re.search(pattern, value, flags=re.DOTALL)
                if not match:
                    continue
                try:
                    return json.loads(match.group(0))
                except Exception:
                    continue
            return None

        if cleaned.startswith(("{", "[")):
            parsed = _parse_json_blob(cleaned)
            if parsed is not None:
                candidate = cls._extract_posted_at_from_payload(
                    parsed,
                    url,
                    depth=depth + 1,
                    seen=seen,
                )
                if candidate is not None:
                    return candidate

        lower = cleaned.lower()
        if "<script" not in lower or "ld+json" not in lower:
            return None

        script_pattern = re.compile(JSON_LD_SCRIPT_PATTERN, flags=re.IGNORECASE | re.DOTALL)
        for match in script_pattern.finditer(cleaned):
            payload_raw = match.group("payload").strip()
            if not payload_raw:
                continue
            parsed = _parse_json_blob(payload_raw)
            if parsed is None:
                unescaped = html_lib.unescape(payload_raw)
                if unescaped and unescaped != payload_raw:
                    parsed = _parse_json_blob(unescaped)
            if parsed is None:
                continue
            candidate = cls._extract_posted_at_from_payload(
                parsed,
                url,
                depth=depth + 1,
                seen=seen,
            )
            if candidate is not None:
                return candidate

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
        if host.endswith((".convex.site", ".convex.cloud")) and path.startswith("/share/"):
            return True
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
