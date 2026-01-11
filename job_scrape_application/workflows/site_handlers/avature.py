from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from .base import BaseSiteHandler
from ..helpers.link_extractors import normalize_url
from ..helpers.regex_patterns import (
    AVATURE_BASE_URL_RE,
    AVATURE_JOB_DETAIL_PATH_RE,
    AVATURE_JOB_DETAIL_URL_RE,
    AVATURE_JOB_RECORDS_PER_PAGE_RE,
    AVATURE_PAGE_RANGE_RE,
    AVATURE_PAGINATION_PATH_RE,
    AVATURE_PAGINATION_URL_RE,
    AVATURE_RESULTS_ARIA_RE,
    BASE_URL_META_PATTERNS,
)

AVATURE_HOST_SUFFIXES = ("avature.net", "avature.com")


class AvatureHandler(BaseSiteHandler):
    name = "avature"
    site_type = "avature"
    needs_page_links = True

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return any(host.endswith(suffix) for suffix in AVATURE_HOST_SUFFIXES)

    def is_listing_url(self, url: str) -> bool:
        try:
            path = (urlparse(url).path or "").lower()
        except Exception:
            return False
        return "/careers/searchjobs" in path or "/careers/searchjobsdata" in path

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        if self.is_listing_url(uri):
            return self._apply_page_links_config(
                {
                    "request": "chrome",
                    "return_format": ["raw_html"],
                    "follow_redirects": True,
                    "redirect_policy": "Loose",
                    "external_domains": ["*"],
                    "preserve_host": True,
                    "wait_for": {
                        "selector": {
                            "selector": "a[href*='/careers/JobDetail/']",
                            "timeout": {"secs": 15, "nanos": 0},
                        },
                        "idle_network0": {"timeout": {"secs": 5, "nanos": 0}},
                    },
                }
            )
        return {
            "request": "chrome",
            "return_format": ["commonmark"],
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
        }

    def get_links_from_raw_html(self, html: str) -> List[str]:
        if not html:
            return []

        base_url = self._extract_base_url(html)
        if base_url:
            base_url = html_lib.unescape(base_url)
        urls: List[str] = []
        seen: set[str] = set()

        def _add(url_val: str | None) -> None:
            if not url_val:
                return
            cleaned = html_lib.unescape(url_val).strip()
            if not cleaned:
                return
            if cleaned in seen:
                return
            seen.add(cleaned)
            urls.append(cleaned)

        for match in AVATURE_JOB_DETAIL_URL_RE.findall(html):
            _add(match)
        for match in AVATURE_PAGINATION_URL_RE.findall(html):
            _add(match)

        if base_url:
            for match in AVATURE_JOB_DETAIL_PATH_RE.findall(html):
                _add(urljoin(base_url, match))
            for match in AVATURE_PAGINATION_PATH_RE.findall(html):
                _add(urljoin(base_url, match))
        else:
            for match in AVATURE_JOB_DETAIL_PATH_RE.findall(html):
                _add(match)
            for match in AVATURE_PAGINATION_PATH_RE.findall(html):
                _add(match)

        pagination_base = base_url if base_url and "/careers/searchjobs" in base_url.lower() else None
        if not pagination_base:
            pagination_base = self._infer_pagination_base_url(html, urls)
        if pagination_base and "/careers/searchjobs" in pagination_base.lower():
            urls.extend(self._augment_pagination_urls(pagination_base, html, urls))

        return self.filter_job_urls(urls)

    def _augment_pagination_urls(self, base_url: str, html: str, urls: List[str]) -> List[str]:
        parsed_base = urlparse(base_url)
        base_offset = None
        base_has_offset = False
        base_page_size = None
        base_pairs = parse_qsl(parsed_base.query, keep_blank_values=True)
        for key, value in base_pairs:
            lower_key = key.lower()
            if lower_key == "joboffset":
                base_has_offset = True
                try:
                    base_offset = int(value)
                except Exception:
                    base_offset = None
            if lower_key == "jobrecordsperpage":
                try:
                    base_page_size = int(value)
                except Exception:
                    base_page_size = None

        def _with_job_offset(url_value: str, offset: int, page_size: int | None) -> str:
            parsed = urlparse(url_value)
            params: list[tuple[str, str]] = []
            existing_page_size: str | None = None
            for key, value in parse_qsl(parsed.query, keep_blank_values=True):
                lower_key = key.lower()
                if lower_key == "joboffset":
                    continue
                if lower_key == "jobrecordsperpage":
                    existing_page_size = value
                    continue
                params.append((key, value))
            if page_size is not None:
                params.append(("jobRecordsPerPage", str(page_size)))
            elif existing_page_size is not None:
                params.append(("jobRecordsPerPage", existing_page_size))
            params.append(("jobOffset", str(offset)))
            return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))

        def _infer_page_data() -> tuple[int | None, int | None, int | None, bool]:
            match = AVATURE_PAGE_RANGE_RE.search(html)
            if match:
                start = int(match.group("start"))
                end = int(match.group("end"))
                total = int(match.group("total"))
                page_size = max(end - start + 1, 1)
                current_offset = max(start - 1, 0)
                return current_offset, page_size, total, True

            page_size = None
            total = None
            match = AVATURE_JOB_RECORDS_PER_PAGE_RE.search(html)
            if match:
                page_size = int(match.group("count"))
            match = AVATURE_RESULTS_ARIA_RE.search(html)
            if match:
                total = int(match.group("count"))
            default_offset = base_offset if base_offset is not None else 0
            return default_offset, page_size, total, total is not None

        augmented: List[str] = []
        current_offset, page_size, total, has_total = _infer_page_data()
        if page_size is None and base_page_size is not None:
            page_size = base_page_size
        normalized_page_size = page_size if page_size is not None else base_page_size
        if base_offset is not None:
            current_offset = base_offset
        should_add_zero = (not base_has_offset) or (current_offset == 0)
        if should_add_zero and not any("joboffset=0" in url.lower() for url in urls):
            augmented.append(_with_job_offset(base_url, 0, normalized_page_size))

        if page_size and current_offset is not None and has_total:
            next_offset = current_offset + page_size
            if total is None or next_offset < total:
                next_token = f"joboffset={next_offset}"
                if not any(next_token in url.lower() for url in urls + augmented):
                    augmented.append(_with_job_offset(base_url, next_offset, normalized_page_size))

        return augmented

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
            if "/savejob" in lower or "/login" in lower or "/register" in lower:
                continue
            if "/careers/" not in lower:
                continue
            if not any(
                token in lower
                for token in (
                    "/careers/jobdetail/",
                    "/careers/searchjobs",
                    "/careers/searchjobsdata",
                )
            ):
                continue
            seen.add(cleaned)
            filtered.append(cleaned)
        return filtered

    def filter_job_urls_for_site(self, urls: List[str], source_url: str | None) -> List[str]:
        filtered = self.filter_job_urls(urls)
        if not source_url:
            return filtered
        normalized_source = normalize_url(source_url, base_url=source_url) or source_url
        if not normalized_source or not self.matches_url(normalized_source):
            return filtered
        if not self.is_listing_url(normalized_source):
            return filtered
        try:
            source_parsed = urlparse(normalized_source)
        except Exception:
            return filtered

        source_host = (source_parsed.hostname or "").lower()
        source_path = (source_parsed.path or "").rstrip("/")
        if not source_host or not source_path:
            return filtered

        source_pairs = parse_qsl(source_parsed.query, keep_blank_values=True)
        base_pairs = [(key, value) for key, value in source_pairs if key.lower() != "joboffset"]
        base_key_values = {key.lower(): value for key, value in base_pairs}
        required_keys = set(base_key_values.keys())
        allowed_keys = set(required_keys)
        allowed_keys.add("joboffset")

        def _matches_listing_path(path: str) -> bool:
            lower = (path or "").lower().rstrip("/")
            base_lower = source_path.lower().rstrip("/")
            if "/careers/searchjobsdata" in lower:
                base_lower = base_lower.replace("/careers/searchjobs", "/careers/searchjobsdata", 1)
                return lower == base_lower
            if "/careers/searchjobs" in lower:
                return lower == base_lower
            return False

        def _canonical_listing_url(path: str, job_offset: int | None) -> str:
            query_pairs = list(base_pairs)
            if job_offset is not None:
                query_pairs.append(("jobOffset", str(job_offset)))
            return urlunparse(
                (
                    source_parsed.scheme or "https",
                    source_parsed.netloc,
                    path.rstrip("/"),
                    "",
                    urlencode(query_pairs, doseq=True),
                    "",
                )
            )

        cleaned: List[str] = []
        seen: set[str] = set()
        for url in filtered:
            normalized = normalize_url(url, base_url=normalized_source) or url
            try:
                parsed = urlparse(normalized)
            except Exception:
                continue
            host = (parsed.hostname or "").lower()
            if not host or host != source_host:
                continue
            path_lower = (parsed.path or "").lower()
            if "/careers/jobdetail/" in path_lower:
                if normalized not in seen:
                    seen.add(normalized)
                    cleaned.append(normalized)
                continue
            if not _matches_listing_path(parsed.path or ""):
                continue
            pairs = parse_qsl(parsed.query, keep_blank_values=True)
            keys_lower = [key.lower() for key, _ in pairs]
            if any(not key or ";" in key for key in keys_lower):
                continue
            key_set = set(keys_lower)
            if required_keys and not required_keys.issubset(key_set):
                continue
            if not key_set.issubset(allowed_keys):
                continue
            invalid = False
            for base_key, base_value in base_key_values.items():
                values = [val for key, val in pairs if key.lower() == base_key]
                if len(values) != 1 or values[0] != base_value:
                    invalid = True
                    break
            if invalid:
                continue
            job_offset = None
            for key, value in pairs:
                if key.lower() == "joboffset":
                    if not value.isdigit():
                        invalid = True
                        break
                    job_offset = int(value)
                    if job_offset < 0:
                        invalid = True
                        break
            if invalid:
                continue
            canonical = _canonical_listing_url(parsed.path or source_path, job_offset)
            if canonical in seen:
                continue
            seen.add(canonical)
            cleaned.append(canonical)

        pagination_candidates = [url for url in filtered if "joboffset=" in url.lower()]
        if pagination_candidates and not any("joboffset=" in url.lower() for url in cleaned):
            for url in pagination_candidates:
                normalized = normalize_url(url, base_url=normalized_source) or url
                try:
                    parsed = urlparse(normalized)
                except Exception:
                    continue
                job_offset = None
                for key, value in parse_qsl(parsed.query, keep_blank_values=True):
                    if key.lower() == "joboffset" and value.isdigit():
                        job_offset = int(value)
                        break
                canonical = _canonical_listing_url(parsed.path or source_path, job_offset)
                if canonical in seen:
                    continue
                seen.add(canonical)
                cleaned.append(canonical)

        if not cleaned:
            return filtered

        return cleaned


    def _infer_pagination_base_url(self, html: str, urls: List[str]) -> Optional[str]:
        if not self._has_pagination_signals(html):
            return None
        for url in urls:
            if self.is_listing_url(url):
                return url
        for url in urls:
            parsed = urlparse(url)
            if parsed.scheme and parsed.netloc:
                return urlunparse(
                    parsed._replace(path="/careers/SearchJobs", params="", query="", fragment="")
                )
        return "/careers/SearchJobs"

    @staticmethod
    def _has_pagination_signals(html: str) -> bool:
        return bool(
            AVATURE_PAGE_RANGE_RE.search(html)
            or AVATURE_RESULTS_ARIA_RE.search(html)
            or AVATURE_JOB_RECORDS_PER_PAGE_RE.search(html)
        )

    @staticmethod
    def _extract_base_url(html: str) -> Optional[str]:
        for pattern in BASE_URL_META_PATTERNS:
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                return match.group("url")
        match = AVATURE_BASE_URL_RE.search(html)
        if match:
            return match.group(0)
        return None
