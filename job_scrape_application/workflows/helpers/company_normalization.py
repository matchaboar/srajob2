"""Company name normalization utilities for job scraping.

This module provides functions for extracting, normalizing, and validating
company names from various sources including URLs, titles, and hints.
"""

from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from .regex_patterns import (
    NON_ALNUM_PATTERN,
    _TITLE_BAR_RE,
    _TITLE_IN_BAR_COMPANY_RE,
    _TITLE_IN_BAR_RE,
)

# Regex to strip common company suffixes (Inc, LLC, Corp, etc.)
_COMPANY_SUFFIX_RE = re.compile(
    r"(,?\s*(inc|inc\.|llc|ltd|limited|corp|corporation|co|company)\.?)$",
    flags=re.IGNORECASE,
)

# Generic hints that don't represent actual company names
_GENERIC_COMPANY_HINTS = {
    "careers",
    "career",
    "jobs",
    "job",
    "careers home",
    "job description",
}

# Language codes and other short tokens that shouldn't be company names
# These often appear as URL path segments (e.g., /en/jobs, /ko/land/)
_INVALID_COMPANY_TOKENS = {
    # Language codes (ISO 639-1)
    "en", "ko", "ja", "zh", "de", "fr", "es", "it", "pt", "ru", "ar", "hi",
    "nl", "pl", "sv", "da", "no", "fi", "cs", "hu", "ro", "tr", "th", "vi",
    "id", "ms", "tl", "uk", "el", "he", "fa", "bn", "ta", "te", "mr", "gu",
    # Common URL segments that aren't company names
    "api", "app", "www", "cdn", "static", "land", "site", "web", "v1", "v2",
    # Generic terms
    "unknown", "untitled", "null", "none", "na", "n/a",
}

# Known job board platform tokens (not company names)
_JOB_BOARD_COMPANY_TOKENS = {
    "ashby",
    "avature",
    "brassring",
    "greenhouse",
    "icims",
    "jibeapply",
    "lever",
    "smartrecruiters",
    "taleo",
    "workday",
}


def _stringify(value: Any) -> str:
    """Convert a value to a string, stripping whitespace."""
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return str(value)


def normalize_company_hint(value: Any) -> Optional[str]:
    """Normalize a company hint value by cleaning up formatting artifacts.

    Handles markdown, HTML entities, company suffixes, and common patterns
    like "Role at Company" to extract just the company name.

    Args:
        value: The raw company hint value

    Returns:
        Cleaned company name, or None if invalid/generic
    """
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    # Remove zero-width characters
    cleaned = cleaned.replace("\u200b", "").replace("\ufeff", "").strip()
    # Remove markdown list markers
    cleaned = re.sub(r"^[#*\-\u2022]+", "", cleaned).strip()
    # Remove markdown emphasis
    cleaned = re.sub(r"^[*_`]+", "", cleaned).strip()
    cleaned = re.sub(r"[*_`]+$", "", cleaned).strip()
    # Remove brackets and quotes
    cleaned = cleaned.strip("[](){}<>\"'")
    # Remove trailing punctuation
    cleaned = cleaned.strip(" ,;:-–—")
    # Remove company suffixes
    cleaned = _COMPANY_SUFFIX_RE.sub("", cleaned).strip(" ,")
    lowered = cleaned.lower()
    # Handle "Role at Company" pattern
    if " at " in lowered:
        cleaned = cleaned.rsplit(" at ", 1)[-1].strip()
        cleaned = _COMPANY_SUFFIX_RE.sub("", cleaned).strip(" ,")
    if not cleaned:
        return None
    # Check if result is a generic hint
    normalized_key = re.sub(NON_ALNUM_PATTERN, " ", cleaned).strip().lower()
    if not normalized_key or normalized_key in _GENERIC_COMPANY_HINTS:
        return None
    return cleaned


def normalize_title_from_bar(title: str) -> str:
    """Extract job title from a title bar pattern like "Title | Company".

    Handles various separator patterns and normalizes whitespace.

    Args:
        title: The raw title string potentially containing company info

    Returns:
        The extracted title portion, or the normalized original if no pattern matches
    """
    if not isinstance(title, str) or not title.strip():
        return title
    # Normalize whitespace first (collapse newlines/tabs/multiple spaces)
    normalized = " ".join(title.split())
    # Decode HTML entities (e.g. &amp; -> &)
    normalized = html_lib.unescape(normalized)
    if not normalized:
        return title
    match = _TITLE_IN_BAR_COMPANY_RE.match(normalized) or _TITLE_IN_BAR_RE.match(normalized) or _TITLE_BAR_RE.match(normalized)
    if match:
        cleaned = _stringify(match.group("title"))
        return cleaned or normalized
    return normalized


def is_generic_company_name(value: str | None) -> bool:
    """Check if a company name is generic (placeholder or job board name).

    Args:
        value: The company name to check

    Returns:
        True if the name is generic/invalid, False if it's a real company name
    """
    if not value:
        return True
    normalized = re.sub(NON_ALNUM_PATTERN, "", value.lower())
    if not normalized:
        return True
    if normalized in {"unknown", "unknowncompany"}:
        return True
    # Check for language codes and other invalid tokens
    if normalized in _INVALID_COMPANY_TOKENS:
        return True
    return normalized in _JOB_BOARD_COMPANY_TOKENS


def apply_company_hint(company: str, hints: Dict[str, Any]) -> str:
    """Apply a company hint to override or supplement the current company name.

    Uses the hint if the current company is generic, or if the hint is a
    better-formatted version of the same name.

    Args:
        company: The current company name
        hints: Dictionary potentially containing a 'company' key

    Returns:
        The best company name to use
    """
    hint = normalize_company_hint(hints.get("company"))
    if not hint:
        return company
    if is_generic_company_name(company):
        return hint
    normalized_company = re.sub(NON_ALNUM_PATTERN, "", company.lower())
    normalized_hint = re.sub(NON_ALNUM_PATTERN, "", hint.lower())
    if normalized_company and normalized_company == normalized_hint and hint != company:
        return hint
    return company


def derive_company_from_url(url: str) -> str:
    """Extract company name from a job posting URL.

    Handles various ATS URL patterns including Greenhouse, Workday, Avature,
    and generic career site URLs.

    Args:
        url: The job posting URL

    Returns:
        Extracted company name in title case, or empty string if not found
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
    except Exception:
        return ""

    hostname = hostname.lower()
    generic_subdomains = {
        "www",
        "jobs",
        "careers",
        "boards",
        "board",
        "apply",
        "app",
        "join",
        "team",
        "teams",
        "work",
    }

    # Avature URLs: company.avature.net
    if hostname.endswith(("avature.net", "avature.com")):
        parts = hostname.split(".")
        if len(parts) >= 3:
            for candidate in parts[:-2]:
                if not candidate or candidate in generic_subdomains:
                    continue
                cleaned = re.sub(NON_ALNUM_PATTERN, " ", candidate).strip()
                if cleaned:
                    return cleaned.title()

    # Workday URLs: company.myworkdayjobs.com
    if hostname.endswith(("myworkdayjobs.com", "myworkdaysite.com")):
        parts = hostname.split(".")
        if len(parts) >= 3:
            subdomains = parts[:-2]
            for candidate in subdomains:
                if not candidate:
                    continue
                if candidate in generic_subdomains:
                    continue
                if re.fullmatch(r"wd\d+", candidate):
                    continue
                cleaned = re.sub(NON_ALNUM_PATTERN, " ", candidate).strip()
                if cleaned:
                    return cleaned.title()
            for candidate in reversed(subdomains):
                if not candidate or re.fullmatch(r"wd\d+", candidate):
                    continue
                cleaned = re.sub(NON_ALNUM_PATTERN, " ", candidate).strip()
                if cleaned:
                    return cleaned.title()

    # Greenhouse boards encode the company slug in the path: /{company}/jobs/...
    if hostname.endswith("greenhouse.io"):
        parts = [p for p in parsed.path.split("/") if p]
        if parts:
            slug = parts[0]
            # Skip language codes and generic path segments
            if slug.lower() in _INVALID_COMPANY_TOKENS:
                return ""
            cleaned_slug = re.sub(NON_ALNUM_PATTERN, " ", slug).strip()
            if cleaned_slug and cleaned_slug.lower() not in _INVALID_COMPANY_TOKENS:
                return cleaned_slug.title()

    # Strip common career subdomain prefixes
    for prefix in ("careers.", "jobs.", "boards.", "boards-", "job-", "boards-"):
        if hostname.startswith(prefix):
            hostname = hostname[len(prefix):]
            break

    # Extract company from domain name
    parts = hostname.split(".")
    if len(parts) >= 2:
        name = parts[-2]
    elif parts:
        name = parts[0]
    else:
        return ""

    cleaned = re.sub(NON_ALNUM_PATTERN, " ", name).strip()
    return cleaned.title() if cleaned else ""


__all__ = [
    # Constants
    "_COMPANY_SUFFIX_RE",
    "_GENERIC_COMPANY_HINTS",
    "_INVALID_COMPANY_TOKENS",
    "_JOB_BOARD_COMPANY_TOKENS",
    # Functions
    "normalize_company_hint",
    "normalize_title_from_bar",
    "is_generic_company_name",
    "apply_company_hint",
    "derive_company_from_url",
]
