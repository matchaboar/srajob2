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
    _TITLE_BAR_RE,
    _TITLE_IN_BAR_COMPANY_RE,
    _TITLE_IN_BAR_RE,
)

# Pattern that matches non-alphanumeric characters (case-insensitive)
# Using [^a-zA-Z0-9]+ to preserve original case when cleaning company names
_NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9]+")

# Regex to strip common company suffixes (Inc, LLC, Corp, etc.)
_COMPANY_SUFFIX_RE = re.compile(
    r"((?:,\s*|\s+)(inc|inc\.|llc|ltd|limited|corp|corporation|co|company)\.?)$",
    flags=re.IGNORECASE,
)
_COMPANY_SUFFIX_VALUE_RE = re.compile(
    r"((?:,\s*|\s+)(inc|inc\.|llc|ltd|limited|corp|corporation|co)\.?)$",
    flags=re.IGNORECASE,
)

# Regex to strip trailing country names from company names (e.g., "Dataminr Australia" -> "Dataminr")
_COMPANY_COUNTRY_SUFFIX_RE = re.compile(
    r"\s+(Australia|Canada|UK|USA|US|Germany|France|India|Japan|Singapore|Ireland|"
    r"Netherlands|Switzerland|Brazil|Mexico|Spain|Italy|China|Korea|APAC|EMEA|LATAM)$",
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
    "ashbyhq",
    "avature",
    "brassring",
    "greenhouse",
    "icims",
    "indeed",
    "jibeapply",
    "lever",
    "linkedin",
    "smartrecruiters",
    "taleo",
    "workday",
    "ziprecruiter",
}

# Known company name mappings for proper capitalization/spacing
# Maps lowercase slug to properly formatted company name
_COMPANY_NAME_MAPPINGS: dict[str, str] = {
    # Casing corrections
    "coreweave": "CoreWeave",
    "mongodb": "MongoDB",
    "github": "GitHub",
    "hubspot": "HubSpot",
    "stubhub": "StubHub",
    "nexhealth": "NexHealth",
    "xai": "xAI",
    "docusign": "Docusign",
    "datadog": "Datadog",
    "dataminr": "Dataminr",
    "lsi": "Broadcom",
    # Space/expansion corrections
    "paloaltonetworks": "Palo Alto Networks",
    "palo alto networks": "Palo Alto Networks",
    "purestorage": "Pure Storage",
    "pure storage": "Pure Storage",
    "togetherai": "Together AI",
    "together ai": "Together AI",
    "thetradedesk": "The Trade Desk",
    "the trade desk": "The Trade Desk",
    # Suffix corrections
    "stubhubinc": "StubHub",
    # Name expansions
    "oscar": "Oscar Health",
    "metacareers": "Meta",
    # Special company names
    "ramp": "Ramp",
    "rubrik": "Rubrik Job Board",
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
    normalized_key = _NON_ALNUM_RE.sub(" ", cleaned).strip().lower()
    if not normalized_key or normalized_key in _GENERIC_COMPANY_HINTS:
        return None
    return cleaned


def normalize_company_value(value: str) -> str:
    """Normalize a company value from structured data or explicit fields."""
    if not isinstance(value, str):
        return _stringify(value)
    cleaned = value.strip()
    if not cleaned:
        return cleaned
    # Remove zero-width characters
    cleaned = cleaned.replace("\u200b", "").replace("\ufeff", "").strip()
    # Workday-style numeric prefixes (e.g., "8613 Broadcom Corporation")
    cleaned = re.sub(r"^\d{4,}\s+", "", cleaned)
    # Remove common company suffixes
    cleaned = _COMPANY_SUFFIX_VALUE_RE.sub("", cleaned).strip(" ,")
    # Remove trailing country names (e.g., "Dataminr Australia" -> "Dataminr")
    cleaned = _COMPANY_COUNTRY_SUFFIX_RE.sub("", cleaned).strip()
    # Normalize whitespace
    cleaned = " ".join(cleaned.split())
    return _apply_company_mapping(cleaned)


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
    normalized = _NON_ALNUM_RE.sub("", value.lower())
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
    normalized_company = _NON_ALNUM_RE.sub("", company.lower())
    normalized_hint = _NON_ALNUM_RE.sub("", hint.lower())
    if normalized_company and normalized_company == normalized_hint and hint != company:
        return hint
    return company


def _apply_company_mapping(company: str) -> str:
    """Apply known company name mappings for proper formatting."""
    if not company:
        return company
    # Check mapping with lowercase key
    normalized_key = company.lower().strip()
    if normalized_key in _COMPANY_NAME_MAPPINGS:
        return _COMPANY_NAME_MAPPINGS[normalized_key]
    return company


def derive_company_from_url(url: str) -> str:
    """Extract company name from a job posting URL.

    Handles various ATS URL patterns including Greenhouse, Workday, Avature,
    AshbyHQ, and generic career site URLs.

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
    path_parts = [p for p in parsed.path.split("/") if p]
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
                cleaned = _NON_ALNUM_RE.sub(" ", candidate).strip()
                if cleaned:
                    return _apply_company_mapping(cleaned.title())

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
                cleaned = _NON_ALNUM_RE.sub(" ", candidate).strip()
                if cleaned:
                    return _apply_company_mapping(cleaned.title())
            for candidate in reversed(subdomains):
                if not candidate or re.fullmatch(r"wd\d+", candidate):
                    continue
                cleaned = _NON_ALNUM_RE.sub(" ", candidate).strip()
                if cleaned:
                    return _apply_company_mapping(cleaned.title())

    # Greenhouse API URLs: boards-api.greenhouse.io/v1/boards/{company}/...
    # or api.greenhouse.io/v1/boards/{company}/...
    if hostname.endswith("greenhouse.io"):
        # Check for API path pattern: /v1/boards/{company}/...
        if len(path_parts) >= 3 and path_parts[0] == "v1" and path_parts[1] == "boards":
            slug = path_parts[2]
            if slug.lower() not in _INVALID_COMPANY_TOKENS:
                cleaned_slug = _NON_ALNUM_RE.sub(" ", slug).strip()
                if cleaned_slug and cleaned_slug.lower() not in _INVALID_COMPANY_TOKENS:
                    return _apply_company_mapping(cleaned_slug.title())
        # Standard Greenhouse board URLs: /{company}/jobs/...
        elif path_parts:
            slug = path_parts[0]
            # Skip language codes and generic path segments
            if slug.lower() not in _INVALID_COMPANY_TOKENS:
                cleaned_slug = _NON_ALNUM_RE.sub(" ", slug).strip()
                if cleaned_slug and cleaned_slug.lower() not in _INVALID_COMPANY_TOKENS:
                    return _apply_company_mapping(cleaned_slug.title())
        return ""

    # Lever URLs: jobs.lever.co/{company}/{job-id}
    if hostname.endswith("lever.co"):
        # Standard pattern: /{company}/{job-id}
        if path_parts:
            slug = path_parts[0]
            if slug.lower() not in _INVALID_COMPANY_TOKENS:
                cleaned_slug = _NON_ALNUM_RE.sub(" ", slug).strip()
                if cleaned_slug:
                    return _apply_company_mapping(cleaned_slug.title())
        return ""

    # AshbyHQ URLs: jobs.ashbyhq.com/{company}/... or api.ashbyhq.com/posting-api/job-board/{company}
    if hostname.endswith("ashbyhq.com"):
        # API pattern: /posting-api/job-board/{company}
        if len(path_parts) >= 3 and path_parts[0] == "posting-api" and path_parts[1] == "job-board":
            slug = path_parts[2]
            if slug.lower() not in _INVALID_COMPANY_TOKENS:
                cleaned_slug = _NON_ALNUM_RE.sub(" ", slug).strip()
                if cleaned_slug:
                    return _apply_company_mapping(cleaned_slug.title())
        # Standard pattern: /{company}/...
        elif path_parts:
            slug = path_parts[0]
            # Skip generic segments like 'positions', 'jobs'
            if slug.lower() not in _INVALID_COMPANY_TOKENS and slug.lower() not in {"positions", "jobs"}:
                cleaned_slug = _NON_ALNUM_RE.sub(" ", slug).strip()
                if cleaned_slug:
                    return _apply_company_mapping(cleaned_slug.title())
        return ""

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

    cleaned = _NON_ALNUM_RE.sub(" ", name).strip()
    return _apply_company_mapping(cleaned.title()) if cleaned else ""


__all__ = [
    # Constants
    "_COMPANY_SUFFIX_RE",
    "_GENERIC_COMPANY_HINTS",
    "_INVALID_COMPANY_TOKENS",
    "_JOB_BOARD_COMPANY_TOKENS",
    # Functions
    "normalize_company_hint",
    "normalize_company_value",
    "normalize_title_from_bar",
    "is_generic_company_name",
    "apply_company_hint",
    "derive_company_from_url",
]
