"""URL handling utilities for job scraping.

This module provides functions for scoring, filtering, and normalizing
URLs from job postings, with special handling for common ATS platforms.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


def _score_apply_url(url: str) -> int:
    """Score a URL to prefer company-hosted URLs over API endpoints.

    Higher scores are better. We want to avoid sending applicants to
    boards-api/api.greenhouse.io when a marketing/careers link exists.

    Args:
        url: The URL to score

    Returns:
        Score from -1 to 2, higher is better
    """
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""

    if "boards-api.greenhouse.io" in host or host.startswith("api.greenhouse.io"):
        return 0  # least preferred: raw API endpoints
    if host.endswith("greenhouse.io"):
        return 1  # fallback: hosted Greenhouse job page
    if host:
        return 2  # best: company-owned domain
    return -1


def _strip_ashby_application_url(url: str) -> str:
    """Return the Ashby job overview URL when given an /application URL.

    Args:
        url: The URL to process

    Returns:
        The job overview URL (without /application suffix)
    """
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


def _apply_url_candidates(row: Dict[str, Any]) -> List[str]:
    """Collect plausible apply URLs from a normalized/raw row.

    Args:
        row: Dictionary containing job data

    Returns:
        List of candidate apply URLs
    """
    fields = (
        "apply_url",
        "applyUrl",
        "company_url",
        "companyUrl",
        "absolute_apply_url",
        "absoluteApplyUrl",
        "absolute_applyUrl",
        "absolute_apply_url",
        "absolute_url",
        "absoluteUrl",
        "job_url",
        "jobUrl",
        "url",
        "link",
        "href",
        "_url",
    )

    candidates: List[str] = []
    for key in fields:
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            candidates.append(val.strip())
    return candidates


def prefer_apply_url(row: Dict[str, Any]) -> Optional[str]:
    """Return the preferred apply URL with a bias toward company domains.

    Args:
        row: Dictionary containing job data

    Returns:
        The best apply URL found, or None
    """
    candidates = _apply_url_candidates(row)
    if not candidates:
        return None

    best = None
    best_score = -2
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        normalized = _strip_ashby_application_url(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        score = _score_apply_url(normalized)
        if score > best_score:
            best = normalized
            best_score = score

    return best


def _first_string(value: Any) -> Optional[str]:
    """Extract first string from a value that might be a list or string.

    Local implementation to avoid circular imports.

    Args:
        value: A string, list, or other value

    Returns:
        First string found, or None
    """
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    return None


def _first_url(value: Any) -> Optional[str]:
    """Extract first URL from a value.

    Args:
        value: A string or list potentially containing URLs

    Returns:
        First valid URL found, or None
    """
    candidate = _first_string(value)
    if not candidate:
        return None
    if candidate.startswith(("http://", "https://")):
        return candidate
    return None


__all__ = [
    "_score_apply_url",
    "_strip_ashby_application_url",
    "_apply_url_candidates",
    "prefer_apply_url",
    "_first_url",
]
