"""Site information extraction utilities.

Provides functions for extracting company and handler information from URLs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass
class SiteInfo:
    """Information extracted from a job site URL."""

    company: str
    """Company identifier (may include hyphens, e.g., 'pure-storage')"""

    normalized_company: str
    """Company identifier normalized for filesystem (e.g., 'pure_storage')"""

    handler: str
    """Handler type (e.g., 'greenhouse', 'ashby', 'workday')"""

    is_known_platform: bool
    """Whether this is a known job platform (vs custom career site)"""

    domain: str = ""
    """Domain extracted from URL"""

    suggested_listing_url: str = ""
    """Suggested listing page URL for this site"""


def extract_site_info_from_url(url: str) -> SiteInfo:
    """Extract site information from a job URL.

    Identifies the company, handler type, and other metadata from a job listing
    or detail URL. Supports major platforms (Greenhouse, Ashby, Workday, etc.)
    and attempts to infer company from domain for custom sites.

    Args:
        url: Job listing or detail URL

    Returns:
        SiteInfo with extracted metadata
    """
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    path = parsed.path.lower()

    # Check for known platforms first
    # Greenhouse
    if "greenhouse.io" in domain:
        match = re.search(r"/boards/([a-z0-9_-]+)", parsed.path, re.I)
        if match:
            company = match.group(1)
            return SiteInfo(
                company=company,
                normalized_company=company.lower().replace("-", "_"),
                handler="greenhouse",
                is_known_platform=True,
                domain=domain,
                suggested_listing_url=f"https://api.greenhouse.io/v1/boards/{company}/jobs",
            )

    # Ashby HQ
    if "ashbyhq.com" in domain:
        match = re.search(r"ashbyhq\.com/([a-z0-9_-]+)", url, re.I)
        if match:
            company = match.group(1)
            return SiteInfo(
                company=company,
                normalized_company=company.lower().replace("-", "_"),
                handler="ashby",
                is_known_platform=True,
                domain=domain,
                suggested_listing_url=f"https://jobs.ashbyhq.com/{company}",
            )

    # Lever
    if "lever.co" in domain:
        match = re.search(r"lever\.co/([a-z0-9_-]+)", url, re.I)
        if match:
            company = match.group(1)
            return SiteInfo(
                company=company,
                normalized_company=company.lower().replace("-", "_"),
                handler="lever",
                is_known_platform=True,
                domain=domain,
                suggested_listing_url=f"https://jobs.lever.co/{company}",
            )

    # Workday
    if "workday.com" in domain or "myworkdayjobs.com" in domain:
        match = re.search(r"([a-z0-9]+)\.wd\d+\.myworkdayjobs\.com", domain, re.I)
        company = match.group(1) if match else ""
        return SiteInfo(
            company=company,
            normalized_company=company.lower() if company else "",
            handler="workday",
            is_known_platform=True,
            domain=domain,
            suggested_listing_url="",  # Workday listing URLs are complex
        )

    # Kula
    if "kula.ai" in domain:
        match = re.search(r"careers\.kula\.ai/([a-z0-9_-]+)", url, re.I)
        if match:
            company = match.group(1)
            return SiteInfo(
                company=company,
                normalized_company=company.lower().replace("-", "_"),
                handler="kula",
                is_known_platform=True,
                domain=domain,
                suggested_listing_url=f"https://careers.kula.ai/{company}",
            )

    # Netflix
    if "netflix" in domain:
        return SiteInfo(
            company="netflix",
            normalized_company="netflix",
            handler="netflix",
            is_known_platform=True,
            domain=domain,
            suggested_listing_url="https://explore.jobs.netflix.net/careers",
        )

    # Meta
    if "metacareers" in domain or "facebook.com/careers" in url:
        return SiteInfo(
            company="meta",
            normalized_company="meta",
            handler="meta",
            is_known_platform=True,
            domain=domain,
            suggested_listing_url="https://www.metacareers.com/jobs",
        )

    # Generic career sites - extract company from domain
    # Try common patterns: careers.company.com, jobs.company.com, company.com/careers
    if domain.startswith("careers.") or domain.startswith("jobs."):
        parts = domain.split(".")
        if len(parts) >= 2:
            company = parts[1]
            normalized = company.lower().replace("-", "_")

            # Try to construct a listing URL
            suggested_listing = ""
            if "/job/" in path or "/jobs/" in path:
                # URL likely has job detail - try to get base
                base_path = re.sub(r"/jobs?/[^/]+.*$", "/jobs", path)
                suggested_listing = f"https://{domain}{base_path}"
            else:
                suggested_listing = f"https://{domain}/careers"

            return SiteInfo(
                company=company,
                normalized_company=normalized,
                handler=normalized,  # Use company name as handler for custom sites
                is_known_platform=False,
                domain=domain,
                suggested_listing_url=suggested_listing,
            )

    # Fallback: use domain as company name
    parts = domain.replace("www.", "").split(".")
    company = parts[0]
    normalized = company.lower().replace("-", "_")

    # Generic listing URL guess
    suggested_listing = ""
    if "/careers" in path:
        suggested_listing = f"https://{domain}/careers"
    elif "/jobs" in path:
        suggested_listing = f"https://{domain}/jobs"
    else:
        suggested_listing = f"https://{domain}/careers"

    return SiteInfo(
        company=company,
        normalized_company=normalized,
        handler=normalized,
        is_known_platform=False,
        domain=domain,
        suggested_listing_url=suggested_listing,
    )


def get_canonical_detail_url(
    detail_url: str,
    source_url: Optional[str] = None,
) -> str:
    """Get canonical detail URL, converting marketing pages to API URLs if needed.

    For Greenhouse marketing pages, converts to API URL. For other handlers,
    returns the URL as-is.

    Args:
        detail_url: Job detail URL (may be marketing or API)
        source_url: Optional source listing URL for context

    Returns:
        Canonical detail URL (API URL for Greenhouse, original for others)
    """
    from job_scrape_application.workflows.site_handlers import get_site_handler

    handler = get_site_handler(detail_url)
    if not handler:
        return detail_url

    # For Greenhouse, convert marketing URL to API URL
    if handler.name == "greenhouse":
        api_url = handler.get_api_uri(detail_url, source_url=source_url or "")
        if api_url:
            return api_url

    return detail_url
