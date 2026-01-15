"""Page detection utilities for job scraping.

This module provides heuristic functions for detecting various page types
during job scraping, including error pages, listing pages, and job detail pages.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from .location_normalization import (
    _COUNTRY_KEY_TO_LABEL,
    _STATE_ABBR_BY_NAME,
    _normalize_location_key,
)
from .regex_patterns import (
    DIGIT_PATTERN,
    ERROR_404_PATTERN,
    WHITESPACE_PATTERN,
    _LISTING_SELECT_RE,
    _LISTING_TABLE_HEADER_RE,
)

# Phrases that typically appear on error/expired job landing pages. We only
# evaluate the first few hundred characters of title+body to avoid false
# positives from legitimate descriptions that happen to contain similar
# language deeper in the text.
_ERROR_LANDING_PHRASES = (
    "page not found",
    "page was not found",
    "requested page was not found",
    "job not found",
    "posting not found",
    "we can't find what you're looking for",
    "we can't find what you're looking for",
    "could not find what you're looking for",
    "couldn't find what you're looking for",
    "no longer available",
    "no longer accepting applications",
    "no longer taking applications",
    "position has been filled",
    "position filled",
    "job has been filled",
    "job posting has expired",
    "posting has expired",
    "job has expired",
    "job is closed",
    "posting is closed",
)

# Terms that suggest a job listing/filter page rather than a single job
_LISTING_FILTER_TERMS = (
    "open positions",
    "open position",
    "search for opportunities",
    "search for jobs",
    "search jobs",
    "select department",
    "select country",
    "select location",
    "select city",
    "select state",
    "select category",
    "search category",
    "all locations",
    "all teams",
    "all roles",
    "all types",
    "view openings",
    "available in multiple locations",
    "job fairs",
    "work programs",
    "view all jobs",
    "filter by",
)

# Markers on listing card snippets
_LISTING_CARD_APPLY_MARKERS = (
    "direct apply",
    "apply with ai",
    "apply now",
    "view job",
    "view details",
)

# URL tokens indicating a listing page
_LISTING_URL_TOKENS = {
    "jobs",
    "careers",
    "career",
    "positions",
    "openings",
}

# Regex for detecting "posted X ago" patterns
_LISTING_CARD_POSTED_RE = re.compile(r"\bposted\b.{0,40}\bago\b")

# Markers indicating a job detail page (not a listing)
_JOB_DETAIL_MARKERS = (
    "responsibilities",
    "requirements",
    "qualifications",
    "what you'll do",
    "what you will do",
    "about the role",
    "about the position",
    "who you are",
    "benefits",
    "compensation",
    "salary",
    "equal opportunity",
)


def looks_like_error_landing(title: str | None, description: str) -> bool:
    """Heuristically detect generic error/expired landing pages.

    Many career sites return a branded 404/"job closed" page that still contains
    navigation text. These pages shouldn't be stored as jobs. We look for strong
    error phrases and the presence of "404" near the top of the combined
    title+body.

    Args:
        title: Page title or None
        description: Page description/content

    Returns:
        True if the page appears to be an error/expired page
    """
    haystack = f"{title or ''} {description or ''}".lower()
    sample = re.sub(WHITESPACE_PATTERN, " ", haystack)[:700]

    if re.search(ERROR_404_PATTERN, sample):
        return True

    for phrase in _ERROR_LANDING_PHRASES:
        if phrase in sample:
            return True

    return False


def _url_suggests_listing(url: str | None) -> bool:
    """Check if a URL pattern suggests it's a listing page by location.

    Args:
        url: The URL to check

    Returns:
        True if the URL pattern suggests a listing page
    """
    if not url:
        return False
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    segments = [seg for seg in (parsed.path or "").split("/") if seg]
    for idx, seg in enumerate(segments[:-1]):
        if seg not in {"job", "jobs", "career", "careers"}:
            continue
        slug = segments[idx + 1]
        if not slug or re.search(DIGIT_PATTERN, slug):
            return False
        normalized = _normalize_location_key(slug.replace("-", " ").replace("_", " "))
        if not normalized:
            return False
        if normalized in _COUNTRY_KEY_TO_LABEL:
            return True
        for state_name in _STATE_ABBR_BY_NAME:
            if _normalize_location_key(state_name) in normalized:
                return True
        if "remote" in normalized:
            return True
    return False


def _url_is_listing_root(url: str | None) -> bool:
    """Check if a URL is a listing root (e.g., /jobs, /careers).

    Args:
        url: The URL to check

    Returns:
        True if the URL is a listing root
    """
    if not url:
        return False
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    segments = [seg for seg in (parsed.path or "").split("/") if seg]
    if not segments:
        return False
    if any(re.search(DIGIT_PATTERN, seg) for seg in segments):
        return False
    return segments[-1].lower() in _LISTING_URL_TOKENS


def _description_mentions_listing_url(description: str) -> bool:
    """Check if description contains URLs that look like listing pages.

    Args:
        description: The page description/content

    Returns:
        True if a listing URL is mentioned
    """
    if not description:
        return False
    for match in re.findall(r"https?://\S+", description):
        cleaned = match.rstrip(").,;]\"'")
        if _url_is_listing_root(cleaned):
            return True
    return False


def _looks_like_listing_card_snippet(
    sample: str,
    description: str,
    url: str | None,
    detail_hits: int,
) -> bool:
    """Check if content looks like a listing card snippet.

    Args:
        sample: Normalized lowercase sample of content
        description: Original description
        url: Page URL
        detail_hits: Count of job detail markers found

    Returns:
        True if content looks like a listing card
    """
    if detail_hits:
        return False
    trimmed = description.strip()
    if not trimmed or len(trimmed) > 500:
        return False
    word_count = len(re.findall(r"\w+", trimmed))
    if word_count > 120:
        return False
    line_count = len([line for line in description.splitlines() if line.strip()])
    if line_count > 14:
        return False
    apply_hits = sum(1 for marker in _LISTING_CARD_APPLY_MARKERS if marker in sample)
    if apply_hits == 0:
        return False
    listing_url_present = _description_mentions_listing_url(description) or _url_is_listing_root(url)
    if not listing_url_present:
        return False
    posted_hit = bool(_LISTING_CARD_POSTED_RE.search(sample)) or ("posted" in sample and "ago" in sample)
    if posted_hit:
        return True
    return apply_hits >= 2 and word_count <= 80


def looks_like_job_listing_page(title: str | None, description: str, url: str | None = None) -> bool:
    """Heuristically detect job board listing/filter pages rather than a single job.

    Uses multiple heuristics including:
    - Presence of filter terms
    - Select/dropdown patterns
    - Table headers
    - Link density
    - URL patterns

    Args:
        title: Page title or None
        description: Page description/content
        url: Page URL or None

    Returns:
        True if the page appears to be a job listing page
    """
    if not description:
        return False
    haystack = f"{title or ''} {description or ''}".lower()
    sample = re.sub(WHITESPACE_PATTERN, " ", haystack)[:2000]
    link_hits = description.count("](")
    marker_hits = sum(1 for marker in _LISTING_FILTER_TERMS if marker in sample)
    select_hits = len(_LISTING_SELECT_RE.findall(sample))
    table_hits = bool(_LISTING_TABLE_HEADER_RE.search(sample))
    detail_hits = sum(1 for marker in _JOB_DETAIL_MARKERS if marker in sample)

    if "open positions" in sample and ("search for opportunities" in sample or select_hits >= 1):
        return True
    if table_hits and link_hits >= 5:
        return True
    if marker_hits >= 4:
        return True
    if marker_hits >= 3 and select_hits >= 1:
        return True
    if select_hits >= 3 and marker_hits >= 1:
        return True
    if link_hits >= 8 and marker_hits >= 2:
        return True
    if marker_hits >= 2 and _url_suggests_listing(url):
        return True
    if _looks_like_listing_card_snippet(sample, description, url, detail_hits):
        return True
    if detail_hits >= 2 and marker_hits <= 2 and select_hits < 2:
        return False

    return False


# Non-job page indicators - URLs or pages that should not be treated as job postings
_NON_JOB_URL_PATTERNS = (
    # Login/auth pages
    "/login",
    "/signin",
    "/sign-in",
    "/signup",
    "/sign-up",
    "/auth",
    "/sso",
    "/land/jobs",  # Greenhouse API landing pages (not actual jobs)
    "/land/jobsnotice",
    # Privacy/policy pages
    "/privacy",
    "/privacy-policy",
    "/terms",
    "/cookie",
    "/legal",
    "/notice",
    "/notices",
    "/acceptable-use",
    "/applicantprivacypolicy",
    # Accommodation/forms
    "/webform/",
    "/accommodation",
    # Note: LinkedIn company pages are handled via domain check, not path pattern
)

# Domains that are never valid job sources
_NON_JOB_DOMAINS = {
    "instagram.com",
    "twitter.com",
    "x.com",
    "facebook.com",
    "linkedin.com",  # Company pages, not job postings
    "edpb.europa.eu",
    "hracuity.net",  # HR accommodation forms
    "convex.site",  # Internal application share links
    "investors.",  # Investor relations
    "esg.",  # ESG pages
    "privacy.",  # Privacy centers
}

# Titles that indicate non-job pages
_INVALID_TITLE_PATTERNS = (
    # Markdown/HTML artifacts
    r"^#\s*",  # Markdown headers (#Sub Title3)
    r"^\[\d",  # Partial markdown links starting with digits ([6. Personal Information)
    # Don't reject all titles starting with [ as some use [Team Name] prefixes
    # e.g., [쿠팡 페이]사내변호사 (Coupang Pay In-house Lawyer)
    # Generic non-job titles
    r"^sign\s*in$",
    r"^sign\s*up$",
    r"^login$",
    r"^untitled$",
    r"^notice\b",
    r"^privacy\b",
    r"^members?\s*en$",
    r"^www\.",  # URL as title (Www.Coupang.Com)
)

# Compiled regex for invalid titles
_INVALID_TITLE_RE = re.compile(
    "|".join(_INVALID_TITLE_PATTERNS),
    flags=re.IGNORECASE,
)


def is_invalid_job_url(url: str | None) -> bool:
    """Check if a URL is definitely not a valid job posting URL.

    Detects:
    - Anchor fragments (#sub-title3)
    - Login/auth pages
    - Privacy/policy pages
    - Social media URLs
    - Accommodation forms
    - Company homepages (root paths)

    Args:
        url: The URL to check

    Returns:
        True if the URL is definitely not a job posting
    """
    if not url:
        return True
    cleaned = url.strip()
    if not cleaned:
        return True

    # Reject anchor fragments
    if cleaned.startswith("#"):
        return True

    # Must be a valid URL
    try:
        parsed = urlparse(cleaned)
    except Exception:
        return True

    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower().rstrip("/")

    # Check for non-job domains
    for domain in _NON_JOB_DOMAINS:
        if domain.startswith("."):
            if host.endswith(domain) or host == domain[1:]:
                return True
        elif domain.endswith("."):
            if host.startswith(domain) or any(
                part.startswith(domain) for part in host.split(".")
            ):
                return True
        else:
            if host.endswith(domain) or host == domain:
                return True

    # Check for non-job URL patterns
    for pattern in _NON_JOB_URL_PATTERNS:
        if pattern in path:
            return True

    # Special cases
    # Greenhouse API landing pages with language codes
    if "greenhouse.io" in host and "/land/" in path:
        return True

    # Static files (PDFs, investor documents)
    if "/static-files/" in path:
        return True

    # Reject company homepages (root path with no job-related segments)
    # e.g., http://www.coupang.com, https://company.com/
    if not path or path == "":
        # Homepage - only allow if it's a known job board domain
        job_board_patterns = ["greenhouse.io", "lever.co", "ashbyhq.com", "workday", "jobs.", "careers."]
        if not any(p in host for p in job_board_patterns):
            return True

    return False


def is_invalid_job_title(title: str | None) -> bool:
    """Check if a title is definitely not a valid job title.

    Detects:
    - Markdown artifacts (#Sub Title3, [6. Personal...)
    - Generic page titles (Sign In, Untitled)
    - URL-as-title patterns (Www.Example.Com)

    Args:
        title: The title to check

    Returns:
        True if the title is definitely not a job title
    """
    if not title:
        return True
    cleaned = title.strip()
    if not cleaned:
        return True

    # Check against invalid patterns
    if _INVALID_TITLE_RE.search(cleaned):
        return True

    # Additional checks
    lower = cleaned.lower()

    # Reject if title is just a URL fragment
    if lower.startswith("http://") or lower.startswith("https://"):
        return True

    # Reject pure emoji/symbol titles (allow Unicode letters including CJK)
    # Use \w to match any Unicode word character (letters, digits, underscore)
    # Also explicitly allow common CJK and other non-Latin scripts
    if not re.search(r"[\w\u4E00-\u9FFF\uAC00-\uD7AF\u3040-\u309F\u30A0-\u30FF]", cleaned):
        return True

    # Reject very short titles that are likely garbage
    if len(cleaned) < 3:
        return True

    return False


def looks_like_non_job_page(title: str | None, description: str | None, url: str | None = None) -> bool:
    """Heuristically detect pages that are not job postings.

    Combines URL, title, and content analysis to detect:
    - Login/authentication pages
    - Privacy policy pages
    - Social media profiles
    - Accommodation forms
    - Other non-job content

    Args:
        title: Page title or None
        description: Page description/content or None
        url: Page URL or None

    Returns:
        True if the page is likely not a job posting
    """
    # Check URL first (fastest)
    if url and is_invalid_job_url(url):
        return True

    # Check title
    if title and is_invalid_job_title(title):
        return True

    # Check description content for non-job indicators
    if description:
        desc_lower = description.lower()[:1000]  # Check first 1000 chars

        # Login page indicators
        login_indicators = [
            "sign in with google",
            "forgot your password",
            "keep me signed in",
            "continue to sso",
        ]
        if sum(1 for ind in login_indicators if ind in desc_lower) >= 2:
            return True

        # Privacy page indicators
        privacy_indicators = [
            "privacy notice",
            "privacy policy",
            "personal information collected",
            "personal information safeguard",
        ]
        if sum(1 for ind in privacy_indicators if ind in desc_lower) >= 2:
            return True

        # Social media page indicators
        social_indicators = [
            "posts", "followers", "following",
        ]
        if all(ind in desc_lower for ind in social_indicators):
            # Also check for profile-like structure
            if "@" in description or "verified" in desc_lower:
                return True

    return False


__all__ = [
    # Constants
    "_ERROR_LANDING_PHRASES",
    "_LISTING_FILTER_TERMS",
    "_LISTING_CARD_APPLY_MARKERS",
    "_LISTING_URL_TOKENS",
    "_LISTING_CARD_POSTED_RE",
    "_JOB_DETAIL_MARKERS",
    "_NON_JOB_URL_PATTERNS",
    "_NON_JOB_DOMAINS",
    "_INVALID_TITLE_PATTERNS",
    "_INVALID_TITLE_RE",
    # Functions
    "looks_like_error_landing",
    "_url_suggests_listing",
    "_url_is_listing_root",
    "_description_mentions_listing_url",
    "_looks_like_listing_card_snippet",
    "looks_like_job_listing_page",
    "is_invalid_job_url",
    "is_invalid_job_title",
    "looks_like_non_job_page",
]
