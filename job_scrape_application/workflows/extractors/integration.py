"""
Integration module for using extractors in the job scraping workflow.

This module provides functions to use the modular extractors in place of
the old scattered extraction logic in scrape_utils.py and heuristics.py.
"""

from __future__ import annotations

import logging
import re
from typing import Any

# Import directly to avoid circular imports
from .context import ExtractionContext
from .base import ExtractionResult, FieldExtractor
from .title_extractor import JobTitleExtractor
from .company_extractor import CompanyExtractor
from .location_extractor import LocationExtractor
from .remote_extractor import RemoteExtractor
from .level_extractor import LevelExtractor
from .compensation_extractor import CompensationExtractor
from .posted_at_extractor import PostedAtExtractor
from .description_extractor import DescriptionExtractor
from ..helpers.location_normalization import (
    _normalize_locations,
)
from ..helpers.compensation_parsing import normalize_compensation_value
from ..helpers.scrape_utils import strip_known_nav_blocks
from ...constants import is_remote_company

logger = logging.getLogger(__name__)


# All extractors (local to avoid circular imports)
_EXTRACTORS: dict[str, type[FieldExtractor]] = {
    "title": JobTitleExtractor,
    "company": CompanyExtractor,
    "location": LocationExtractor,
    "remote": RemoteExtractor,
    "level": LevelExtractor,
    "compensation": CompensationExtractor,
    "posted_at": PostedAtExtractor,
    "description": DescriptionExtractor,
}

_extractor_instances: dict[str, FieldExtractor] | None = None


def _get_extractors() -> dict[str, FieldExtractor]:
    """Get or create extractor instances."""
    global _extractor_instances
    if _extractor_instances is None:
        _extractor_instances = {
            name: cls() for name, cls in _EXTRACTORS.items()
        }
    return _extractor_instances


def _extract_job_fields(
    context: ExtractionContext,
    *,
    fields: list[str] | None = None,
    run_all: bool | None = None,
) -> dict[str, ExtractionResult]:
    """Extract all (or specified) job fields using modular extractors."""
    extractors = _get_extractors()

    if run_all is None:
        run_all = context.debug

    if fields is None:
        fields = list(extractors.keys())

    results: dict[str, ExtractionResult] = {}

    for field in fields:
        if field not in extractors:
            continue

        extractor = extractors[field]
        result = extractor.extract(context, run_all=run_all)
        results[field] = result

        # Update context with extracted values for cross-field dependencies
        if result.final_value is not None:
            if field == "title":
                context.extracted_title = result.final_value
            elif field == "company":
                context.extracted_company = result.final_value
            elif field == "location":
                context.extracted_location = result.final_value
            elif field == "remote":
                context.extracted_remote = result.final_value

    return results


# Location processing helpers (derived from heuristics.py)
_CANADIAN_PROVINCE_CODES = {
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT",
}
_CANADIAN_PROVINCE_NAMES = {
    "alberta", "british columbia", "manitoba", "new brunswick",
    "newfoundland and labrador", "nova scotia", "northwest territories",
    "nunavut", "ontario", "prince edward island", "quebec", "saskatchewan", "yukon",
}
_UNKNOWN_LOCATION_TOKENS = {"unknown", "n/a", "na", "unspecified", "not available"}
_US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia",
}
_US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}

COUNTRY_CODE_PATTERN = re.compile(r"^[A-Z]{2}$")
LOCATION_TOKEN_SPLIT_PATTERN = re.compile(r"[,;|/]+")


def _derive_location_states(locations: list[str]) -> list[str]:
    """Extract state components from location strings."""
    states: list[str] = []
    for loc in locations:
        parts = [p.strip() for p in str(loc).split(",") if p.strip()]
        if len(parts) >= 2:
            state_val = parts[-2] if len(parts) >= 3 else parts[-1]
            if state_val and state_val not in states:
                states.append(state_val)
    return states


def _derive_countries(locations: list[str]) -> list[str]:
    """Extract country from location strings."""
    countries: list[str] = []
    for loc in locations:
        parts = [p.strip() for p in str(loc).split(",") if p.strip()]
        if not parts:
            continue
        country = parts[-1]
        lowered = country.lower()
        country_upper = country.upper()
        mapped: str | None = None
        if "remote" in lowered:
            mapped = "United States"
        elif lowered in {"locations"}:
            continue
        elif lowered in _UNKNOWN_LOCATION_TOKENS:
            mapped = "United States"
        elif country_upper in _US_STATE_CODES:
            mapped = "United States"
        elif COUNTRY_CODE_PATTERN.match(country):
            if country_upper in _CANADIAN_PROVINCE_CODES:
                mapped = "Canada"
            else:
                continue
        elif lowered in _CANADIAN_PROVINCE_NAMES:
            mapped = "Canada"
        elif lowered in _US_STATE_NAMES:
            mapped = "United States"
        else:
            mapped = country
        if mapped and mapped not in countries:
            countries.append(mapped)
    return countries


def _build_location_search(locations: list[str]) -> str:
    """Build search string from locations."""
    tokens: set[str] = set()
    for loc in locations:
        for token in LOCATION_TOKEN_SPLIT_PATTERN.split(loc):
            cleaned = token.strip()
            if cleaned:
                tokens.add(cleaned)
    return " ".join(tokens)


def extract_job_from_scrape(
    url: str,
    markdown: str,
    *,
    raw_row: dict[str, Any] | None = None,
    handler: Any = None,
    structured_data: dict[str, Any] | None = None,
    site_configs: list[dict[str, Any]] | None = None,
    debug: bool = False,
) -> dict[str, Any] | None:
    """
    Extract job fields from SpiderCloud scrape result using modular extractors.

    This function replaces the extraction logic in _JobRowNormalizer.normalize_row().

    Args:
        url: Job URL
        markdown: Raw markdown content from SpiderCloud
        raw_row: Optional raw row data with pre-extracted fields
        handler: Site handler instance
        structured_data: Optional structured data (JSON-LD, API response)
        site_configs: Site-specific regex configurations
        debug: If True, run all strategies and log detailed trace

    Returns:
        Normalized job row dict, or None if extraction failed
    """
    # Create extraction context
    ctx = ExtractionContext.from_scrape_result(
        url=url,
        markdown=markdown,
        handler=handler,
        structured_data=structured_data,
        raw_row=raw_row or {},
        site_configs=site_configs or [],
        debug=debug,
    )

    # Extract all fields
    results = _extract_job_fields(ctx)

    # Get extracted values
    title = results.get("title")
    title_value = title.final_value if title and title.final_value else "Untitled"

    company = results.get("company")
    company_value = company.final_value if company and company.final_value else "Unknown"

    location = results.get("location")
    location_value = location.final_value if location and location.final_value else "Unknown"

    remote = results.get("remote")
    remote_value = bool(remote.final_value) if remote else False

    level = results.get("level")
    level_value = level.final_value if level and level.final_value else "mid"

    compensation = results.get("compensation")
    compensation_value = compensation.final_value if compensation else 0

    posted_at = results.get("posted_at")
    posted_at_value = posted_at.final_value if posted_at else None

    description = results.get("description")
    description_value = description.final_value if description and description.final_value else markdown

    # Apply remote company override
    if is_remote_company(company_value):
        remote_value = True

    # Process compensation
    total_comp = normalize_compensation_value(compensation_value) or 0
    compensation_unknown = total_comp <= 0

    # Process posted_at
    posted_at_ms: int | None = None
    posted_at_unknown = True
    if posted_at_value:
        if hasattr(posted_at_value, "timestamp"):
            posted_at_ms = int(posted_at_value.timestamp() * 1000)
            posted_at_unknown = False
        elif isinstance(posted_at_value, (int, float)):
            posted_at_ms = int(posted_at_value)
            posted_at_unknown = False

    # Build normalized row
    normalized_row: dict[str, Any] = {
        "job_title": title_value,
        "title": title_value,
        "company": company_value,
        "location": location_value,
        "remote": remote_value,
        "level": level_value,
        "total_compensation": int(total_comp) if total_comp else 0,
        "compensation_unknown": compensation_unknown,
        "url": url,
        "description": strip_known_nav_blocks(description_value),
        "posted_at": posted_at_ms,
        "posted_at_unknown": posted_at_unknown,
    }

    # Log debug trace if enabled
    if debug:
        from . import format_debug_trace
        logger.debug("Extraction trace:\n%s", format_debug_trace(results))

    return normalized_row


def build_heuristic_patch_from_extractors(
    row: dict[str, Any],
    configs: list[dict[str, Any]] | None = None,
    now_ms: int | None = None,
    *,
    debug: bool = False,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """
    Build heuristic patch for existing job row using modular extractors.

    This function replaces _build_job_detail_heuristic_patch() in heuristics.py.

    Args:
        row: Existing job row from database
        configs: Site-specific regex configurations
        now_ms: Current timestamp in milliseconds
        debug: If True, run all strategies and log detailed trace

    Returns:
        Tuple of (patch dict, records list)
    """
    import time as time_module

    if now_ms is None:
        now_ms = int(time_module.time() * 1000)

    url = row.get("url") or ""
    description = row.get("description") or ""
    company = row.get("company") or ""

    # Create extraction context from existing row
    ctx = ExtractionContext.from_scrape_result(
        url=url,
        markdown=description,
        raw_row=row,
        site_configs=configs or [],
        debug=debug,
    )

    # Extract fields
    results = _extract_job_fields(ctx)

    # Build patch from extraction results
    patch: dict[str, Any] = {
        "heuristicAttempts": int(row.get("heuristicAttempts") or 0) + 1,
        "heuristicLastTried": now_ms,
        "heuristicVersion": 5,  # Increment version for extractor-based heuristics
    }
    records: list[dict[str, str]] = []

    # Domain for records
    domain = ctx.domain or "default"

    # Title patch (only if current title is bad)
    title_result = results.get("title")
    if title_result and title_result.final_value:
        current_title = row.get("title") or row.get("jobTitle") or ""
        if _should_override_title(current_title) and title_result.final_value != current_title:
            patch["title"] = title_result.final_value
            patch["jobTitle"] = title_result.final_value

    # Location patch
    location_result = results.get("location")
    current_location = row.get("location") or ""
    if location_result and location_result.final_value:
        raw_location = location_result.final_value
        # Normalize and split multiple locations
        locations = _normalize_locations([raw_location])
        if locations:
            patch["locations"] = locations
            patch["location"] = locations[0]
            patch["locationStates"] = _derive_location_states(locations)
            patch["locationSearch"] = _build_location_search(locations)
            records.append({
                "domain": domain,
                "field": "location",
                "regex": f"extractor:{location_result.winning_strategy or 'unknown'}",
            })

            # Derive countries
            countries = _derive_countries(locations)
            if countries:
                patch["countries"] = countries
                patch["country"] = countries[0]
        elif raw_location and raw_location.lower() not in _UNKNOWN_LOCATION_TOKENS:
            # Fallback: use raw location if normalization failed but value is meaningful
            # This handles cases like URL-derived locations that aren't in the dictionary
            patch["location"] = raw_location
            patch["locationSearch"] = raw_location
            records.append({
                "domain": domain,
                "field": "location",
                "regex": f"extractor:{location_result.winning_strategy or 'unknown'}_raw",
            })

    # Remote patch
    remote_result = results.get("remote")
    scraper_remote = row.get("remote")
    company_remote = is_remote_company(company)

    if company_remote:
        patch["remote"] = True
    elif remote_result and remote_result.final_value is not None:
        # Don't override scraper's authoritative remote=True
        if scraper_remote is not True:
            if remote_result.final_value and scraper_remote is not True:
                patch["remote"] = True
            elif not remote_result.final_value and scraper_remote is not False:
                patch["remote"] = False

    # Compensation patch
    comp_result = results.get("compensation")
    current_comp = normalize_compensation_value(row.get("totalCompensation")) or 0
    if comp_result and comp_result.final_value and comp_result.final_value > 0:
        new_comp = normalize_compensation_value(comp_result.final_value)
        if new_comp and (not current_comp or current_comp <= 0):
            patch["totalCompensation"] = int(new_comp)
            patch["compensationUnknown"] = False
            patch["compensationReason"] = f"extractor:{comp_result.winning_strategy or 'heuristic'}"
            records.append({
                "domain": domain,
                "field": "compensation",
                "regex": f"extractor:{comp_result.winning_strategy or 'unknown'}",
            })

    # Description normalization
    desc_result = results.get("description")
    if desc_result and desc_result.final_value:
        normalized_desc = strip_known_nav_blocks(desc_result.final_value)
        if normalized_desc and normalized_desc != description:
            patch["description"] = normalized_desc

    # Log debug trace if enabled
    if debug:
        from . import format_debug_trace
        logger.debug("Heuristic extraction trace:\n%s", format_debug_trace(results))

    return patch, records


def _should_override_title(value: str) -> bool:
    """Check if title value should be overridden."""
    if not value:
        return True
    lowered = value.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", lowered).strip()

    # Generic/placeholder titles
    if normalized in {
        "the role", "our team", "the team", "role", "job description",
        "description", "description and requirements", "description requirements",
    }:
        return True
    if lowered in {"unknown", "n/a", "na", "untitled"}:
        return True

    # Titles that look like requirements, not job titles
    if re.search(r"\b\d+\+?\s+years?\b", lowered):
        return True
    if re.search(r"\byears?\s+(?:of\s+)?experience\b", lowered):
        return True
    if re.search(r"\byears?\s+working\b", lowered):
        return True
    if re.search(
        r"\bexperience\s+(?:in|with|providing|working|leading|managing|developing|designing|supporting)\b",
        lowered,
    ):
        return True
    if re.search(r"\bability\s+to\b", lowered):
        return True
    if re.search(r"\bknowledge\s+of\b", lowered):
        return True

    # Title looks like a sentence
    if lowered.endswith((".", "!", "?")):
        return True

    # Too long to be a title
    if len(lowered.split()) > 14:
        return True

    return False
