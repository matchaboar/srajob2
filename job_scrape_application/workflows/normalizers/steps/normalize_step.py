"""Normalize step: Standardize field formats.

This step handles:
- Location normalization (state abbreviations, country codes)
- Company name standardization
- URL cleanup
- Compensation bounds checking
"""

from __future__ import annotations

import html
import logging
import re
from typing import Iterable, List

from ..types import ParsedContent, ExtractedFields, NormalizedJob, NORMALIZATION_VERSION
from ...helpers.compensation_parsing import normalize_compensation_value

logger = logging.getLogger(__name__)


def _normalize_text(value: str | None) -> str:
    """Normalize text by unescaping HTML entities and stripping whitespace."""
    if not value:
        return ""
    return html.unescape(value).strip()


# Pattern to split multiple locations (semicolon, pipe, or slash)
LOCATION_SPLIT_PATTERN = r"[;|/]"
MULTI_SPACE_PATTERN = r"\s+"

# Location tokens that indicate unknown/unspecified
_UNKNOWN_LOCATION_TOKENS = {
    "unknown",
    "unspecified",
    "n/a",
    "na",
    "none",
    "anywhere",
    "global",
    "worldwide",
}

# US state abbreviations for state extraction
_US_STATE_ABBREVS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR",
}


def normalize_fields(
    extracted: ExtractedFields,
    parsed: ParsedContent,
) -> NormalizedJob:
    """
    Normalize extracted fields into final format.

    Args:
        extracted: Raw extracted fields
        parsed: Parsed content for context

    Returns:
        NormalizedJob with standardized values
    """
    # Normalize location
    # Prefer locations_raw (from hints) over single location string
    location, locations, location_states, location_search, countries, country = (
        _normalize_location_data(
            extracted.location,
            locations_raw=extracted.locations_raw,
        )
    )

    # Default countries to US when remote or location unknown
    is_remote = extracted.is_remote or False
    location_unknown = not location or (location and location.lower().strip() in _UNKNOWN_LOCATION_TOKENS)
    if not countries and (is_remote or location_unknown):
        countries = ["United States"]
        country = "United States"

    # Normalize compensation
    comp_min, comp_max = _normalize_compensation(
        extracted.compensation_min,
        extracted.compensation_max,
    )

    # Unescape HTML entities in text fields
    title = _normalize_text(extracted.title)
    company = _normalize_text(extracted.company)

    return NormalizedJob(
        url=parsed.url,
        title=title,
        company=company,
        location=location,
        locations=locations,
        location_states=location_states,
        location_search=location_search,
        countries=countries,
        country=country,
        compensation_min=comp_min,
        compensation_max=comp_max,
        compensation_text=extracted.compensation_text,
        posted_at=extracted.posted_at,
        posted_at_unknown=extracted.posted_at_unknown,
        level=extracted.level,
        is_remote=extracted.is_remote or False,
        description=extracted.description,
        handler=parsed.handler_name,
        normalization_version=NORMALIZATION_VERSION,
    )


def _is_plausible_location(value: str) -> bool:
    """Check if value looks like a plausible location."""
    lowered = value.lower()
    # Reject HR/benefits terminology
    if any(token in lowered for token in (
        "diversity", "equity", "inclusion", "benefits", "culture",
        "salary", "compensation", "pay", "package", "bonus", "range"
    )):
        return False
    if "$" in value or "401k" in lowered or "401(k" in lowered:
        return False
    # Reject job title words
    job_title_words = {
        "executive", "engineer", "manager", "analyst", "designer", "specialist",
        "developer", "director", "lead", "senior", "junior", "staff", "principal",
        "sales", "account", "marketing", "operations", "product", "project",
        "intern", "associate", "coordinator", "administrator", "consultant",
        "architect", "scientist", "researcher", "writer", "editor", "strategist",
    }
    if any(word in lowered for word in job_title_words):
        return False
    # Reject description fragments
    description_fragments = {
        "we're", "we are", "you'll", "you will", "our", "their", "pursuing",
        "society", "mission", "explorers", "join", "team", "company",
        "about", "looking", "seeking", "building", "creating", "developing",
    }
    if any(fragment in lowered for fragment in description_fragments):
        return False
    if "," in value:
        segments = [p.strip() for p in value.split(",") if p.strip()]
        if len(segments) > 3:
            return False
        if any(len(seg.split()) > 3 for seg in segments):
            return False
        if any("remote" in seg.lower() for seg in segments[1:]):
            return True
        return True
    if "remote" in lowered:
        return True
    return len(value.split()) <= 4


def _split_and_normalize_locations(raw_locations: Iterable[str]) -> List[str]:
    """Split and dedupe multiple location hints (e.g., 'Madrid, Spain; Paris, France')."""
    seen: set[str] = set()
    cleaned: List[str] = []
    for raw in raw_locations:
        if not raw:
            continue
        for part in re.split(LOCATION_SPLIT_PATTERN, str(raw)):
            candidate = (part or "").strip(" ;|/\t")
            if not candidate:
                continue
            candidate = re.sub(MULTI_SPACE_PATTERN, " ", candidate)
            lowered = candidate.lower()
            if lowered in _UNKNOWN_LOCATION_TOKENS:
                continue
            if len(candidate) < 3 or len(candidate) > 100:
                continue
            if not _is_plausible_location(candidate):
                continue
            if candidate not in seen:
                seen.add(candidate)
                cleaned.append(candidate)
    return cleaned[:5]


def _normalize_location_data(
    raw_location: str | None,
    locations_raw: list[str] | None = None,
) -> tuple[str | None, list[str], list[str], str | None, list[str], str | None]:
    """
    Normalize location string into structured data.

    Args:
        raw_location: Single location string (may contain multiple locations separated by ; | /)
        locations_raw: Pre-split list of raw locations (from hints). If provided, takes precedence.

    Returns:
        Tuple of (location, locations, location_states, location_search, countries, country)
    """
    # If we have pre-split raw locations from hints, use them directly
    if locations_raw:
        locations = _split_and_normalize_locations(locations_raw)
    elif raw_location:
        # Check for unknown tokens
        if raw_location.lower().strip() in _UNKNOWN_LOCATION_TOKENS:
            return None, [], [], None, [], None

        # Split and normalize multiple locations (e.g., "Madrid, Spain; Paris, France")
        locations = _split_and_normalize_locations([raw_location])

        if not locations:
            # Fallback: use raw location if normalization produced nothing
            if _is_plausible_location(raw_location):
                locations = [raw_location]
    else:
        return None, [], [], None, [], None

    if not locations:
        return None, [], [], None, [], None

    # Primary location
    location = locations[0] if locations else None

    # Extract states from locations
    location_states = _derive_location_states(locations)

    # Build search string
    location_search = _build_location_search(locations)

    # Derive countries
    countries = _derive_countries(locations)
    country = countries[0] if countries else None

    return location, locations, location_states, location_search, countries, country


def _derive_location_states(locations: list[str]) -> list[str]:
    """Extract US state abbreviations from location strings."""
    states: list[str] = []
    seen: set[str] = set()

    for loc in locations:
        # Match state abbreviations (e.g., "CA" in "San Francisco, CA")
        for part in loc.replace(",", " ").split():
            part_upper = part.upper().strip()
            if part_upper in _US_STATE_ABBREVS and part_upper not in seen:
                states.append(part_upper)
                seen.add(part_upper)

    return states


def _build_location_search(locations: list[str]) -> str | None:
    """Build searchable location string."""
    if not locations:
        return None

    # Join all locations for search
    return "; ".join(locations)


def _derive_countries(locations: list[str]) -> list[str]:
    """Derive country names from location strings."""
    countries: list[str] = []
    seen: set[str] = set()

    # Common country patterns
    country_patterns = {
        "united states": "United States",
        "usa": "United States",
        "us": "United States",
        "canada": "Canada",
        "uk": "United Kingdom",
        "united kingdom": "United Kingdom",
        "germany": "Germany",
        "france": "France",
        "india": "India",
        "australia": "Australia",
        "japan": "Japan",
        "singapore": "Singapore",
        "ireland": "Ireland",
        "netherlands": "Netherlands",
        "israel": "Israel",
        "brazil": "Brazil",
        "mexico": "Mexico",
        "spain": "Spain",
        "china": "China",
        "south korea": "South Korea",
        "korea": "South Korea",
    }

    for loc in locations:
        loc_lower = loc.lower()
        for pattern, country in country_patterns.items():
            if pattern in loc_lower and country not in seen:
                countries.append(country)
                seen.add(country)
                break

    # Default to US if we found US states but no country
    if not countries:
        for loc in locations:
            for part in loc.replace(",", " ").split():
                if part.upper().strip() in _US_STATE_ABBREVS:
                    if "United States" not in seen:
                        countries.append("United States")
                        seen.add("United States")
                    break

    return countries


def _normalize_compensation(
    comp_min: int | None,
    comp_max: int | None,
) -> tuple[int | None, int | None]:
    """Normalize and validate compensation values."""
    if comp_min is not None:
        comp_min = normalize_compensation_value(comp_min)
    if comp_max is not None:
        comp_max = normalize_compensation_value(comp_max)

    # Ensure min <= max
    if comp_min is not None and comp_max is not None:
        if comp_min > comp_max:
            comp_min, comp_max = comp_max, comp_min

    return comp_min, comp_max
