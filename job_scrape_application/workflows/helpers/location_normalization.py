"""Location normalization utilities for job scraping.

This module provides functions for normalizing, resolving, and formatting
location data from various sources including job postings, URLs, and
structured data.
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

from .regex_patterns import (
    LOCATION_KEY_BOUNDARY_PATTERN_TEMPLATE,
    LOCATION_SPLIT_PATTERN,
    NON_ALNUM_SPACE_PATTERN,
    PARENTHETICAL_PATTERN,
    WHITESPACE_PATTERN,
)

def _stringify_value(value: Any) -> str:
    """Convert a value to string, stripping whitespace.

    This is a local implementation to avoid circular imports with scrape_utils.
    """
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return str(value)

# US State abbreviation to full name mapping
_STATE_NAME_BY_ABBR: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}


def _normalize_location_key(value: str) -> str:
    """Normalize a location string for dictionary lookup.

    Strips diacritics, removes parentheticals, and normalizes whitespace.

    Args:
        value: The location string to normalize

    Returns:
        A normalized lowercase string for lookup
    """
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    lowered = normalized.lower()
    lowered = re.sub(PARENTHETICAL_PATTERN, " ", lowered)
    lowered = re.sub(NON_ALNUM_SPACE_PATTERN, " ", lowered)
    lowered = re.sub(WHITESPACE_PATTERN, " ", lowered)
    return lowered.strip()


# Derived state lookup dictionaries
_STATE_ABBR_BY_NAME: Dict[str, str] = {name: abbr for abbr, name in _STATE_NAME_BY_ABBR.items()}
_STATE_ABBR_BY_KEY: Dict[str, str] = {
    _normalize_location_key(name): abbr for name, abbr in _STATE_ABBR_BY_NAME.items()
}


def _normalize_us_city_state(value: str) -> Optional[str]:
    """Normalize a US city, state string to standard format.

    Args:
        value: A string like "San Francisco, California"

    Returns:
        Normalized format like "San Francisco, CA" or None if invalid
    """
    if "," not in value:
        return None
    parts = [part.strip() for part in value.split(",") if part.strip()]
    if len(parts) != 2:
        return None
    city, state_raw = parts
    if not city or city.lower() == "remote":
        return None
    state_key = _normalize_location_key(state_raw)
    state_abbr = _STATE_ABBR_BY_KEY.get(state_key)
    if not state_abbr:
        state_upper = state_raw.strip().upper()
        if state_upper in _STATE_NAME_BY_ABBR:
            state_abbr = state_upper
    if not state_abbr:
        return None
    return f"{city}, {state_abbr}"


def _format_location_label(city: str | None, state: str | None, country: str | None = None) -> str:
    """Format location components into a display label.

    Args:
        city: City name or None
        state: State name or None
        country: Country name or None

    Returns:
        Formatted location string
    """
    clean_city = (city or "").strip()
    clean_state = (state or "").strip()
    clean_country = (country or "").strip()

    country_lower = clean_country.lower()
    state_label = clean_state
    if clean_state and country_lower in {"united states", "usa", "us", "united states of america"}:
        state_label = _STATE_ABBR_BY_NAME.get(clean_state, clean_state)

    if clean_city.lower() == "remote" or clean_state.lower() == "remote":
        return "Remote"

    if clean_city and state_label and clean_city != "Unknown" and state_label != "Unknown":
        return f"{clean_city}, {state_label}"
    if clean_city and clean_country and clean_country != "Unknown":
        return f"{clean_city}, {clean_country}"
    if clean_city and clean_city != "Unknown":
        return clean_city
    if state_label and state_label != "Unknown":
        return state_label
    if clean_country and clean_country != "Unknown":
        return clean_country
    return "Unknown"


# Load location dictionary from JSON file
_LOCATION_DICT_PATH = Path(__file__).resolve().parents[3] / "job_board_application" / "convex" / "locationDictionary.json"
try:
    _raw_location_entries = json.loads(_LOCATION_DICT_PATH.read_text(encoding="utf-8"))
except FileNotFoundError:
    _raw_location_entries = []

_LOCATION_ENTRIES: List[Dict[str, Any]] = []
if isinstance(_raw_location_entries, list):
    _LOCATION_ENTRIES = [entry for entry in _raw_location_entries if isinstance(entry, dict)]
elif isinstance(_raw_location_entries, dict):
    for city_key, value in _raw_location_entries.items():
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    if "city" not in entry:
                        entry = {**entry, "city": city_key}
                    _LOCATION_ENTRIES.append(entry)
        elif isinstance(value, dict):
            if "city" not in value:
                value = {**value, "city": city_key}
            _LOCATION_ENTRIES.append(value)

_LOCATION_DICTIONARY: Dict[str, Dict[str, Any]] = {}
_CITY_KEYWORDS: Dict[str, Dict[str, Any]] = {}
_COUNTRY_KEY_TO_LABEL: Dict[str, str] = {}


def _register_location_key(value: str, entry: Dict[str, Any], track_city: bool = False) -> None:
    """Register a location key in the lookup dictionary.

    Args:
        value: The location string to register
        entry: The location entry data
        track_city: Whether to also track as a city keyword
    """
    key = _normalize_location_key(value)
    if not key or key in _LOCATION_DICTIONARY:
        return
    _LOCATION_DICTIONARY[key] = entry
    if track_city and not entry.get("remoteOnly"):
        _CITY_KEYWORDS[key] = entry


# Build location dictionaries from entries
for _entry in _LOCATION_ENTRIES:
    city = (_entry.get("city") or "").strip()
    state = (_entry.get("state") or "").strip() or "Unknown"
    country = (_entry.get("country") or "").strip()
    country = country or None
    remote_only = bool(_entry.get("remoteOnly"))
    state_abbr = _STATE_ABBR_BY_NAME.get(state)
    record = {"city": city, "state": state, "country": country, "remoteOnly": remote_only}
    country_key = _normalize_location_key(country) if country else None
    if country_key and country_key not in _COUNTRY_KEY_TO_LABEL and isinstance(country, str):
        _COUNTRY_KEY_TO_LABEL[country_key] = country
    aliases_raw = _entry.get("aliases")
    aliases_list = aliases_raw if isinstance(aliases_raw, list) else []
    aliases = {
        alias
        for alias in [city, *aliases_list]
        if isinstance(alias, str) and alias.strip()
    }
    for alias in aliases:
        _register_location_key(alias, record, track_city=True)
        _register_location_key(f"{alias}, {state}", record)
        if country:
            _register_location_key(f"{alias}, {country}", record)
        if state_abbr:
            _register_location_key(f"{alias}, {state_abbr}", record)

_LOCATION_DICTIONARY_KEYS: List[tuple[str, Dict[str, Any]]] = sorted(
    _LOCATION_DICTIONARY.items(), key=lambda item: len(item[0]), reverse=True
)
_CITY_KEYWORD_KEYS: List[str] = sorted(_CITY_KEYWORDS.keys(), key=len, reverse=True)


def _normalize_country_label(value: str) -> Optional[str]:
    """Get the canonical country label for a location value.

    Args:
        value: A string that might be a country name

    Returns:
        The canonical country label or None
    """
    key = _normalize_location_key(value)
    if not key:
        return None
    return _COUNTRY_KEY_TO_LABEL.get(key)


def _resolve_location_from_dictionary(value: str, allow_remote: bool = True) -> Optional[Dict[str, Any]]:
    """Resolve a location string to a dictionary entry.

    Args:
        value: The location string to resolve
        allow_remote: Whether to allow remote-only entries

    Returns:
        Location dictionary entry or None
    """
    normalized = _normalize_location_key(value)
    if not normalized:
        return None
    country_label = _normalize_country_label(value)
    if country_label:
        return {"city": None, "state": None, "country": country_label}

    direct = _LOCATION_DICTIONARY.get(normalized)
    if direct and (allow_remote or not direct.get("remoteOnly")):
        return direct

    # Handle "City, State, Country" format by trying progressively shorter prefixes
    # This ensures "Redmond, Washington, United States" matches "Redmond, Washington"
    # instead of matching "Washington" (DC) via boundary pattern
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if len(parts) >= 2:
        # Try "City, State" first (most specific)
        city_state = f"{parts[0]}, {parts[1]}"
        city_state_key = _normalize_location_key(city_state)
        city_state_match = _LOCATION_DICTIONARY.get(city_state_key)
        if city_state_match and (allow_remote or not city_state_match.get("remoteOnly")):
            return city_state_match

        # Try just the city name
        city_key = _normalize_location_key(parts[0])
        city_match = _LOCATION_DICTIONARY.get(city_key)
        if city_match and (allow_remote or not city_match.get("remoteOnly")):
            # Verify it's not a state name that could be confused with a city
            # (e.g., "Washington" should not match if parts[1] is also a state)
            (city_match.get("city") or "").lower()
            state_name = (city_match.get("state") or "").lower()
            # If the city name matches and the state in parts[1] matches, use it
            second_part_key = _normalize_location_key(parts[1])
            if second_part_key == _normalize_location_key(state_name):
                return city_match
            # If parts[1] is a US state abbreviation or name, and city_match is in that state
            state_abbr = _STATE_ABBR_BY_KEY.get(second_part_key)
            if state_abbr and _STATE_NAME_BY_ABBR.get(state_abbr, "").lower() == state_name:
                return city_match

    for key, entry in _LOCATION_DICTIONARY_KEYS:
        if not allow_remote and entry.get("remoteOnly"):
            continue
        if entry.get("remoteOnly"):
            if normalized == key:
                return entry
            continue
        if key and len(key) >= 3 and re.search(
            LOCATION_KEY_BOUNDARY_PATTERN_TEMPLATE.format(key=re.escape(key)),
            normalized,
        ):
            return entry
    return None


def _find_city_in_text(text: str) -> Optional[Dict[str, Any]]:
    """Find a city mention in free-form text.

    Args:
        text: Text that might contain a city name

    Returns:
        City entry from dictionary or None
    """
    normalized_text = _normalize_location_key(text)
    for key in _CITY_KEYWORD_KEYS:
        idx = normalized_text.find(key)
        if idx == -1:
            continue
        before_ok = idx == 0 or normalized_text[idx - 1] == " "
        after_ok = idx + len(key) == len(normalized_text) or normalized_text[idx + len(key)] == " "
        if before_ok and after_ok:
            entry = _CITY_KEYWORDS.get(key)
            if entry:
                return entry
    return None


def _is_plausible_location(value: str) -> bool:
    """Check if a string looks like a plausible location.

    Filters out compensation text, overly long strings, and other non-location data.

    Args:
        value: The string to check

    Returns:
        True if it could be a location
    """
    if not value or len(value) < 2 or len(value) > 100:
        return False
    lowered = value.lower().strip()
    if lowered in ("unknown", "n/a", "na"):
        return False
    if any(token in lowered for token in ("diversity", "equity", "inclusion", "benefits", "culture", "salary", "compensation", "pay", "package", "bonus", "range")):
        return False
    if "$" in value or "401k" in lowered or "401(k" in lowered:
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


def _reorder_by_us_preference(locations: List[str]) -> List[str]:
    """Reorder locations to prefer US locations first.

    Args:
        locations: List of location strings

    Returns:
        Reordered list with US locations prioritized
    """
    prioritized = list(locations)

    def _is_us_location(loc: str) -> tuple[bool, bool]:
        """Check if location is in the US.

        Returns:
            Tuple of (is_us, is_remote)
        """
        resolved = _resolve_location_from_dictionary(loc)
        if resolved:
            country = (resolved.get("country") or "").strip()
            is_remote = (resolved.get("city") or "").lower() == "remote" or (resolved.get("state") or "").lower() == "remote"
            return country == "United States", is_remote

        # Fallback: check if location matches "City, STATE_ABBR" pattern
        # This handles cities not in the dictionary (like Livingston, NJ)
        if "," in loc:
            parts = [p.strip() for p in loc.split(",")]
            if len(parts) == 2:
                state_part = parts[1].upper()
                if state_part in _STATE_NAME_BY_ABBR:
                    return True, False

        return False, False

    def find_index(allow_remote: bool) -> int:
        for idx, loc in enumerate(prioritized):
            is_us, is_remote = _is_us_location(loc)
            if not is_us:
                continue
            if not allow_remote and is_remote:
                continue
            return idx
        return -1

    non_remote_idx = find_index(False)
    if non_remote_idx > 0:
        hit = prioritized.pop(non_remote_idx)
        prioritized.insert(0, hit)
        return prioritized

    remote_idx = find_index(True)
    if remote_idx > 0:
        hit = prioritized.pop(remote_idx)
        prioritized.insert(0, hit)

    return prioritized


def _normalize_locations(locations: List[str]) -> List[str]:
    """Normalize a list of location strings.

    Resolves each location against the dictionary, deduplicates, and reorders
    to prefer US locations.

    Args:
        locations: Raw location strings

    Returns:
        Normalized and deduplicated location list
    """
    seen: set[str] = set()
    normalized: List[str] = []
    for raw in locations:
        if not raw:
            continue
        for part in re.split(LOCATION_SPLIT_PATTERN, raw):
            candidate = _stringify_value(part)
            if not candidate:
                continue
            candidate = re.sub(WHITESPACE_PATTERN, " ", candidate).strip(" ,;/\t")
            if not candidate:
                continue
            if not _is_plausible_location(candidate):
                continue
            resolved = _resolve_location_from_dictionary(candidate)
            if not resolved:
                state_key = _normalize_location_key(candidate)
                state_abbr = _STATE_ABBR_BY_KEY.get(state_key)
                if not state_abbr:
                    state_upper = candidate.strip().upper()
                    if state_upper in _STATE_NAME_BY_ABBR:
                        state_abbr = state_upper
                if state_abbr:
                    state_name = _STATE_NAME_BY_ABBR.get(state_abbr, state_abbr)
                    if state_name not in seen:
                        seen.add(state_name)
                        normalized.append(state_name)
                    continue
                us_city_state = _normalize_us_city_state(candidate)
                if us_city_state and us_city_state not in seen:
                    seen.add(us_city_state)
                    normalized.append(us_city_state)
                    continue
                country_label = _normalize_country_label(candidate)
                if country_label and country_label not in seen:
                    seen.add(country_label)
                    normalized.append(country_label)
                continue
            label = _format_location_label(resolved.get("city"), resolved.get("state"), resolved.get("country"))
            if label and label not in seen:
                seen.add(label)
                normalized.append(label)
    normalized = _reorder_by_us_preference(normalized)
    return normalized


__all__ = [
    # State dictionaries
    "_STATE_NAME_BY_ABBR",
    "_STATE_ABBR_BY_NAME",
    "_STATE_ABBR_BY_KEY",
    # Location dictionaries
    "_LOCATION_DICTIONARY",
    "_LOCATION_DICTIONARY_KEYS",
    "_CITY_KEYWORDS",
    "_CITY_KEYWORD_KEYS",
    "_COUNTRY_KEY_TO_LABEL",
    "_LOCATION_ENTRIES",
    # Functions
    "_normalize_location_key",
    "_normalize_us_city_state",
    "_format_location_label",
    "_register_location_key",
    "_resolve_location_from_dictionary",
    "_find_city_in_text",
    "_normalize_country_label",
    "_is_plausible_location",
    "_reorder_by_us_preference",
    "_normalize_locations",
]
