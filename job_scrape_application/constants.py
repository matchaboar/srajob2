from __future__ import annotations

import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Tuple

import yaml

from .config import resolve_config_path

FILTERS_YAML_PATH = Path(__file__).resolve().parent / "scraper_filters.yaml"

REMOTE_COMPANIES_YAML_PATH = resolve_config_path("remote_companies.yaml")

_COMPANY_SUFFIXES = {
    "inc",
    "incorporated",
    "llc",
    "ltd",
    "limited",
    "corp",
    "corporation",
    "company",
    "co",
    "plc",
    "gmbh",
    "sarl",
    "ag",
    "bv",
    "sa",
    "pte",
    "pty",
    "holdings",
    "group",
}
_COMPANY_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")

ZIP_CODE_PATTERN = r"\b\d{5}(?:-\d{4})?\b"
US_ABBREVIATION_PATTERN = r"\bU\.?S\.?A?\b"
US_STATE_CODE_PATTERN_TEMPLATE = r"\b{code}\b"

# Simple US ZIP code heuristic to catch addresses in the listing.
ZIP_CODE_RE = re.compile(ZIP_CODE_PATTERN)

DEFAULT_REQUIRED_KEYWORDS: Tuple[str, ...] = ("engineer", "developer", "software", "development")
DEFAULT_ALLOW_UNKNOWN_TITLE = True
DEFAULT_REQUIRE_US = True
DEFAULT_ALLOW_UNKNOWN_LOCATION = True

DEFAULT_US_TERMS: Tuple[str, ...] = (
    "united states",
    "united states of america",
    "usa",
    "u.s.",
    "u.s.a",
    "u.s",
    "america",
    "within the us",
    "anywhere in the us",
    "remote in us",
    "remote - us",
    "us remote",
    "us-based",
    "us only",
)
DEFAULT_US_STATE_CODES: Tuple[str, ...] = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "PR",
    "GU",
    "VI",
)
DEFAULT_US_STATE_NAMES: Tuple[str, ...] = (
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "district of columbia",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "puerto rico",
    "guam",
    "virgin islands",
)
DEFAULT_US_CITY_HINTS: Tuple[str, ...] = (
    "new york",
    "san francisco",
    "seattle",
    "austin",
    "boston",
    "los angeles",
    "denver",
    "atlanta",
    "chicago",
    "portland",
    "san diego",
    "dallas",
    "houston",
    "miami",
    "phoenix",
    "raleigh",
    "washington",
    "san jose",
    "philadelphia",
    "salt lake city",
    "columbus",
    "charlotte",
)
DEFAULT_NON_US_TERMS: Tuple[str, ...] = (
    "canada",
    "toronto",
    "ontario",
    "vancouver",
    "montreal",
    "london",
    "united kingdom",
    "uk",
    "ireland",
    "scotland",
    "wales",
    "australia",
    "new zealand",
    "singapore",
    "india",
    "pakistan",
    "bangladesh",
    "germany",
    "france",
    "spain",
    "italy",
    "netherlands",
    "sweden",
    "norway",
    "finland",
    "denmark",
    "poland",
    "mexico",
    "brazil",
    "argentina",
    "chile",
    "colombia",
    "peru",
    "japan",
    "china",
    "taiwan",
    "hong kong",
    "south korea",
    "korea",
    "vietnam",
    "thailand",
    "philippines",
    "malaysia",
    "indonesia",
    "south africa",
    "nigeria",
    "egypt",
    "israel",
)


@dataclass(frozen=True)
class FilterSettings:
    required_keywords: Tuple[str, ...]
    allow_unknown_title: bool
    require_us_only: bool
    allow_unknown_location: bool
    us_terms: Tuple[str, ...]
    us_state_codes: Tuple[str, ...]
    us_state_names: Tuple[str, ...]
    us_city_hints: Tuple[str, ...]
    non_us_terms: Tuple[str, ...]


def _parse_keywords(raw: str | None) -> Tuple[str, ...]:
    if not raw:
        return ()
    return tuple(
        keyword.strip().lower()
        for keyword in raw.split(",")
        if keyword and keyword.strip()
    )


def _dedupe_preserve_order(values: Iterable[str]) -> Tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _normalize_list(raw: Any, *, lower: bool = False, upper: bool = False) -> Tuple[str, ...]:
    if not raw:
        return ()
    if isinstance(raw, str):
        items = [raw]
    elif isinstance(raw, Iterable):
        items = list(raw)
    else:
        return ()

    normalized: list[str] = []
    for item in items:
        if not isinstance(item, str):
            continue
        trimmed = item.strip()
        if not trimmed:
            continue
        if lower:
            trimmed = trimmed.lower()
        if upper:
            trimmed = trimmed.upper()
        normalized.append(trimmed)
    return _dedupe_preserve_order(normalized)


def _normalize_company_name(value: str | None) -> str:
    if not value:
        return ""
    cleaned = _COMPANY_NORMALIZE_RE.sub(" ", value.lower()).strip()
    if not cleaned:
        return ""
    tokens = cleaned.split()
    while tokens and tokens[-1] in _COMPANY_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def _merge_list(defaults: Tuple[str, ...], extra: Any, *, lower: bool = False, upper: bool = False) -> Tuple[str, ...]:
    merged = list(defaults)
    merged.extend(_normalize_list(extra, lower=lower, upper=upper))
    return _dedupe_preserve_order(merged)


def _load_yaml_filters() -> dict[str, Any]:
    if not FILTERS_YAML_PATH.exists():
        return {}
    try:
        raw = FILTERS_YAML_PATH.read_text()
    except Exception:
        return {}

    try:
        loaded = yaml.safe_load(raw) or {}
    except Exception:
        return {}

    return loaded if isinstance(loaded, dict) else {}


def _load_remote_companies_yaml() -> Tuple[str, ...]:
    if not REMOTE_COMPANIES_YAML_PATH.exists():
        return ()
    try:
        raw = REMOTE_COMPANIES_YAML_PATH.read_text()
    except Exception:
        return ()

    try:
        loaded = yaml.safe_load(raw)
    except Exception:
        return ()

    if isinstance(loaded, dict):
        values = loaded.get("companies") or []
    elif isinstance(loaded, list):
        values = loaded
    else:
        values = []

    normalized: list[str] = []
    for item in values:
        if not isinstance(item, str):
            continue
        normalized_name = _normalize_company_name(item)
        if normalized_name:
            normalized.append(normalized_name)

    return _dedupe_preserve_order(normalized)


@lru_cache(maxsize=1)
def get_remote_companies() -> Tuple[str, ...]:
    return _load_remote_companies_yaml()


def is_remote_company(company: str | None) -> bool:
    normalized = _normalize_company_name(company)
    if not normalized:
        return False
    return normalized in get_remote_companies()


@lru_cache(maxsize=1)
def get_filter_settings() -> FilterSettings:
    data = _load_yaml_filters()

    title_section = data.get("title_keywords") or {}
    env_keywords = os.getenv("JOB_TITLE_REQUIRED_KEYWORDS") or os.getenv("JOB_TITLE_KEYWORDS")
    required_keywords = (
        _parse_keywords(env_keywords)
        if env_keywords
        else _normalize_list(title_section.get("required"), lower=True) or DEFAULT_REQUIRED_KEYWORDS
    )
    allow_unknown_title = bool(title_section.get("allow_when_missing", DEFAULT_ALLOW_UNKNOWN_TITLE))

    location_section = data.get("location_filters") or {}
    require_us_only = bool(location_section.get("require_usa", DEFAULT_REQUIRE_US))
    allow_unknown_location = bool(location_section.get("allow_when_missing", DEFAULT_ALLOW_UNKNOWN_LOCATION))
    us_terms = _merge_list(DEFAULT_US_TERMS, location_section.get("us_terms"), lower=True)
    us_state_codes = _merge_list(DEFAULT_US_STATE_CODES, location_section.get("us_state_codes"), upper=True)
    us_state_names = _merge_list(DEFAULT_US_STATE_NAMES, location_section.get("us_state_names"), lower=True)
    us_city_hints = _merge_list(DEFAULT_US_CITY_HINTS, location_section.get("us_city_hints"), lower=True)
    non_us_terms = _merge_list(DEFAULT_NON_US_TERMS, location_section.get("non_us_terms"), lower=True)

    return FilterSettings(
        required_keywords=required_keywords,
        allow_unknown_title=allow_unknown_title,
        require_us_only=require_us_only,
        allow_unknown_location=allow_unknown_location,
        us_terms=us_terms,
        us_state_codes=us_state_codes,
        us_state_names=us_state_names,
        us_city_hints=us_city_hints,
        non_us_terms=non_us_terms,
    )


REQUIRED_JOB_TITLE_KEYWORDS: Tuple[str, ...] = get_filter_settings().required_keywords


def title_matches_required_keywords(
    title: str | None, keywords: Iterable[str] | None = None
) -> bool:
    """
    Return True when the provided title matches the configured required keywords.

    - If no keywords are configured, allow all titles.
    - If the title is unknown/empty, allow it (we'll still scrape in that case).
    - Otherwise, require that at least one keyword appears case-insensitively as a substring.
    """

    settings = get_filter_settings()
    required = tuple(k.lower() for k in keywords) if keywords is not None else settings.required_keywords
    if not required:
        return True

    if not title:
        return settings.allow_unknown_title

    normalized_title = title.lower()
    return any(keyword in normalized_title for keyword in required)


def location_matches_usa(location: str | None, settings: FilterSettings | None = None) -> bool:
    """
    Return True when the job location is in the US or when we intentionally allow
    unknown/blank locations (per admin UI requirements).
    """

    cfg = settings or get_filter_settings()
    if not cfg.require_us_only:
        return True

    if location is None or not str(location).strip():
        return cfg.allow_unknown_location

    normalized = str(location).strip()
    lower = normalized.lower()
    upper = normalized.upper()

    if any(term in lower for term in cfg.non_us_terms):
        return False

    if "remote" in lower and not any(term in lower for term in cfg.us_terms):
        return cfg.allow_unknown_location

    if any(term in lower for term in cfg.us_terms):
        return True

    if re.search(US_ABBREVIATION_PATTERN, upper):
        return True

    if ZIP_CODE_RE.search(lower):
        return True

    if any(re.search(US_STATE_CODE_PATTERN_TEMPLATE.format(code=code), upper) for code in cfg.us_state_codes):
        return True

    if any(name in lower for name in cfg.us_state_names):
        return True

    if any(city in lower for city in cfg.us_city_hints):
        return True

    return False


def job_passes_filters(title: str | None, location: str | None, *, keywords: Iterable[str] | None = None) -> bool:
    """Helper to check both title keyword and US location filters together."""

    if not title_matches_required_keywords(title, keywords):
        return False
    return location_matches_usa(location)
