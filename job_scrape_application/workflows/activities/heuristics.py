"""Job detail heuristic extraction for compensation, location, and metadata."""

from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ...constants import (
    DEFAULT_US_STATE_CODES,
    DEFAULT_US_STATE_NAMES,
    is_remote_company,
    location_matches_usa,
    title_matches_required_keywords,
)
from ..helpers.regex_patterns import (
    COMP_INR_RANGE_PATTERN,
    COMP_K_PATTERN,
    COMP_LPA_PATTERN,
    COMP_USD_RANGE_PATTERN,
    COUNTRY_CODE_PATTERN,
    LOCATION_ANYWHERE_PATTERN,
    LOCATION_CITY_STATE_PATTERN,
    LOCATION_FULL_PATTERN,
    LOCATION_LABEL_PATTERN,
    LOCATION_PAREN_PATTERN,
    LOCATION_SPLIT_PATTERN,
    LOCATION_TOKEN_SPLIT_PATTERN,
    MULTI_SPACE_PATTERN,
    NON_NUMERIC_DOT_PATTERN,
    NON_NUMERIC_PATTERN,
    REQUEST_ID_PATTERN,
    RETIREMENT_PLAN_PATTERN,
)
from ..helpers.scrape_utils import (
    _extract_job_detail_seed_from_json,
    normalize_compensation_value,
    parse_markdown_hints,
    split_description_metadata,
    strip_known_nav_blocks,
)


# Canadian province constants
_CANADIAN_PROVINCE_CODES = {
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT",
}
_CANADIAN_PROVINCE_NAMES = {
    "alberta", "british columbia", "manitoba", "new brunswick",
    "newfoundland and labrador", "nova scotia", "northwest territories",
    "nunavut", "ontario", "prince edward island", "quebec", "saskatchewan", "yukon",
}
_UNKNOWN_LOCATION_TOKENS = {"unknown", "n/a", "na", "unspecified", "not available"}
_US_STATE_NAMES = {name.lower() for name in DEFAULT_US_STATE_NAMES}
_US_STATE_CODES = {code.upper() for code in DEFAULT_US_STATE_CODES}

# Compensation magnitude suffix pattern
COMP_MAGNITUDE_SUFFIX_PATTERN = r"^\s*(?:[kmb]|bn|mm|million|billion|trillion)\b"
COMP_MAGNITUDE_SUFFIX_RE = re.compile(COMP_MAGNITUDE_SUFFIX_PATTERN, flags=re.IGNORECASE)

# Heuristic version for tracking
HEURISTIC_VERSION = 4


def _domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        if host.startswith("www."):
            host = host[4:]
        return host.lower()
    except Exception:
        return ""


def _normalize_locations(raw_locations: Iterable[str]) -> List[str]:
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
            if lowered in ("unknown", "n/a", "na"):
                continue
            if len(candidate) < 3 or len(candidate) > 100:
                continue
            if not _is_plausible_location(candidate):
                continue
            if candidate not in seen:
                seen.add(candidate)
                cleaned.append(candidate)
    return cleaned[:5]


def _is_plausible_location(value: str) -> bool:
    """Check if value looks like a plausible location."""
    lowered = value.lower()
    if any(token in lowered for token in (
        "diversity", "equity", "inclusion", "benefits", "culture",
        "salary", "compensation", "pay", "package", "bonus", "range"
    )):
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


def _derive_location_states(locations: List[str]) -> List[str]:
    """Extract state components from location strings."""
    states: List[str] = []
    for loc in locations:
        parts = [p.strip() for p in str(loc).split(",") if p.strip()]
        if len(parts) >= 2:
            state_val = parts[-2] if len(parts) >= 3 else parts[-1]
            if state_val and state_val not in states:
                states.append(state_val)
    return states


def _derive_countries(locations: List[str]) -> List[str]:
    """Extract country from location strings."""
    countries: List[str] = []
    for loc in locations:
        parts = [p.strip() for p in str(loc).split(",") if p.strip()]
        if not parts:
            continue
        country = parts[-1]
        lowered = country.lower()
        country_upper = country.upper()
        mapped: Optional[str] = None
        if "remote" in lowered:
            mapped = "United States"
        elif lowered in {"locations"}:
            continue
        elif lowered in _UNKNOWN_LOCATION_TOKENS:
            mapped = "United States"
        elif country_upper in _US_STATE_CODES:
            mapped = "United States"
        elif re.match(COUNTRY_CODE_PATTERN, country):
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


def _build_location_search(locations: List[str]) -> str:
    """Build search string from locations."""
    tokens: set[str] = set()
    for loc in locations:
        for token in re.split(LOCATION_TOKEN_SPLIT_PATTERN, loc):
            cleaned = token.strip()
            if cleaned:
                tokens.add(cleaned)
    return " ".join(tokens)


def _looks_like_location_anywhere(value: str) -> bool:
    """Check if location value matches 'anywhere' patterns."""
    return bool(re.search(LOCATION_ANYWHERE_PATTERN, value, re.IGNORECASE))


def _describe_exception(exc: Exception) -> str:
    """Provide a compact string for unexpected errors."""
    parts: List[str] = [f"{type(exc).__name__}: {exc}"]
    resp = getattr(exc, "response", None)
    if resp is not None:
        status = getattr(resp, "status_code", None)
        request_id = None
        try:
            headers = getattr(resp, "headers", {}) or {}
            request_id = headers.get("x-request-id") or headers.get("request-id")
        except Exception:
            request_id = None
        parts.append(f"status={status}")
        if request_id:
            parts.append(f"request_id={request_id}")
    data = getattr(exc, "data", None)
    if data:
        parts.append(f"data={data}")
    return " ".join(str(p) for p in parts if p)


def _extract_request_id(exc: Exception) -> Optional[str]:
    """Best-effort extraction of Convex request id from exception or message."""
    msg = ""
    try:
        msg = str(exc)
    except Exception:
        msg = ""
    match = re.search(REQUEST_ID_PATTERN, msg)
    if match:
        return match.group(1).strip()
    resp = getattr(exc, "response", None)
    if resp is not None:
        try:
            headers = getattr(resp, "headers", {}) or {}
            candidate = headers.get("x-request-id") or headers.get("request-id")
            if candidate:
                return str(candidate)
        except Exception:
            return None
    return None


def _extract_pending_count(value: Any) -> Optional[int]:
    """Pull a numeric pending count from a Convex response or bare number."""
    if isinstance(value, dict):
        for key in ("pending", "remaining", "count", "total"):
            candidate = value.get(key)
            if isinstance(candidate, (int, float)):
                return int(candidate)
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _parse_comp_int(value: Optional[str]) -> Optional[int]:
    """Parse integer compensation value."""
    if not value:
        return None
    cleaned = re.sub(NON_NUMERIC_PATTERN, "", value)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_comp_float(value: Optional[str]) -> Optional[float]:
    """Parse float compensation value."""
    if not value:
        return None
    cleaned = re.sub(NON_NUMERIC_DOT_PATTERN, "", value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _match_has_comp_magnitude_suffix(text: str, match: re.Match[str]) -> bool:
    """Check if match is followed by a magnitude suffix (k, m, etc.)."""
    tail = text[match.end():]
    return bool(COMP_MAGNITUDE_SUFFIX_RE.match(tail))


def _select_compensation_from_bounds(
    low_val: Optional[int],
    high_val: Optional[int],
) -> Optional[int]:
    """Select compensation value from low/high bounds."""
    candidates = [normalize_compensation_value(val) for val in (low_val, high_val)]
    valid = [val for val in candidates if val is not None]
    if not valid:
        return None
    if len(valid) == 2:
        candidate = int((valid[0] + valid[1]) / 2)
    else:
        candidate = valid[0]
    return normalize_compensation_value(candidate)


def _parse_compensation_match(match: re.Match[str]) -> Optional[int]:
    """Parse compensation value from regex match."""
    group_dict = match.groupdict() or {}
    if "low" in group_dict or "high" in group_dict:
        low_val = _parse_comp_int(group_dict.get("low"))
        high_val = _parse_comp_int(group_dict.get("high"))
        return _select_compensation_from_bounds(low_val, high_val)

    raw = match.group(0) or ""
    cleaned = raw.lower()
    comp_val: Optional[int] = None
    if "lpa" in cleaned or "lakh" in cleaned:
        base_val = _parse_comp_float(cleaned)
        if base_val is not None:
            comp_val = int(base_val * 100_000)
    elif cleaned.strip().endswith("k"):
        base_val = _parse_comp_float(cleaned[:-1])
        if base_val is not None:
            comp_val = int(base_val * 1000)
    else:
        base_val = _parse_comp_int(cleaned)
        if base_val is not None:
            comp_val = int(base_val)
    if comp_val is None:
        return None
    return normalize_compensation_value(comp_val)


def _extract_compensation_from_text(
    text: str,
    regexes: List[str],
) -> Tuple[Optional[int], Optional[str]]:
    """Extract compensation value from text using regex patterns."""
    for pattern in regexes:
        try:
            matches = re.finditer(pattern, text, flags=re.MULTILINE | re.IGNORECASE)
        except re.error:
            continue
        for match in matches:
            if _match_has_comp_magnitude_suffix(text, match):
                continue
            comp_val = _parse_compensation_match(match)
            if comp_val is None:
                continue
            return comp_val, pattern
    return None, None


def _first_match(text: str, regexes: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """Find first regex match in text, returning (pattern, matched_value)."""
    for pattern in regexes:
        try:
            match = re.search(pattern, text, flags=re.MULTILINE | re.IGNORECASE)
        except re.error:
            continue
        if match:
            group_dict = match.groupdict() if match.groupdict() else {}
            if "location" in group_dict:
                return pattern, group_dict.get("location")
            if "value" in group_dict:
                return pattern, group_dict.get("value")
            return pattern, match.group(0)
    return None, None


def _build_ordered_regexes(
    configs: List[Dict[str, Any]],
    field: str,
    defaults: List[str],
) -> List[str]:
    """Build ordered list of regex patterns from configs and defaults."""
    regexes: List[str] = []
    seen: set[str] = set()
    for config in configs:
        if not isinstance(config, dict):
            continue
        patterns = config.get(field) or config.get(f"{field}Regexes") or []
        if isinstance(patterns, str):
            patterns = [patterns]
        if not isinstance(patterns, list):
            continue
        for pattern in patterns:
            if not isinstance(pattern, str) or not pattern.strip():
                continue
            pattern = pattern.strip()
            if pattern in seen:
                continue
            seen.add(pattern)
            regexes.append(pattern)
    for default in defaults:
        if default not in seen:
            seen.add(default)
            regexes.append(default)
    return regexes


def _detect_currency_code(text: str) -> Optional[str]:
    """Detect currency code from text patterns."""
    from ..helpers.regex_patterns import (
        CAD_CURRENCY_PATTERNS,
        GBP_CURRENCY_PATTERNS,
        INR_CURRENCY_PATTERNS,
        EUR_CURRENCY_PATTERNS,
        AUD_CURRENCY_PATTERNS,
    )

    currency_patterns = [
        (INR_CURRENCY_PATTERNS, "INR"),
        (GBP_CURRENCY_PATTERNS, "GBP"),
        (EUR_CURRENCY_PATTERNS, "EUR"),
        (CAD_CURRENCY_PATTERNS, "CAD"),
        (AUD_CURRENCY_PATTERNS, "AUD"),
    ]
    for patterns, code in currency_patterns:
        for pattern in patterns:
            try:
                if re.search(pattern, text, re.IGNORECASE):
                    return code
            except re.error:
                continue
    return None


def _build_job_detail_heuristic_patch(
    row: Dict[str, Any],
    configs: List[Dict[str, Any]],
    now_ms: int,
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """Return heuristic patch + records for a job row without mutating Convex."""
    source_description = row.get("description") or ""
    raw_description = source_description
    seed_description, seed_hints = _extract_job_detail_seed_from_json(raw_description)
    if seed_description:
        raw_description = seed_description
    cleaned_description = strip_known_nav_blocks(raw_description)
    analysis_description = cleaned_description
    description_body, description_metadata = split_description_metadata(cleaned_description)
    url = row.get("url") or ""
    domain = _domain_from_url(url)
    attempts = int(row.get("heuristicAttempts") or 0)
    recorded_location = False
    recorded_comp = False
    records: List[Dict[str, str]] = []

    location_defaults = [
        LOCATION_FULL_PATTERN,
        LOCATION_LABEL_PATTERN,
        LOCATION_CITY_STATE_PATTERN,
        LOCATION_PAREN_PATTERN,
    ]
    comp_defaults = [
        COMP_USD_RANGE_PATTERN,
        COMP_INR_RANGE_PATTERN,
        COMP_K_PATTERN,
        COMP_LPA_PATTERN,
    ]

    location_regexes = _build_ordered_regexes(configs, "location", location_defaults)
    comp_regexes = _build_ordered_regexes(configs, "compensation", comp_defaults)

    hints = parse_markdown_hints(analysis_description)
    if isinstance(seed_hints, dict) and seed_hints:
        if not hints.get("title") and seed_hints.get("title"):
            hints["title"] = seed_hints.get("title")
        if not hints.get("company") and seed_hints.get("company"):
            hints["company"] = seed_hints.get("company")
        if not hints.get("locations") and seed_hints.get("locations"):
            hints["locations"] = seed_hints.get("locations")
        if not hints.get("location") and seed_hints.get("location"):
            hints["location"] = seed_hints.get("location")
        if "remote" not in hints and seed_hints.get("remote") is not None:
            hints["remote"] = seed_hints.get("remote")
        seed_locations = seed_hints.get("locations")
        seed_location = seed_hints.get("location")
        if seed_locations and isinstance(seed_locations, list) and seed_location:
            hint_locations = hints.get("locations") or []
            if not isinstance(hint_locations, list):
                hint_locations = []
            overlaps = False
            for loc in hint_locations:
                if not isinstance(loc, str):
                    continue
                loc_lower = loc.lower()
                if any(loc_lower in seed.lower() or seed.lower() in loc_lower for seed in seed_locations):
                    overlaps = True
                    break
            if not overlaps and seed_description:
                hints["locations"] = seed_locations
                hints["location"] = seed_location

    hinted_title = hints.get("title") if isinstance(hints, dict) else None
    raw_title_value = row.get("title") or row.get("jobTitle") or row.get("job_title") or ""
    raw_title = str(raw_title_value).strip()
    hinted_comp = hints.get("compensation")
    comp_range_hint = hints.get("compensation_range") or {}
    locations_hint = hints.get("locations") or []
    raw_company = row.get("company")
    company_name = raw_company if isinstance(raw_company, str) else str(raw_company or "")
    company_remote = is_remote_company(company_name)
    raw_location_value = (row.get("location") or "").strip()
    raw_location_lower = raw_location_value.lower()
    seed_location = None
    seed_locations: List[str] = []
    if isinstance(seed_hints, dict):
        seed_location = seed_hints.get("location")
        raw_seed_locations = seed_hints.get("locations")
        if isinstance(raw_seed_locations, list):
            seed_locations = [loc for loc in raw_seed_locations if isinstance(loc, str) and loc.strip()]
    location_fallback = (
        hints.get("location")
        if (not raw_location_value or raw_location_lower in _UNKNOWN_LOCATION_TOKENS)
        else raw_location_value or hints.get("location")
    )
    if seed_description and seed_location and isinstance(seed_location, str) and seed_location.strip():
        if not raw_location_value or raw_location_lower in _UNKNOWN_LOCATION_TOKENS:
            location_fallback = seed_location
        elif seed_locations:
            raw_norm = raw_location_lower.strip()
            if not any(
                raw_norm in loc.lower() or loc.lower() in raw_norm for loc in seed_locations
            ):
                location_fallback = seed_location
    is_remote = company_remote or hints.get("remote") is True or bool(row.get("remote"))
    if hints.get("remote") is False and not company_remote:
        is_remote = False
    if "remote" in raw_location_lower:
        is_remote = True
    location_unknown = raw_location_lower in _UNKNOWN_LOCATION_TOKENS or not raw_location_value
    locations = _normalize_locations(locations_hint or ([location_fallback] if location_fallback else []))
    comp_reason = row.get("compensationReason")
    raw_total_comp = row.get("totalCompensation")
    total_comp = normalize_compensation_value(raw_total_comp) or 0
    raw_comp_unknown = row.get("compensationUnknown")
    compensation_unknown = bool(raw_comp_unknown) if raw_comp_unknown is not None else None
    currency_code = row.get("currencyCode")
    currency_hint = _detect_currency_code(analysis_description)
    if currency_hint and currency_hint != currency_code:
        currency_code = currency_hint
    if raw_total_comp and not total_comp:
        compensation_unknown = True
    if (not total_comp or total_comp <= 0) and isinstance(hinted_comp, (int, float)):
        normalized_hint = normalize_compensation_value(hinted_comp)
        if normalized_hint is not None:
            total_comp = normalized_hint
            compensation_unknown = False
            comp_reason = "parsed from description"
    elif (not total_comp or total_comp <= 0) and isinstance(comp_range_hint, dict):
        low_hint = normalize_compensation_value(comp_range_hint.get("low"))
        high_hint = normalize_compensation_value(comp_range_hint.get("high"))
        range_values = [v for v in (low_hint, high_hint) if v is not None]
        if range_values:
            candidate = int(sum(range_values) / len(range_values))
            candidate = normalize_compensation_value(candidate)
            if candidate is not None:
                total_comp = candidate
                compensation_unknown = False
                comp_reason = "parsed from description"
    elif total_comp and total_comp > 0 and compensation_unknown is None:
        compensation_unknown = False

    matched_locations: List[str] = []
    if analysis_description:
        used_pattern, found = _first_match(analysis_description, location_regexes)
        if found and (location_matches_usa(found) or _looks_like_location_anywhere(found)):
            found_locations = _normalize_locations([found])
            if found_locations:
                matched_locations = found_locations
                if used_pattern:
                    records.append(
                        {"domain": domain or "default", "field": "location", "regex": used_pattern}
                    )
                    recorded_location = True
    if matched_locations and not locations:
        locations = matched_locations
    if (not locations) and currency_hint and currency_hint != "USD":
        if currency_hint == "INR":
            locations = ["India"]
        elif currency_hint == "GBP":
            locations = ["United Kingdom"]
        elif currency_hint == "EUR":
            locations = ["Europe"]
    if not locations and is_remote:
        locations = ["Remote"]
    if locations:
        seen_cities: set[str] = set()
        deduped_locations: List[str] = []
        for loc in locations:
            city_part = loc.split(",")[0].strip().lower()
            if city_part in seen_cities:
                continue
            seen_cities.add(city_part)
            deduped_locations.append(loc)
        locations = deduped_locations

    countries = _derive_countries(locations)
    if not countries and (is_remote or location_unknown):
        countries = ["United States"]

    if (not total_comp or total_comp <= 0) and analysis_description:
        comp_description = re.sub(RETIREMENT_PLAN_PATTERN, "", analysis_description, flags=re.IGNORECASE)
        comp_val, used_pattern = _extract_compensation_from_text(comp_description, comp_regexes)
        if comp_val is not None:
            total_comp = comp_val
            compensation_unknown = False
            comp_reason = "parsed with heuristic"
            if currency_hint and currency_hint != "USD":
                currency_code = currency_hint
            if used_pattern:
                records.append(
                    {"domain": domain or "default", "field": "compensation", "regex": used_pattern}
                )
                recorded_comp = True

    if locations and not recorded_location:
        records.append({"domain": domain or "default", "field": "location", "regex": "hint:location"})
        recorded_location = True
    if total_comp and total_comp > 0 and not recorded_comp:
        records.append(
            {"domain": domain or "default", "field": "compensation", "regex": "hint:compensation"}
        )
        recorded_comp = True

    def _should_override_title(value: str) -> bool:
        if not value:
            return True
        lowered = value.strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
        if normalized in {
            "the role", "our team", "the team", "role", "job description",
            "description", "description and requirements", "description requirements",
        }:
            return True
        if lowered in {"unknown", "n/a", "na", "untitled"}:
            return True
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
        if lowered.endswith((".", "!", "?")):
            return True
        if len(lowered.split()) > 14:
            return True
        if not title_matches_required_keywords(value):
            return True
        return False

    title_patch: Optional[str] = None
    if isinstance(hinted_title, str) and hinted_title.strip():
        cleaned_hint = html_lib.unescape(hinted_title).strip()
        if cleaned_hint and cleaned_hint.lower() != raw_title.lower():
            if _should_override_title(raw_title):
                title_patch = cleaned_hint

    patch: Dict[str, Any] = {
        "heuristicAttempts": attempts + 1,
        "heuristicLastTried": now_ms,
        "heuristicVersion": HEURISTIC_VERSION,
    }
    if title_patch:
        patch["title"] = title_patch
        patch["jobTitle"] = title_patch
    if locations:
        patch["locations"] = locations
        patch["location"] = locations[0]
        patch["locationStates"] = _derive_location_states(locations)
        patch["locationSearch"] = _build_location_search(locations)
    if countries:
        patch["countries"] = countries
        patch["country"] = countries[0]
    if total_comp and total_comp > 0:
        patch["totalCompensation"] = int(total_comp)
    if comp_reason:
        patch["compensationReason"] = comp_reason
    if compensation_unknown is not None:
        patch["compensationUnknown"] = compensation_unknown
    if currency_code:
        patch["currencyCode"] = currency_code
    remote_hint = hints.get("remote")
    if company_remote:
        remote_hint = True
    if remote_hint is True and row.get("remote") is not True:
        patch["remote"] = True
    elif remote_hint is False and row.get("remote") is not False:
        patch["remote"] = False
    if description_metadata and description_metadata != row.get("metadata"):
        patch["metadata"] = description_metadata
    normalized_description = ""
    if description_body.strip():
        normalized_description = description_body
    elif cleaned_description.strip():
        normalized_description = cleaned_description
    if normalized_description:
        patch["description"] = normalized_description

    return patch, records
