"""
Location extraction strategies and extractor.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

from .base import (
    ExtractionStrategy,
    FieldExtractor,
    StrategyPriority,
    StrategyResult,
)

if TYPE_CHECKING:
    from .context import ExtractionContext


# Location patterns
_LOCATION_LABEL_RE = re.compile(
    r"location[:\-\s]+(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})",
    re.IGNORECASE,
)
_LOCATION_CITY_STATE_RE = re.compile(
    r"(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})",
)
_LOCATION_FULL_RE = re.compile(
    r"(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z][A-Za-z .'-]{3,})",
)
_LOCATION_PAREN_RE = re.compile(
    r"\((?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\)",
)
_LOCATION_LINE_RE = re.compile(
    r"^\s*location\b\s*[:\-–]?\s*(?P<location>.+)$",
    re.IGNORECASE | re.MULTILINE,
)
_SIMPLE_LOCATION_LINE_RE = re.compile(
    r"^[ \t]*(?P<location>[A-Z][\w .'-]+,\s*[A-Z][\w .'-]+)\s*$",
    re.MULTILINE,
)

# Valid US state codes
_VALID_US_STATE_CODES = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
})

# Valid Canadian province codes
_VALID_CA_PROVINCE_CODES = frozenset({
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT",
})

# Common country codes that might appear in location
_VALID_COUNTRY_CODES = frozenset({
    "UK", "US", "JP", "DE", "FR", "IT", "ES", "AU", "NZ", "IN", "CN", "SG", "HK",
    "BR", "MX", "AR", "CL", "CO", "NL", "BE", "CH", "AT", "SE", "NO", "DK", "FI",
    "IE", "PL", "CZ", "HU", "RO", "BG", "GR", "PT", "IL", "AE", "SA", "ZA", "KR",
    "TW", "TH", "MY", "PH", "ID", "VN",
})

_VALID_REGION_CODES = _VALID_US_STATE_CODES | _VALID_CA_PROVINCE_CODES | _VALID_COUNTRY_CODES

# Words that indicate a string is NOT a location
_NON_LOCATION_WORDS = frozenset(
    {
        # Job titles
        "engineer",
        "manager",
        "developer",
        "analyst",
        "designer",
        "specialist",
        "director",
        "lead",
        "senior",
        "junior",
        "staff",
        "principal",
        "intern",
        "coordinator",
        "associate",
        "consultant",
        "architect",
        "scientist",
        "executive",
        "account",
        "representative",
        "administrator",
        "recruiter",
        "strategist",
        "planner",
        "officer",
        # Programming languages/tech terms that look like locations
        "kotlin",
        "java",
        "python",
        "swift",
        "ruby",
        "scala",
        "golang",
        "rust",
        "javascript",
        "typescript",
        "espresso",
        "android",
        "flutter",
        "react",
        "angular",
        "vue",
        "node",
        # Job requirement words
        "experience",
        "knowledge",
        "demonstrable",
        "proficiency",
        "required",
        "preferred",
        "skills",
        "ability",
        "understanding",
        "familiarity",
        "expertise",
        # Other non-location terms
        "team",
        "company",
        "role",
        "position",
        "opportunity",
    }
)


def _is_valid_location(value: str | None, *, allow_country_only: bool = False) -> tuple[bool, str]:
    """
    Validate a location value.

    Args:
        value: The location string to validate
        allow_country_only: If True, accept country-only values like "United States".
                           Use this for explicit field sources (raw_row, structured_data).
    """
    if not value:
        return False, "Empty location"

    value = value.strip()
    if len(value) < 2:
        return False, f"Location too short: {len(value)} chars"

    if len(value) > 200:
        return False, f"Location too long: {len(value)} chars"

    # Check for job title words (not a location)
    # Use word boundary matching to avoid false positives like "Bellevue" containing "vue"
    lower = value.lower()
    # Tokenize by non-alphanumeric chars
    tokens = set(re.split(r"[^a-z0-9]+", lower))
    for word in _NON_LOCATION_WORDS:
        if word in tokens:
            return False, f"Contains job title word '{word}'"

    # Check for "Unknown" or similar
    if lower in {"unknown", "n/a", "na", "none", "tbd", "to be determined"}:
        return False, f"Placeholder location: {value}"

    # Check country-only values (too generic for inferred locations, but OK for explicit sources)
    country_only_values = {
        "united states", "united states of america", "usa", "us",
        "canada", "united kingdom", "uk", "germany", "france", "japan",
        "india", "australia", "brazil", "mexico", "china", "singapore",
        "netherlands", "spain", "italy", "ireland", "israel", "poland",
        "south korea", "taiwan", "hong kong", "sweden", "switzerland",
        "norway", "denmark", "finland", "belgium", "austria", "portugal",
        "czech republic", "hungary", "romania", "greece", "new zealand",
        "south africa", "argentina", "chile", "colombia", "peru",
        "thailand", "malaysia", "philippines", "indonesia", "vietnam",
        "egypt", "turkey", "saudi arabia", "uae", "united arab emirates",
        "global", "worldwide", "international", "multiple locations",
        "various locations", "multiple", "various",
    }
    if lower in country_only_values:
        if allow_country_only:
            return True, f"Country-only location accepted (explicit source): {value}"
        return False, f"Country-only location too generic: {value}"

    # Validate "City, XX" patterns - the XX should be a valid region code
    city_state_match = re.match(r"^([^,]+),\s*([A-Z]{2})$", value)
    if city_state_match:
        city, code = city_state_match.groups()
        city = city.strip()
        if code not in _VALID_REGION_CODES:
            return False, f"Invalid region code: {code}"
        # City name should be at least 3 chars (rejects "BS, MS" pattern)
        if len(city) < 3:
            return False, f"City name too short: {city}"
        # Reject common degree abbreviations that look like city names
        degree_abbrevs = {"ba", "bs", "ma", "ms", "phd", "mba", "md", "jd", "llm", "edd", "bsc", "msc"}
        if city.lower() in degree_abbrevs:
            return False, f"Looks like degree abbreviation, not city: {city}"

    return True, "Valid location"


class StructuredDataLocationStrategy(ExtractionStrategy[str]):
    """Extract location from Schema.org JobPosting or API JSON."""

    name = "structured_data_location"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        # Try common location keys
        for key in (
            "location",
            "jobLocation",
            "job_location",
            "city",
            "office",
            "offices",
            "workLocation",
        ):
            value = data.get(key)
            if value is None:
                continue

            # Handle dict with name field
            if isinstance(value, dict):
                value = value.get("name") or value.get("location") or value.get("address")

            # Handle list of locations
            if isinstance(value, list) and value:
                # Join first few locations
                locations = []
                for loc in value[:5]:
                    if isinstance(loc, dict):
                        loc = loc.get("name") or loc.get("location")
                    if isinstance(loc, str) and loc.strip():
                        locations.append(loc.strip())
                if locations:
                    value = "; ".join(locations)

            if isinstance(value, str) and value.strip():
                cleaned = value.strip()
                # Allow country-only for explicit structured data
                is_valid, reason = _is_valid_location(cleaned, allow_country_only=True)
                return self._make_result(
                    cleaned if is_valid else None,
                    reason,
                    is_valid=is_valid,
                    confidence=0.95,
                    debug_info={"key": key, "raw_value": data.get(key)},
                )

        return self._make_skip_result("No location key found in structured data")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value, allow_country_only=True)


class SiteHandlerLocationHintStrategy(ExtractionStrategy[str]):
    """Extract location from site handler's extract_location_hint() method."""

    name = "site_handler_location_hint"
    priority = StrategyPriority.SITE_HANDLER

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        handler = context.handler
        if not handler:
            return self._make_skip_result("No handler available")

        # Check if handler has extract_location_hint method
        if not hasattr(handler, "extract_location_hint"):
            return self._make_skip_result("Handler has no extract_location_hint method")

        try:
            content = context.normalized_markdown or context.raw_markdown
            location_hint = handler.extract_location_hint(content)
        except Exception as e:
            return self._make_skip_result(f"Handler error: {e}")

        if not location_hint:
            return self._make_skip_result(
                f"Handler '{context.handler_name}' returned no location hint"
            )

        is_valid, reason = _is_valid_location(location_hint)
        return self._make_result(
            location_hint if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.90,
            debug_info={"handler": context.handler_name},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value)


class RawRowLocationStrategy(ExtractionStrategy[str]):
    """Extract location from raw row data (location field)."""

    name = "raw_row_location"
    priority = StrategyPriority.EXPLICIT_FIELD

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        raw_location = context.get_raw_field(
            "location", "city", "region", "office", "jobLocation"
        )
        if not raw_location:
            return self._make_skip_result("No location field in raw row")

        # Handle dict with name field
        if isinstance(raw_location, dict):
            raw_location = (
                raw_location.get("name")
                or raw_location.get("location")
                or raw_location.get("address")
            )

        if not isinstance(raw_location, str):
            raw_location = str(raw_location) if raw_location else None

        if not raw_location:
            return self._make_skip_result("Location field is empty or invalid")

        cleaned = raw_location.strip()
        # Don't accept country-only from raw row - let more specific patterns win
        is_valid, reason = _is_valid_location(cleaned)
        return self._make_result(
            cleaned if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.85,
            debug_info={"raw_value": raw_location},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value)


_URL_PATH_WORDS_TO_SKIP = frozenset({
    # Job-related path segments
    "job", "jobs", "position", "positions", "opening", "openings",
    "career", "careers", "vacancy", "vacancies", "posting", "postings",
    "opportunity", "opportunities", "role", "roles",
    # Common URL structure words
    "detail", "details", "view", "apply", "search", "list",
    "page", "results", "index", "show", "display",
    # Department/category words
    "engineering", "design", "product", "sales", "marketing",
    "finance", "operations", "hr", "legal", "support",
})


class URLLocationStrategy(ExtractionStrategy[str]):
    """Extract location from job URL path (e.g., /job/san-francisco/...)."""

    name = "url_location"
    priority = StrategyPriority.URL_DERIVED

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        url = context.url
        if not url:
            return self._make_skip_result("No URL available")

        try:
            parsed = urlparse(url)
            path = unquote(parsed.path or "").lower()
        except Exception as e:
            return self._make_skip_result(f"URL parse error: {e}")

        # Common path patterns for location:
        # /job/{location}/... or /jobs/{location}/...
        segments = [s for s in path.split("/") if s]
        location_segment = None

        for i, seg in enumerate(segments):
            if seg in ("job", "jobs", "position", "positions", "opening", "openings"):
                # Check if next segment looks like a location
                if i + 1 < len(segments):
                    candidate = segments[i + 1]
                    # Skip if it looks like a job ID (numeric or too long)
                    if candidate.isdigit() or len(candidate) > 40:
                        continue
                    # Skip if it looks like a UUID (e.g., 9f38c542-fe09-4fb1-bae2-09e3b789119b)
                    if re.match(r"^[0-9a-f]{8}[- ]?[0-9a-f]{4}[- ]?[0-9a-f]{4}[- ]?[0-9a-f]{4}[- ]?[0-9a-f]{12}$", candidate):
                        continue
                    # Skip if it's mostly hex chars (likely an ID)
                    if re.match(r"^[0-9a-f-]{20,}$", candidate):
                        continue
                    # Skip language codes
                    if candidate in ("en", "de", "fr", "es", "it", "pt", "nl", "ja", "zh"):
                        continue
                    # Skip common URL path words that aren't locations
                    if candidate in _URL_PATH_WORDS_TO_SKIP:
                        continue
                    # Convert slug to title case
                    location_segment = candidate.replace("-", " ").title()
                    break

        if not location_segment:
            return self._make_skip_result("No location pattern found in URL path")

        # Validate
        if len(location_segment) < 3 or len(location_segment) > 50:
            return self._make_skip_result(f"Location segment invalid length: {location_segment}")

        # Skip if it looks like a job title
        lower = location_segment.lower()
        for word in _NON_LOCATION_WORDS:
            if word in lower:
                return self._make_skip_result(f"Location segment looks like job title: {location_segment}")

        return self._make_result(
            location_segment,
            "Extracted from URL path",
            is_valid=True,
            confidence=0.80,
            debug_info={"url": url, "segment": location_segment},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value)


class ExplicitLabelLocationStrategy(ExtractionStrategy[str]):
    """Extract location from explicit labels like 'Location: San Francisco, CA'."""

    name = "explicit_label_location"
    priority = StrategyPriority.EXPLICIT_FIELD + 50  # After raw row

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        content = context.description_body or context.normalized_markdown
        if not content:
            return self._make_skip_result("No content available")

        # Try location line pattern first (most explicit)
        match = _LOCATION_LINE_RE.search(content)
        if match:
            location = match.group("location").strip()
            is_valid, reason = _is_valid_location(location)
            if is_valid:
                return self._make_result(
                    location,
                    "Found 'Location:' label",
                    is_valid=True,
                    confidence=0.85,
                    debug_info={"pattern": "LOCATION_LINE"},
                )

        # Try label pattern (Location: City, ST)
        match = _LOCATION_LABEL_RE.search(content)
        if match:
            location = match.group("location").strip()
            is_valid, reason = _is_valid_location(location)
            if is_valid:
                return self._make_result(
                    location,
                    "Found location label pattern",
                    is_valid=True,
                    confidence=0.80,
                    debug_info={"pattern": "LOCATION_LABEL"},
                )

        return self._make_skip_result("No explicit location label found")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value)


class ContentPatternLocationStrategy(ExtractionStrategy[str]):
    """Extract location using regex patterns in content."""

    name = "content_pattern_location"
    priority = StrategyPriority.CONTENT_PATTERN

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        content = context.description_body or context.normalized_markdown
        if not content:
            return self._make_skip_result("No content available")

        # Try patterns in order of specificity
        patterns = [
            (_LOCATION_PAREN_RE, "LOCATION_PAREN", 0.75),
            (_LOCATION_CITY_STATE_RE, "LOCATION_CITY_STATE", 0.70),
            (_LOCATION_FULL_RE, "LOCATION_FULL", 0.65),
            (_SIMPLE_LOCATION_LINE_RE, "SIMPLE_LOCATION_LINE", 0.60),
        ]

        for pattern, name, confidence in patterns:
            match = pattern.search(content)
            if match:
                location = match.group("location").strip()
                is_valid, reason = _is_valid_location(location)
                if is_valid:
                    return self._make_result(
                        location,
                        f"Matched pattern {name}",
                        is_valid=True,
                        confidence=confidence,
                        debug_info={"pattern": name, "match_position": match.start()},
                    )

        return self._make_skip_result("No location pattern matched")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value)


class HintedLocationStrategy(ExtractionStrategy[str]):
    """Extract location from parse_markdown_hints() result."""

    name = "hinted_location"
    priority = StrategyPriority.HEURISTIC

    # Remote-only values should not be treated as locations
    _REMOTE_ONLY_VALUES = frozenset({
        "remote", "remote only", "fully remote", "100% remote",
        "work from home", "wfh", "anywhere", "work from anywhere",
    })

    def _is_remote_only(self, value: str) -> bool:
        """Check if value is a remote-only indicator, not a real location."""
        return value.lower().strip() in self._REMOTE_ONLY_VALUES

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        # Try locations list first, then single location
        locations = context.hints.get("locations")
        if isinstance(locations, list) and locations:
            # Join first few locations, filtering out remote-only values
            valid_locs = []
            for loc in locations[:5]:
                if isinstance(loc, str) and loc.strip():
                    cleaned = loc.strip()
                    # Skip remote-only values
                    if self._is_remote_only(cleaned):
                        continue
                    is_valid, _ = _is_valid_location(cleaned)
                    if is_valid:
                        valid_locs.append(cleaned)
            if valid_locs:
                combined = "; ".join(valid_locs)
                return self._make_result(
                    combined,
                    f"From hints.locations ({len(valid_locs)} locations)",
                    is_valid=True,
                    confidence=0.65,
                    debug_info={"source": "hints.locations", "count": len(valid_locs)},
                )

        # Single location hint
        location = context.hints.get("location")
        if isinstance(location, str) and location.strip():
            cleaned = location.strip()
            # Skip remote-only values
            if self._is_remote_only(cleaned):
                return self._make_skip_result(f"Remote-only location '{cleaned}' skipped")
            is_valid, reason = _is_valid_location(cleaned)
            return self._make_result(
                cleaned if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.60,
                debug_info={"source": "hints.location"},
            )

        return self._make_skip_result("No location in hints")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value)


class CountryOnlyFallbackLocationStrategy(ExtractionStrategy[str]):
    """Accept country-only locations when no more specific location is available."""

    name = "country_only_fallback_location"
    # Priority between CONTENT_PATTERN (500) and HEURISTIC (600) so explicit
    # country-only data beats inferred locations from content
    priority = StrategyPriority.CONTENT_PATTERN + 50  # 550

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        # Try raw row location even if it's country-only
        raw_location = context.get_raw_field("location", "city", "region", "office")
        if not raw_location:
            return self._make_skip_result("No raw location field")

        if isinstance(raw_location, dict):
            raw_location = raw_location.get("name") or raw_location.get("location")

        if not isinstance(raw_location, str) or not raw_location.strip():
            return self._make_skip_result("Raw location is empty")

        cleaned = raw_location.strip()
        # Accept country-only for this fallback strategy
        is_valid, reason = _is_valid_location(cleaned, allow_country_only=True)
        if is_valid:
            return self._make_result(
                cleaned,
                f"Country-only fallback: {reason}",
                is_valid=True,
                confidence=0.40,  # Low confidence for country-only
                debug_info={"raw_value": raw_location},
            )

        return self._make_skip_result(f"Location invalid even with country-only allowed: {reason}")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_location(value, allow_country_only=True)


class RemoteFallbackLocationStrategy(ExtractionStrategy[str]):
    """Use 'Remote' as location if job is marked as remote."""

    name = "remote_fallback_location"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        # Check if remote flag is set
        is_remote = context.hints.get("remote")
        if is_remote is True:
            return self._make_result(
                "Remote",
                "Job marked as remote, using 'Remote' as location",
                is_valid=True,
                confidence=0.50,
            )

        # Check extracted_remote from context
        if context.extracted_remote is True:
            return self._make_result(
                "Remote",
                "Job previously extracted as remote",
                is_valid=True,
                confidence=0.50,
            )

        return self._make_skip_result("Job not marked as remote")

    def validate(self, value: str) -> tuple[bool, str]:
        return True, "Valid fallback"


class LocationExtractor(FieldExtractor[str]):
    """
    Extracts job location using multiple strategies in priority order.

    Strategies (in order of priority):
    1. structured_data_location (100) - From JSON-LD/API response
    2. site_handler_location_hint (200) - From handler's extract_location_hint()
    3. raw_row_location (300) - From explicit location field (rejects country-only)
    4. explicit_label_location (350) - From "Location:" labels
    5. url_location (400) - From URL path patterns
    6. content_pattern_location (500) - From regex patterns in content
    7. country_only_fallback_location (550) - Accept country-only from raw row
    8. hinted_location (600) - From parse_markdown_hints()
    9. remote_fallback_location (900) - Fallback to "Remote" if applicable
    """

    field_name = "location"

    def _register_strategies(self) -> list[ExtractionStrategy[str]]:
        return [
            StructuredDataLocationStrategy(),
            SiteHandlerLocationHintStrategy(),
            RawRowLocationStrategy(),
            ExplicitLabelLocationStrategy(),
            URLLocationStrategy(),
            ContentPatternLocationStrategy(),
            HintedLocationStrategy(),
            CountryOnlyFallbackLocationStrategy(),
            RemoteFallbackLocationStrategy(),
        ]
