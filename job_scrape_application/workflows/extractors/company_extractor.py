"""
Company extraction strategies and extractor.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .base import (
    ExtractionStrategy,
    FieldExtractor,
    StrategyPriority,
    StrategyResult,
)

if TYPE_CHECKING:
    from .context import ExtractionContext


# Generic company names to reject
_GENERIC_COMPANY_NAMES = frozenset(
    {
        "unknown",
        "company",
        "employer",
        "hiring",
        "confidential",
        "n/a",
        "na",
        "none",
        "tbd",
        "various",
        "multiple",
    }
)


def _is_valid_company(value: str | None) -> tuple[bool, str]:
    """Validate a company name value."""
    if not value:
        return False, "Empty company name"

    value = value.strip()
    if len(value) < 2:
        return False, f"Company name too short: {len(value)} chars"

    if len(value) > 100:
        return False, f"Company name too long: {len(value)} chars"

    # Check for generic names
    lower = value.lower().strip()
    if lower in _GENERIC_COMPANY_NAMES:
        return False, f"Generic company name: {value}"

    # Check for URL-as-company
    if lower.startswith(("http://", "https://", "www.")):
        return False, "URL as company name rejected"

    return True, "Valid company name"


class StructuredDataCompanyStrategy(ExtractionStrategy[str]):
    """Extract company from Schema.org JobPosting or API JSON."""

    name = "structured_data_company"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        # Try common company keys
        for key in (
            "company",
            "company_name",
            "companyName",
            "employer",
            "organization",
            "hiringOrganization",
        ):
            value = data.get(key)
            if value is None:
                continue

            # Handle dict with name field
            if isinstance(value, dict):
                value = value.get("name") or value.get("company")

            if isinstance(value, str) and value.strip():
                cleaned = value.strip()
                is_valid, reason = _is_valid_company(cleaned)
                return self._make_result(
                    cleaned if is_valid else None,
                    reason,
                    is_valid=is_valid,
                    confidence=0.95,
                    debug_info={"key": key, "raw_value": data.get(key)},
                )

        return self._make_skip_result("No company key found in structured data")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_company(value)


class SiteHandlerCompanyStrategy(ExtractionStrategy[str]):
    """Extract company from site handler's extract_company() method."""

    name = "site_handler_company"
    priority = StrategyPriority.SITE_HANDLER

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        handler = context.handler
        if not handler:
            return self._make_skip_result("No handler available")

        if not hasattr(handler, "extract_company"):
            return self._make_skip_result("Handler has no extract_company method")

        try:
            payload = context.structured_data or context.json_payload
            company = handler.extract_company(payload, context.url)
        except Exception as e:
            return self._make_skip_result(f"Handler error: {e}")

        if not company:
            return self._make_skip_result(
                f"Handler '{context.handler_name}' returned no company"
            )

        is_valid, reason = _is_valid_company(company)
        return self._make_result(
            company if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.90,
            debug_info={"handler": context.handler_name},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_company(value)


class RawRowCompanyStrategy(ExtractionStrategy[str]):
    """Extract company from raw row data."""

    name = "raw_row_company"
    priority = StrategyPriority.EXPLICIT_FIELD

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        raw_company = context.get_raw_field(
            "company", "company_name", "employer", "organization"
        )
        if not raw_company:
            return self._make_skip_result("No company field in raw row")

        if not isinstance(raw_company, str):
            raw_company = str(raw_company)

        cleaned = raw_company.strip()
        is_valid, reason = _is_valid_company(cleaned)
        return self._make_result(
            cleaned if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.85,
            debug_info={"raw_value": raw_company},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_company(value)


class URLCompanyStrategy(ExtractionStrategy[str]):
    """Extract company from URL domain/path patterns."""

    name = "url_company"
    priority = StrategyPriority.URL_DERIVED

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        url = context.url
        if not url:
            return self._make_skip_result("No URL available")

        # Use the existing derive_company_from_url function
        from ..helpers.company_normalization import derive_company_from_url

        try:
            company = derive_company_from_url(url)
        except Exception as e:
            return self._make_skip_result(f"URL parse error: {e}")

        if not company:
            return self._make_skip_result("Could not derive company from URL")

        is_valid, reason = _is_valid_company(company)
        return self._make_result(
            company if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.70,
            debug_info={"url": url},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_company(value)


class HintedCompanyStrategy(ExtractionStrategy[str]):
    """Extract company from parse_markdown_hints() result."""

    name = "hinted_company"
    priority = StrategyPriority.HEURISTIC

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        company = context.hints.get("company")
        if not company:
            return self._make_skip_result("No company in hints")

        if not isinstance(company, str):
            return self._make_skip_result(f"Hint company is not a string: {type(company)}")

        is_valid, reason = _is_valid_company(company)
        return self._make_result(
            company if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.60,
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_company(value)


class ContentPatternCompanyStrategy(ExtractionStrategy[str]):
    """Extract company from common patterns in content."""

    name = "content_pattern_company"
    priority = StrategyPriority.CONTENT_PATTERN

    # Pattern: "About [Company]" or "[Company] is a"
    _ABOUT_COMPANY_RE = re.compile(
        r"about\s+(?P<company>[A-Z][A-Za-z0-9\s&.'-]+?)(?:\s+is|\s*$)",
        re.IGNORECASE,
    )
    _COMPANY_IS_RE = re.compile(
        r"^\s*(?P<company>[A-Z][A-Za-z0-9\s&.'-]{2,40}?)\s+is\s+(?:a|an|the)\s+",
        re.IGNORECASE | re.MULTILINE,
    )
    _COMPANY_LINK_RE = re.compile(
        r"^\s*\[(?P<company>[^\]]+)\]\([^)]+\)\s+is\s+(?:a|an|the)\b",
        re.IGNORECASE | re.MULTILINE,
    )

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        content = context.description_body or context.normalized_markdown
        if not content:
            return self._make_skip_result("No content available")

        # Try company link pattern first
        match = self._COMPANY_LINK_RE.search(content)
        if match:
            company = match.group("company").strip()
            is_valid, reason = _is_valid_company(company)
            if is_valid:
                return self._make_result(
                    company,
                    "Found company in markdown link",
                    is_valid=True,
                    confidence=0.65,
                    debug_info={"pattern": "COMPANY_LINK"},
                )

        # Try "Company is a" pattern
        match = self._COMPANY_IS_RE.search(content)
        if match:
            company = match.group("company").strip()
            is_valid, reason = _is_valid_company(company)
            if is_valid:
                return self._make_result(
                    company,
                    "Found 'Company is a' pattern",
                    is_valid=True,
                    confidence=0.55,
                    debug_info={"pattern": "COMPANY_IS"},
                )

        return self._make_skip_result("No company pattern matched")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_company(value)


class FallbackCompanyStrategy(ExtractionStrategy[str]):
    """Use 'Unknown' as company name as last resort."""

    name = "fallback_company"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        return self._make_result(
            "Unknown",
            "Fallback to 'Unknown' company name",
            is_valid=True,
            confidence=0.10,
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return True, "Valid fallback"


class CompanyExtractor(FieldExtractor[str]):
    """
    Extracts company name using multiple strategies in priority order.

    Strategies (in order of priority):
    1. structured_data_company (100) - From JSON-LD/API response
    2. site_handler_company (200) - From handler's extract_company()
    3. raw_row_company (300) - From explicit company field
    4. url_company (400) - From URL domain/path
    5. content_pattern_company (500) - From content patterns
    6. hinted_company (600) - From parse_markdown_hints()
    7. fallback_company (900) - Use 'Unknown'
    """

    field_name = "company"

    def _register_strategies(self) -> list[ExtractionStrategy[str]]:
        return [
            StructuredDataCompanyStrategy(),
            SiteHandlerCompanyStrategy(),
            RawRowCompanyStrategy(),
            URLCompanyStrategy(),
            ContentPatternCompanyStrategy(),
            HintedCompanyStrategy(),
            FallbackCompanyStrategy(),
        ]
