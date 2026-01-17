"""
Compensation extraction strategies and extractor.
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


# Compensation range patterns
# Note: character class includes hyphen (-), en-dash (–), and em-dash (—)
_COMP_USD_RANGE_RE = re.compile(
    r"\$\s*(?P<low>\d{2,3}(?:[.,]\d{3})?)\s*(?:k|K)?\s*[-–—to]+\s*\$?\s*(?P<high>\d{2,3}(?:[.,]\d{3})?)\s*(?:k|K)?",
    re.IGNORECASE,
)
_COMP_SINGLE_RE = re.compile(
    r"\$\s*(?P<value>\d{2,3}(?:[.,]\d{3})?)\s*(?:k|K)?",
)
_COMP_K_RE = re.compile(
    r"(?P<value>\d{2,3})k",
    re.IGNORECASE,
)

# Bounds for valid compensation (annual, in dollars)
MIN_COMPENSATION = 20_000
MAX_COMPENSATION = 2_000_000


def _parse_comp_value(value: str) -> int | None:
    """Parse a compensation value string to int."""
    if not value:
        return None

    # Remove commas and spaces
    cleaned = value.replace(",", "").replace(" ", "").strip()

    try:
        num = float(cleaned)
        # If it looks like "150" (thousands in K notation), multiply by 1000
        if num < 1000:
            num = num * 1000
        return int(num)
    except ValueError:
        return None


def _normalize_comp(value: int | float | None) -> int | None:
    """Normalize and validate a compensation value."""
    if value is None:
        return None

    value = int(value)

    # Validate bounds
    if value < MIN_COMPENSATION:
        return None
    if value > MAX_COMPENSATION:
        return None

    return value


def _is_valid_compensation(value: int | None) -> tuple[bool, str]:
    """Validate a compensation value."""
    if value is None:
        return False, "No compensation value"

    if value < MIN_COMPENSATION:
        return False, f"Compensation too low: ${value:,}"

    if value > MAX_COMPENSATION:
        return False, f"Compensation too high: ${value:,}"

    return True, f"Valid compensation: ${value:,}"


class ExplicitCompensationFieldStrategy(ExtractionStrategy[int]):
    """Extract compensation from explicit field in raw data."""

    name = "explicit_compensation_field"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        raw_comp = context.get_raw_field(
            "total_compensation",
            "totalCompensation",
            "salary",
            "compensation",
            "pay",
        )

        if raw_comp is None:
            return self._make_skip_result("No compensation field in raw row")

        # Handle numeric values directly
        if isinstance(raw_comp, (int, float)) and raw_comp > 0:
            normalized = _normalize_comp(raw_comp)
            if normalized:
                return self._make_result(
                    normalized,
                    f"Explicit compensation: {raw_comp} -> ${normalized:,}",
                    is_valid=True,
                    confidence=0.95,
                    debug_info={"raw_value": raw_comp},
                )
            return self._make_skip_result(f"Compensation out of bounds: {raw_comp}")

        # Handle string values
        if isinstance(raw_comp, str):
            from ..helpers.compensation_parsing import parse_compensation

            parsed, is_unknown = parse_compensation(raw_comp, with_meta=True)
            if parsed > 0 and not is_unknown:
                return self._make_result(
                    parsed,
                    f"Parsed compensation: {raw_comp} -> ${parsed:,}",
                    is_valid=True,
                    confidence=0.90,
                    debug_info={"raw_value": raw_comp},
                )

        return self._make_skip_result(f"Could not parse compensation: {raw_comp}")

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_compensation(value)


class StructuredDataCompensationStrategy(ExtractionStrategy[int]):
    """Extract compensation from structured data."""

    name = "structured_data_compensation"
    priority = StrategyPriority.STRUCTURED_DATA + 50

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        for key in (
            "baseSalary",
            "salary",
            "compensation",
            "pay",
            "totalCompensation",
            "estimatedSalary",
        ):
            value = data.get(key)
            if value is None:
                continue

            # Handle nested salary object (Schema.org format)
            if isinstance(value, dict):
                # Try to get value from nested structure
                nested_value = value.get("value") or value.get("minValue") or value.get("maxValue")
                if nested_value is not None:
                    value = nested_value

            # Parse the value
            if isinstance(value, (int, float)) and value > 0:
                normalized = _normalize_comp(value)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"Structured data compensation: ${normalized:,}",
                        is_valid=True,
                        confidence=0.90,
                        debug_info={"key": key},
                    )

            if isinstance(value, str):
                from ..helpers.compensation_parsing import parse_compensation

                parsed, is_unknown = parse_compensation(value, with_meta=True)
                if parsed > 0 and not is_unknown:
                    return self._make_result(
                        parsed,
                        f"Parsed structured compensation: ${parsed:,}",
                        is_valid=True,
                        confidence=0.85,
                        debug_info={"key": key, "raw_value": value},
                    )

        return self._make_skip_result("No compensation in structured data")

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_compensation(value)


class HintedCompensationStrategy(ExtractionStrategy[int]):
    """Extract compensation from parse_markdown_hints() result.

    This has higher priority than content pattern matching because
    parse_markdown_hints() applies location-aware logic to select
    the appropriate compensation range for the job's location.
    """

    name = "hinted_compensation"
    priority = StrategyPriority.CONTENT_PATTERN - 50  # Higher priority than content patterns

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        # Try compensation range first
        comp_range = context.hints.get("compensation_range")
        if comp_range:
            low, high = None, None
            # Handle dict format: {"low": 186300, "high": 279500}
            if isinstance(comp_range, dict):
                low = comp_range.get("low") or comp_range.get("min")
                high = comp_range.get("high") or comp_range.get("max")
            # Handle list/tuple format: [186300, 279500]
            elif isinstance(comp_range, (list, tuple)) and len(comp_range) >= 2:
                low, high = comp_range[0], comp_range[1]

            if isinstance(low, (int, float)) and isinstance(high, (int, float)):
                # Use average of range for backwards compatibility with old heuristics
                avg = int((low + high) / 2)
                normalized = _normalize_comp(avg)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"Compensation from hint range: ${int(low):,}-${int(high):,} -> ${normalized:,} (avg)",
                        is_valid=True,
                        confidence=0.70,
                        debug_info={"range": [low, high]},
                    )

        # Try single compensation value
        comp = context.hints.get("compensation") or context.hints.get("salary")
        if comp is not None:
            if isinstance(comp, (int, float)) and comp > 0:
                normalized = _normalize_comp(comp)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"Compensation from hints: ${normalized:,}",
                        is_valid=True,
                        confidence=0.65,
                    )

            if isinstance(comp, str):
                from ..helpers.compensation_parsing import parse_compensation

                parsed, is_unknown = parse_compensation(comp, with_meta=True)
                if parsed > 0 and not is_unknown:
                    return self._make_result(
                        parsed,
                        f"Parsed hint compensation: ${parsed:,}",
                        is_valid=True,
                        confidence=0.60,
                    )

        return self._make_skip_result("No compensation in hints")

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_compensation(value)


class ContentPatternCompensationStrategy(ExtractionStrategy[int]):
    """Extract compensation from content patterns."""

    name = "content_pattern_compensation"
    priority = StrategyPriority.CONTENT_PATTERN

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        content = context.description_body or context.normalized_markdown
        if not content:
            return self._make_skip_result("No content available")

        # Try USD range pattern first
        match = _COMP_USD_RANGE_RE.search(content)
        if match:
            low_str = match.group("low")
            high_str = match.group("high")
            low = _parse_comp_value(low_str)
            high = _parse_comp_value(high_str)

            if low and high:
                # Use average of range
                avg = int((low + high) / 2)
                normalized = _normalize_comp(avg)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"Compensation range pattern: ${low:,}-${high:,} -> ${normalized:,}",
                        is_valid=True,
                        confidence=0.70,
                        debug_info={"pattern": "USD_RANGE", "match": match.group()},
                    )

        # Try single USD pattern
        match = _COMP_SINGLE_RE.search(content)
        if match:
            value_str = match.group("value")
            value = _parse_comp_value(value_str)
            if value:
                normalized = _normalize_comp(value)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"Single compensation pattern: ${normalized:,}",
                        is_valid=True,
                        confidence=0.55,
                        debug_info={"pattern": "SINGLE_USD", "match": match.group()},
                    )

        # Try K notation (e.g., "150k")
        match = _COMP_K_RE.search(content)
        if match:
            value = int(match.group("value")) * 1000
            # Skip 401k - it's a retirement plan, not compensation
            if value == 401000:
                pass  # Skip this match
            else:
                normalized = _normalize_comp(value)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"K notation compensation: ${normalized:,}",
                        is_valid=True,
                        confidence=0.50,
                        debug_info={"pattern": "K_NOTATION", "match": match.group()},
                    )

        return self._make_skip_result("No compensation pattern in content")

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_compensation(value)


class UnknownCompensationStrategy(ExtractionStrategy[int]):
    """Return 0 to indicate unknown compensation."""

    name = "unknown_compensation"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        return self._make_result(
            0,
            "No compensation found, using 0 (unknown)",
            is_valid=True,
            confidence=0.10,
        )

    def validate(self, value: int) -> tuple[bool, str]:
        return True, "Valid unknown placeholder"


class CompensationExtractor(FieldExtractor[int]):
    """
    Extracts compensation using multiple strategies in priority order.

    Strategies (in order of priority):
    1. explicit_compensation_field (100) - From explicit field
    2. structured_data_compensation (150) - From structured data
    3. hinted_compensation (450) - From parse_markdown_hints()
    4. content_pattern_compensation (500) - From content patterns
    5. unknown_compensation (900) - Default to 0 (unknown)

    Note: hinted_compensation has higher priority than content patterns
    because parse_markdown_hints() applies location-aware logic to select
    the appropriate compensation range for the job's location.

    Returns compensation in annual dollars, or 0 if unknown.
    """

    field_name = "compensation"

    def _register_strategies(self) -> list[ExtractionStrategy[int]]:
        return [
            ExplicitCompensationFieldStrategy(),
            StructuredDataCompensationStrategy(),
            ContentPatternCompensationStrategy(),
            HintedCompensationStrategy(),
            UnknownCompensationStrategy(),
        ]
