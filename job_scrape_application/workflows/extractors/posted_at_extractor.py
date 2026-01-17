"""
Posted date extraction strategies and extractor.
"""

from __future__ import annotations

from datetime import datetime, date
from typing import TYPE_CHECKING, Any

from .base import (
    ExtractionStrategy,
    FieldExtractor,
    StrategyPriority,
    StrategyResult,
)

if TYPE_CHECKING:
    from .context import ExtractionContext


def _normalize_posted_at(value: Any) -> datetime | None:
    """Normalize various date formats to datetime."""
    if value is None:
        return None

    # Already a datetime
    if isinstance(value, datetime):
        return value

    # Date object
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    # Unix timestamp (int or float)
    if isinstance(value, (int, float)):
        # Check if it's milliseconds (> 1e12) or seconds
        if value > 1e12:
            value = value / 1000
        if 0 < value < 1e11:  # Reasonable timestamp range
            try:
                return datetime.fromtimestamp(value)
            except (ValueError, OSError):
                pass
        return None

    # String parsing
    if isinstance(value, str):
        from ..helpers.timestamp_parsing import parse_posted_at_with_unknown

        try:
            # parse_posted_at_with_unknown returns (timestamp_ms, is_unknown)
            result_ms, is_unknown = parse_posted_at_with_unknown(value)
            if not is_unknown and result_ms:
                # Convert milliseconds timestamp to datetime
                return datetime.fromtimestamp(result_ms / 1000)
        except Exception:
            pass

    return None


def _is_valid_posted_at(value: datetime | None) -> tuple[bool, str]:
    """Validate a posted_at datetime."""
    if value is None:
        return False, "No date value"

    # Check if date is in reasonable range
    now = datetime.now()
    min_date = datetime(2010, 1, 1)  # Job posting before 2010 unlikely

    if value < min_date:
        return False, f"Date too old: {value}"

    if value > now:
        return False, f"Date in future: {value}"

    return True, f"Valid date: {value.isoformat()}"


class ExplicitPostedAtFieldStrategy(ExtractionStrategy[datetime]):
    """Extract posted_at from explicit field in raw data."""

    name = "explicit_posted_at_field"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        raw_date = context.get_raw_field(
            "posted_at",
            "postedAt",
            "date_posted",
            "datePosted",
            "publishedAt",
            "published_at",
        )

        if raw_date is None:
            return self._make_skip_result("No posted_at field in raw row")

        normalized = _normalize_posted_at(raw_date)
        if normalized:
            is_valid, reason = _is_valid_posted_at(normalized)
            return self._make_result(
                normalized if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.95,
                debug_info={"raw_value": str(raw_date)},
            )

        return self._make_skip_result(f"Could not parse date: {raw_date}")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_posted_at(value)


class StructuredDataPostedAtStrategy(ExtractionStrategy[datetime]):
    """Extract posted_at from structured data."""

    name = "structured_data_posted_at"
    priority = StrategyPriority.STRUCTURED_DATA + 50

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        # Try common date keys in priority order
        for key in (
            "datePosted",
            "postedAt",
            "posted_at",
            "publishedAt",
            "published_at",
            "createdAt",
            "created_at",
            "updatedAt",
            "updated_at",
        ):
            value = data.get(key)
            if value is not None:
                normalized = _normalize_posted_at(value)
                if normalized:
                    is_valid, reason = _is_valid_posted_at(normalized)
                    if is_valid:
                        return self._make_result(
                            normalized,
                            f"Date from structured data ({key}): {normalized.isoformat()}",
                            is_valid=True,
                            confidence=0.90,
                            debug_info={"key": key, "raw_value": str(value)},
                        )

        return self._make_skip_result("No date key found in structured data")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_posted_at(value)


class SiteHandlerPostedAtStrategy(ExtractionStrategy[datetime]):
    """Extract posted_at from site handler's extract_posted_at() method."""

    name = "site_handler_posted_at"
    priority = StrategyPriority.SITE_HANDLER

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        handler = context.handler
        if not handler:
            return self._make_skip_result("No handler available")

        if not hasattr(handler, "extract_posted_at"):
            return self._make_skip_result("Handler has no extract_posted_at method")

        try:
            payload = context.structured_data or context.json_payload
            posted_at = handler.extract_posted_at(payload, context.url)
        except Exception as e:
            return self._make_skip_result(f"Handler error: {e}")

        if posted_at is None:
            return self._make_skip_result(
                f"Handler '{context.handler_name}' returned no posted_at"
            )

        normalized = _normalize_posted_at(posted_at)
        if normalized:
            is_valid, reason = _is_valid_posted_at(normalized)
            return self._make_result(
                normalized if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.85,
                debug_info={"handler": context.handler_name},
            )

        return self._make_skip_result(f"Could not normalize handler date: {posted_at}")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_posted_at(value)


class HintedPostedAtStrategy(ExtractionStrategy[datetime]):
    """Extract posted_at from parse_markdown_hints() result."""

    name = "hinted_posted_at"
    priority = StrategyPriority.HEURISTIC

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        posted_at = context.hints.get("posted_at") or context.hints.get("date")

        if posted_at is None:
            return self._make_skip_result("No posted_at in hints")

        normalized = _normalize_posted_at(posted_at)
        if normalized:
            is_valid, reason = _is_valid_posted_at(normalized)
            return self._make_result(
                normalized if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.60,
            )

        return self._make_skip_result(f"Could not normalize hint date: {posted_at}")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_posted_at(value)


class ContentPatternPostedAtStrategy(ExtractionStrategy[datetime]):
    """Extract posted_at from content patterns."""

    name = "content_pattern_posted_at"
    priority = StrategyPriority.CONTENT_PATTERN

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        content = context.metadata_block or context.description_body
        if not content:
            return self._make_skip_result("No content available")

        # Use the handler's extract_posted_at_from_markdown if available
        handler = context.handler
        if handler and hasattr(handler, "extract_posted_at_from_markdown"):
            try:
                result = handler.extract_posted_at_from_markdown(content)
                if result:
                    normalized = _normalize_posted_at(result)
                    if normalized:
                        is_valid, reason = _is_valid_posted_at(normalized)
                        if is_valid:
                            return self._make_result(
                                normalized,
                                "Date from markdown via handler",
                                is_valid=True,
                                confidence=0.55,
                            )
            except Exception:
                pass

        return self._make_skip_result("No date pattern in content")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_posted_at(value)


class NowFallbackPostedAtStrategy(ExtractionStrategy[datetime]):
    """Use current datetime as fallback."""

    name = "now_fallback_posted_at"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        now = datetime.now()
        return self._make_result(
            now,
            "Using current time as fallback (date unknown)",
            is_valid=True,
            confidence=0.10,
        )

    def validate(self, value: datetime) -> tuple[bool, str]:
        return True, "Valid fallback"


class PostedAtExtractor(FieldExtractor[datetime]):
    """
    Extracts posted_at date using multiple strategies in priority order.

    Strategies (in order of priority):
    1. explicit_posted_at_field (100) - From explicit field
    2. structured_data_posted_at (150) - From structured data
    3. site_handler_posted_at (200) - From handler's extract_posted_at()
    4. content_pattern_posted_at (500) - From content patterns
    5. hinted_posted_at (600) - From parse_markdown_hints()
    6. now_fallback_posted_at (900) - Default to now

    Returns a datetime object.
    """

    field_name = "posted_at"

    def _register_strategies(self) -> list[ExtractionStrategy[datetime]]:
        return [
            ExplicitPostedAtFieldStrategy(),
            StructuredDataPostedAtStrategy(),
            SiteHandlerPostedAtStrategy(),
            ContentPatternPostedAtStrategy(),
            HintedPostedAtStrategy(),
            NowFallbackPostedAtStrategy(),
        ]
