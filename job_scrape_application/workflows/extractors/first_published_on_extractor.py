"""
First published date extraction strategies and extractor.
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


def _normalize_first_published(value: Any) -> datetime | None:
    """Normalize various date formats to datetime."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    if isinstance(value, (int, float)):
        if value > 1e12:
            value = value / 1000
        if 0 < value < 1e11:
            try:
                return datetime.fromtimestamp(value)
            except (ValueError, OSError):
                pass
        return None

    if isinstance(value, str):
        from ..helpers.timestamp_parsing import parse_posted_at_with_unknown

        try:
            result_ms, is_unknown = parse_posted_at_with_unknown(value)
            if not is_unknown and result_ms:
                return datetime.fromtimestamp(result_ms / 1000)
        except Exception:
            pass

    return None


def _is_valid_first_published(value: datetime | None) -> tuple[bool, str]:
    """Validate a first_published datetime."""
    if value is None:
        return False, "No date value"

    now = datetime.now()
    min_date = datetime(2010, 1, 1)

    if value < min_date:
        return False, f"Date too old: {value}"

    if value > now:
        return False, f"Date in future: {value}"

    return True, f"Valid date: {value.isoformat()}"


class ExplicitFirstPublishedFieldStrategy(ExtractionStrategy[datetime]):
    """Extract first_published from explicit field in raw data."""

    name = "explicit_first_published_field"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        raw_date = context.get_raw_field(
            "first_published",
            "firstPublished",
            "first_published_on",
            "firstPublishedOn",
            "postingFirstPublishedAt",
        )

        if raw_date is None:
            return self._make_skip_result("No first_published field in raw row")

        normalized = _normalize_first_published(raw_date)
        if normalized:
            is_valid, reason = _is_valid_first_published(normalized)
            return self._make_result(
                normalized if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.95,
                debug_info={"raw_value": str(raw_date)},
            )

        return self._make_skip_result(f"Could not parse date: {raw_date}")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_first_published(value)


class StructuredDataFirstPublishedStrategy(ExtractionStrategy[datetime]):
    """Extract first_published from structured data."""

    name = "structured_data_first_published"
    priority = StrategyPriority.STRUCTURED_DATA + 50

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        for key in (
            "first_published",
            "firstPublished",
            "first_published_on",
            "firstPublishedOn",
        ):
            value = data.get(key)
            if value is not None:
                normalized = _normalize_first_published(value)
                if normalized:
                    is_valid, reason = _is_valid_first_published(normalized)
                    if is_valid:
                        return self._make_result(
                            normalized,
                            f"Date from structured data ({key}): {normalized.isoformat()}",
                            is_valid=True,
                            confidence=0.90,
                            debug_info={"key": key, "raw_value": str(value)},
                        )

        return self._make_skip_result("No first_published key found in structured data")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_first_published(value)


class SiteHandlerFirstPublishedStrategy(ExtractionStrategy[datetime]):
    """Extract first_published from site handler's extract_first_published() method."""

    name = "site_handler_first_published"
    priority = StrategyPriority.SITE_HANDLER

    def extract(self, context: ExtractionContext) -> StrategyResult[datetime]:
        handler = context.handler
        if not handler:
            return self._make_skip_result("No handler available")

        if not hasattr(handler, "extract_first_published"):
            return self._make_skip_result("Handler has no extract_first_published method")

        try:
            payload = context.structured_data or context.json_payload
            first_published = handler.extract_first_published(payload, context.url)
        except Exception as e:
            return self._make_skip_result(f"Handler error: {e}")

        if first_published is None:
            return self._make_skip_result(
                f"Handler '{context.handler_name}' returned no first_published"
            )

        normalized = _normalize_first_published(first_published)
        if normalized:
            is_valid, reason = _is_valid_first_published(normalized)
            return self._make_result(
                normalized if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.85,
                debug_info={"handler": context.handler_name},
            )

        return self._make_skip_result(f"Could not normalize handler date: {first_published}")

    def validate(self, value: datetime) -> tuple[bool, str]:
        return _is_valid_first_published(value)


class FirstPublishedOnExtractor(FieldExtractor[datetime]):
    """
    Extracts first_published date using multiple strategies in priority order.

    Strategies (in order of priority):
    1. explicit_first_published_field (100) - From explicit field
    2. structured_data_first_published (150) - From structured data
    3. site_handler_first_published (200) - From handler's extract_first_published()

    Returns a datetime object.
    """

    field_name = "first_published_on"

    def _register_strategies(self) -> list[ExtractionStrategy[datetime]]:
        return [
            ExplicitFirstPublishedFieldStrategy(),
            StructuredDataFirstPublishedStrategy(),
            SiteHandlerFirstPublishedStrategy(),
        ]
