"""
Description extraction strategies and extractor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import (
    ExtractionStrategy,
    FieldExtractor,
    StrategyPriority,
    StrategyResult,
)

if TYPE_CHECKING:
    from .context import ExtractionContext


# Minimum description length to be considered valid
MIN_DESCRIPTION_LENGTH = 50

# Maximum description length (will be truncated)
MAX_DESCRIPTION_LENGTH = 50_000


def _clean_description(text: str) -> str:
    """Clean and normalize description text."""
    if not text:
        return ""

    # Remove excessive whitespace
    lines = []
    for line in text.splitlines():
        stripped = line.rstrip()
        lines.append(stripped)

    # Remove more than 2 consecutive blank lines
    result = []
    blank_count = 0
    for line in lines:
        if not line.strip():
            blank_count += 1
            if blank_count <= 2:
                result.append("")
        else:
            blank_count = 0
            result.append(line)

    text = "\n".join(result).strip()

    # Truncate if too long
    if len(text) > MAX_DESCRIPTION_LENGTH:
        text = text[:MAX_DESCRIPTION_LENGTH] + "..."

    return text


def _is_valid_description(value: str | None) -> tuple[bool, str]:
    """Validate a description value."""
    if not value:
        return False, "Empty description"

    if len(value) < MIN_DESCRIPTION_LENGTH:
        return False, f"Description too short: {len(value)} chars"

    # Check for placeholder content
    lower = value.lower()
    if lower in {"description", "job description", "n/a", "none", "tbd"}:
        return False, f"Placeholder description: {value[:50]}"

    return True, f"Valid description ({len(value)} chars)"


class NormalizedMarkdownDescriptionStrategy(ExtractionStrategy[str]):
    """Use the handler-normalized description body."""

    name = "normalized_markdown_description"
    priority = StrategyPriority.SITE_HANDLER

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        # Prefer description_body (cleaned) over raw normalized markdown
        content = context.description_body
        if not content:
            content = context.normalized_markdown

        if not content:
            return self._make_skip_result("No normalized markdown available")

        cleaned = _clean_description(content)
        is_valid, reason = _is_valid_description(cleaned)

        return self._make_result(
            cleaned if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.90,
            debug_info={"length": len(cleaned) if cleaned else 0},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_description(value)


class RawRowDescriptionStrategy(ExtractionStrategy[str]):
    """Extract description from raw row data."""

    name = "raw_row_description"
    priority = StrategyPriority.EXPLICIT_FIELD

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        raw_desc = context.get_raw_field(
            "job_description",
            "description",
            "desc",
            "body",
            "summary",
            "content",
        )

        if not raw_desc:
            return self._make_skip_result("No description field in raw row")

        if not isinstance(raw_desc, str):
            raw_desc = str(raw_desc)

        cleaned = _clean_description(raw_desc)
        is_valid, reason = _is_valid_description(cleaned)

        return self._make_result(
            cleaned if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.85,
            debug_info={"length": len(cleaned) if cleaned else 0},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_description(value)


class StructuredDataDescriptionStrategy(ExtractionStrategy[str]):
    """Extract description from structured data."""

    name = "structured_data_description"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        for key in ("description", "jobDescription", "content", "body", "summary"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                cleaned = _clean_description(value)
                is_valid, reason = _is_valid_description(cleaned)
                if is_valid:
                    return self._make_result(
                        cleaned,
                        f"Description from structured data ({key})",
                        is_valid=True,
                        confidence=0.90,
                        debug_info={"key": key, "length": len(cleaned)},
                    )

        return self._make_skip_result("No description in structured data")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_description(value)


class RawMarkdownDescriptionStrategy(ExtractionStrategy[str]):
    """Use raw markdown as fallback."""

    name = "raw_markdown_description"
    priority = StrategyPriority.FALLBACK - 100  # Higher priority than final fallback

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        content = context.raw_markdown
        if not content:
            return self._make_skip_result("No raw markdown available")

        cleaned = _clean_description(content)
        is_valid, reason = _is_valid_description(cleaned)

        return self._make_result(
            cleaned if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.50,
            debug_info={"length": len(cleaned) if cleaned else 0},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_description(value)


class EmptyDescriptionFallbackStrategy(ExtractionStrategy[str]):
    """Return empty string as last resort."""

    name = "empty_description_fallback"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        return self._make_result(
            "",
            "No description found, using empty string",
            is_valid=True,
            confidence=0.10,
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return True, "Valid empty fallback"


class DescriptionExtractor(FieldExtractor[str]):
    """
    Extracts job description using multiple strategies in priority order.

    Strategies (in order of priority):
    1. structured_data_description (100) - From structured data
    2. normalized_markdown_description (200) - Handler-normalized content
    3. raw_row_description (300) - From explicit description field
    4. raw_markdown_description (800) - Raw markdown fallback
    5. empty_description_fallback (900) - Empty string

    Returns the cleaned description text.
    """

    field_name = "description"

    def _register_strategies(self) -> list[ExtractionStrategy[str]]:
        return [
            StructuredDataDescriptionStrategy(),
            NormalizedMarkdownDescriptionStrategy(),
            RawRowDescriptionStrategy(),
            RawMarkdownDescriptionStrategy(),
            EmptyDescriptionFallbackStrategy(),
        ]
