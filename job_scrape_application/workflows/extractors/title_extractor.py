"""
Title extraction strategies and extractor.
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


# Invalid/generic titles that should be rejected
_INVALID_TITLES = frozenset(
    {
        "job description",
        "description",
        "the role",
        "our team",
        "about",
        "untitled",
        "unknown",
        "overview",
        "summary",
        "position",
        "opportunity",
        "career",
        "careers",
        "job",
        "jobs",
        "role",
        "apply now",
        "apply",
        "home",
        "homepage",
    }
)

# Markdown heading pattern
_HEADING_RE = re.compile(r"^[ \t]*#{1,3}\s+(?P<title>.+)$", re.MULTILINE)


def _is_valid_title(value: str | None) -> tuple[bool, str]:
    """Validate a title value."""
    if not value:
        return False, "Empty title"

    value = value.strip()
    if len(value) < 3:
        return False, f"Title too short: {len(value)} chars"

    if len(value) > 200:
        return False, f"Title too long: {len(value)} chars"

    # Check for generic/invalid titles
    lower = value.lower().strip()
    if lower in _INVALID_TITLES:
        return False, f"Generic title rejected: {value}"

    # Check for URL-as-title
    if lower.startswith(("http://", "https://", "www.")):
        return False, "URL as title rejected"

    # Check for markdown artifacts
    if value.startswith(("#", "[", "*")):
        # These are likely leftover markdown
        if re.match(r"^[#\[\]*]+\s*$", value):
            return False, "Markdown artifact rejected"

    # Check word count (too many words suggests a sentence, not a title)
    word_count = len(value.split())
    if word_count > 15:
        return False, f"Title has too many words ({word_count}), likely a sentence"

    return True, "Valid title"


class StructuredDataTitleStrategy(ExtractionStrategy[str]):
    """Extract title from Schema.org JobPosting or API JSON."""

    name = "structured_data_title"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        # Try common title keys in priority order
        for key in ("title", "name", "jobTitle", "job_title", "positionTitle", "position"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                cleaned = value.strip()
                is_valid, reason = _is_valid_title(cleaned)
                return self._make_result(
                    cleaned if is_valid else None,
                    reason,
                    is_valid=is_valid,
                    confidence=0.95,
                    debug_info={"key": key, "raw_value": value},
                )

        return self._make_skip_result("No title key found in structured data")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_title(value)


class SiteHandlerTitleStrategy(ExtractionStrategy[str]):
    """Extract title from site handler's normalize_markdown() return value."""

    name = "site_handler_title"
    priority = StrategyPriority.SITE_HANDLER

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        handler_title = context.handler_extracted_title
        if not handler_title:
            handler_name = context.handler_name or "base"
            return self._make_skip_result(f"Handler '{handler_name}' did not extract title")

        is_valid, reason = _is_valid_title(handler_title)
        return self._make_result(
            handler_title if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.90,
            debug_info={"handler": context.handler_name},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_title(value)


class RawRowTitleStrategy(ExtractionStrategy[str]):
    """Extract title from raw row data (job_title or title field)."""

    name = "raw_row_title"
    priority = StrategyPriority.EXPLICIT_FIELD

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        # Try common field names
        raw_title = context.get_raw_field("job_title", "title", "jobTitle", "positionTitle")
        if not raw_title:
            return self._make_skip_result("No title field in raw row")

        if not isinstance(raw_title, str):
            raw_title = str(raw_title)

        # Normalize: remove " | Company" suffix patterns
        from ..helpers.company_normalization import normalize_title_from_bar

        cleaned = normalize_title_from_bar(raw_title.strip())
        if not cleaned:
            cleaned = raw_title.strip()

        is_valid, reason = _is_valid_title(cleaned)
        return self._make_result(
            cleaned if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.85,
            debug_info={"raw_value": raw_title},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_title(value)


class MarkdownHeadingTitleStrategy(ExtractionStrategy[str]):
    """Extract title from first markdown heading (# Title)."""

    name = "markdown_heading_title"
    priority = StrategyPriority.CONTENT_PATTERN

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        content = context.normalized_markdown or context.raw_markdown
        if not content:
            return self._make_skip_result("No markdown content")

        # Find first heading
        match = _HEADING_RE.search(content)
        if not match:
            return self._make_skip_result("No markdown heading found")

        raw_title = match.group("title").strip()

        # Clean up: remove trailing " | Location" or " - Company"
        if " | " in raw_title:
            raw_title = raw_title.split(" | ", 1)[0].strip()
        elif " - " in raw_title and len(raw_title.split(" - ")) == 2:
            parts = raw_title.split(" - ")
            # Keep the longer part as title
            raw_title = max(parts, key=len).strip()

        is_valid, reason = _is_valid_title(raw_title)
        return self._make_result(
            raw_title if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.70,
            raw_input=content[:300],
            debug_info={"match_position": match.start()},
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_title(value)


class HintedTitleStrategy(ExtractionStrategy[str]):
    """Extract title from parse_markdown_hints() result."""

    name = "hinted_title"
    priority = StrategyPriority.HEURISTIC

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        hinted = context.hints.get("title")
        if not hinted:
            return self._make_skip_result("No title in hints")

        if not isinstance(hinted, str):
            return self._make_skip_result(f"Hint title is not a string: {type(hinted)}")

        is_valid, reason = _is_valid_title(hinted)
        return self._make_result(
            hinted if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.65,
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_title(value)


class FirstLineTitleStrategy(ExtractionStrategy[str]):
    """Extract title from first non-empty line of content."""

    name = "first_line_title"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        content = context.description_body or context.normalized_markdown or context.raw_markdown
        if not content:
            return self._make_skip_result("No content available")

        # Find first non-empty line
        for line in content.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            # Skip lines that are clearly not titles
            lower = stripped.lower()
            if lower.startswith(("http", "location", "company", "posted", "date", "apply")):
                continue

            # Clean markdown artifacts
            if stripped.startswith("#"):
                stripped = stripped.lstrip("#").strip()
            if stripped.startswith(("*", "-", "•")):
                continue  # Skip list items

            is_valid, reason = _is_valid_title(stripped)
            if is_valid:
                return self._make_result(
                    stripped,
                    reason,
                    is_valid=True,
                    confidence=0.40,
                    raw_input=content[:200],
                )

        return self._make_skip_result("No valid title found in first lines")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_title(value)


class JobTitleExtractor(FieldExtractor[str]):
    """
    Extracts job title using multiple strategies in priority order.

    Strategies (in order of priority):
    1. structured_data_title (100) - From JSON-LD/API response
    2. site_handler_title (200) - From handler's normalize_markdown()
    3. raw_row_title (300) - From explicit job_title/title field
    4. markdown_heading_title (500) - From first # heading
    5. hinted_title (600) - From parse_markdown_hints()
    6. first_line_title (900) - Fallback to first line
    """

    field_name = "title"

    def _register_strategies(self) -> list[ExtractionStrategy[str]]:
        return [
            StructuredDataTitleStrategy(),
            SiteHandlerTitleStrategy(),
            RawRowTitleStrategy(),
            MarkdownHeadingTitleStrategy(),
            HintedTitleStrategy(),
            FirstLineTitleStrategy(),
        ]
