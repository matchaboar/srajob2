"""
Job level extraction strategies and extractor.
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


# Valid job levels
VALID_LEVELS = frozenset(
    {
        "intern",
        "junior",
        "mid",
        "senior",
        "staff",
        "principal",
        "lead",
        "manager",
        "director",
        "vp",
        "cxo",
    }
)

# Level detection patterns
_LEVEL_RE = re.compile(
    r"\b(?P<level>intern(?:ship)?|junior|jr\.?|mid(?:-level)?|"
    r"sr\.?|senior|staff|principal|lead|manager|director|vp|"
    r"chief|head|c-level|cto|ceo|cfo)\b",
    re.IGNORECASE,
)


def _normalize_level(value: str) -> str:
    """Normalize a level string to a standard value.

    Level classification guidelines:
    - junior: intern, junior, jr, new grad, entry level (0-2 years)
    - mid: standard roles without level prefix (2-5 years)
    - senior: Senior prefix, significant experience (5+ years)
    - staff: Staff/Principal/Lead/Director/VP/Chief/Head/Distinguished titles (8+ years)

    Note: "Manager" is ambiguous and maps to "senior" since it can refer to
    either people managers (senior+) or non-management roles like "Account Manager" (mid).
    """
    lower = value.lower().strip()

    # Staff/Principal/Executive level (highest seniority)
    # These titles indicate staff-level or above: Director, VP, Chief, Head, Lead, Principal, Staff
    if any(token in lower for token in ("staff", "principal", "director", "vp", "chief", "head", "lead", "distinguished")):
        return "staff"

    # Senior level
    if any(token in lower for token in ("senior", "sr", "sr.")):
        return "senior"

    # Manager is ambiguous - could be "Engineering Manager" (senior+) or "Account Manager" (mid)
    # Map to senior as a reasonable middle ground
    if "manager" in lower:
        return "senior"

    # Junior level
    if any(token in lower for token in ("intern", "jr", "junior")):
        return "junior"

    # Mid level
    if "mid" in lower:
        return "mid"

    return ""


def _is_valid_level(value: str | None) -> tuple[bool, str]:
    """Validate a level value."""
    if not value:
        return False, "Empty level"

    normalized = _normalize_level(value)
    if normalized:
        return True, f"Valid level: {normalized}"

    return False, f"Unknown level: {value}"


class ExplicitLevelFieldStrategy(ExtractionStrategy[str]):
    """Extract level from explicit level field in raw data."""

    name = "explicit_level_field"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        raw_level = context.get_raw_field("level", "seniority", "experience_level", "jobLevel")

        if not raw_level:
            return self._make_skip_result("No level field in raw row")

        if not isinstance(raw_level, str):
            raw_level = str(raw_level)

        normalized = _normalize_level(raw_level)
        if normalized:
            return self._make_result(
                normalized,
                f"Explicit level field: {raw_level} -> {normalized}",
                is_valid=True,
                confidence=0.95,
                debug_info={"raw_value": raw_level},
            )

        return self._make_skip_result(f"Could not normalize level: {raw_level}")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_level(value)


class StructuredDataLevelStrategy(ExtractionStrategy[str]):
    """Extract level from structured data."""

    name = "structured_data_level"
    priority = StrategyPriority.STRUCTURED_DATA + 50

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        data = context.structured_data or context.json_payload
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        for key in ("level", "seniority", "experience_level", "experienceLevel", "jobLevel"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                normalized = _normalize_level(value)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"Structured data level: {value} -> {normalized}",
                        is_valid=True,
                        confidence=0.90,
                        debug_info={"key": key, "raw_value": value},
                    )

        return self._make_skip_result("No level key found in structured data")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_level(value)


class TitleLevelStrategy(ExtractionStrategy[str]):
    """Extract level from job title."""

    name = "title_level"
    priority = StrategyPriority.CONTENT_PATTERN

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        title = context.extracted_title
        if not title:
            title = context.get_raw_field("job_title", "title")

        if not title or not isinstance(title, str):
            return self._make_skip_result("No title available")

        title_lower = title.lower()

        # Check for level indicators in title
        match = _LEVEL_RE.search(title_lower)
        if match:
            level_text = match.group("level")
            normalized = _normalize_level(level_text)
            if normalized:
                return self._make_result(
                    normalized,
                    f"Level from title: '{level_text}' -> {normalized}",
                    is_valid=True,
                    confidence=0.85,
                    debug_info={"title": title, "match": level_text},
                )

        # Check for common title patterns - order matters!
        # Staff-level titles first (highest seniority)
        if any(token in title_lower for token in ("staff", "principal", "director", "vp", "chief", "head", "lead", "distinguished")):
            return self._make_result(
                "staff",
                f"Staff/Executive level in title: {title}",
                is_valid=True,
                confidence=0.85,
            )

        if any(token in title_lower for token in ("senior", "sr ", "sr.", "sr-", "sr/")):
            return self._make_result(
                "senior",
                f"Senior indicator in title: {title}",
                is_valid=True,
                confidence=0.85,
            )

        # Manager is ambiguous - map to senior as middle ground
        if "manager" in title_lower:
            return self._make_result(
                "senior",
                f"Manager role in title: {title}",
                is_valid=True,
                confidence=0.80,
            )

        if "intern" in title_lower:
            return self._make_result(
                "junior",
                f"Intern in title: {title}",
                is_valid=True,
                confidence=0.90,
            )

        if "jr" in title_lower or "junior" in title_lower:
            return self._make_result(
                "junior",
                f"Junior indicator in title: {title}",
                is_valid=True,
                confidence=0.85,
            )

        return self._make_skip_result("No level indicator in title")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_level(value)


class HintedLevelStrategy(ExtractionStrategy[str]):
    """Extract level from parse_markdown_hints() result."""

    name = "hinted_level"
    priority = StrategyPriority.HEURISTIC

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        level = context.hints.get("level")

        if not level:
            return self._make_skip_result("No level in hints")

        if not isinstance(level, str):
            return self._make_skip_result(f"Hint level is not a string: {type(level)}")

        normalized = _normalize_level(level)
        if normalized:
            return self._make_result(
                normalized,
                f"Level from hints: {level} -> {normalized}",
                is_valid=True,
                confidence=0.70,
            )

        return self._make_skip_result(f"Could not normalize hint level: {level}")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_level(value)


class ContentPatternLevelStrategy(ExtractionStrategy[str]):
    """Extract level from content patterns."""

    name = "content_pattern_level"
    priority = StrategyPriority.CONTENT_PATTERN + 50

    # Tokens that are commonly used as verbs in job descriptions and should be ignored
    # when found in content (but are still valid in titles where they indicate job level)
    _VERB_TOKENS = frozenset({"lead", "head", "manager"})

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        content = context.description_body or context.normalized_markdown
        if not content:
            return self._make_skip_result("No content available")

        # Look for level patterns in first part of content
        first_section = content[:500]

        match = _LEVEL_RE.search(first_section)
        if match:
            level_text = match.group("level")
            # Skip tokens that are commonly verbs when found in content
            # (e.g., "Lead analytics engineering efforts" - "lead" is a verb, not a title)
            if level_text.lower() in self._VERB_TOKENS:
                return self._make_skip_result(f"Skipping verb token in content: {level_text}")
            normalized = _normalize_level(level_text)
            if normalized:
                return self._make_result(
                    normalized,
                    f"Level from content: '{level_text}' -> {normalized}",
                    is_valid=True,
                    confidence=0.60,
                    debug_info={"match_position": match.start()},
                )

        return self._make_skip_result("No level pattern in content")

    def validate(self, value: str) -> tuple[bool, str]:
        return _is_valid_level(value)


class DefaultLevelStrategy(ExtractionStrategy[str]):
    """Default to 'mid' level if no other signal found."""

    name = "default_level"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        return self._make_result(
            "mid",
            "Default to mid level (no clear level signals found)",
            is_valid=True,
            confidence=0.30,
        )

    def validate(self, value: str) -> tuple[bool, str]:
        return True, "Valid default"


class LevelExtractor(FieldExtractor[str]):
    """
    Extracts job level using multiple strategies in priority order.

    Strategies (in order of priority):
    1. explicit_level_field (100) - From explicit level field
    2. structured_data_level (150) - From structured data
    3. title_level (500) - From job title patterns
    4. content_pattern_level (550) - From content patterns
    5. hinted_level (600) - From parse_markdown_hints()
    6. default_level (900) - Default to 'mid'

    Returns one of: intern, junior, mid, senior, staff
    """

    field_name = "level"

    def _register_strategies(self) -> list[ExtractionStrategy[str]]:
        return [
            ExplicitLevelFieldStrategy(),
            StructuredDataLevelStrategy(),
            TitleLevelStrategy(),
            ContentPatternLevelStrategy(),
            HintedLevelStrategy(),
            DefaultLevelStrategy(),
        ]
