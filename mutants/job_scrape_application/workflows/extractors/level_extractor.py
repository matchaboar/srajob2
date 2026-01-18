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
_ACCOUNT_MANAGER_RE = re.compile(
    r"\b(?:named\s+)?account manager\b",
    re.IGNORECASE,
)
_ACCOUNT_MANAGER_SENIOR_TOKENS = (
    "senior",
    "sr",
    "principal",
    "staff",
    "director",
    "vp",
    "chief",
    "head",
    "lead",
    "distinguished",
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
            title = context.extracted_title or context.get_raw_field("job_title", "title")
            if (
                normalized == "senior"
                and isinstance(title, str)
                and _ACCOUNT_MANAGER_RE.search(title.lower())
                and not any(token in title.lower() for token in _ACCOUNT_MANAGER_SENIOR_TOKENS)
            ):
                return self._make_skip_result(
                    "Skipping explicit senior level for account manager title"
                )
            if normalized == "mid":
                return self._make_skip_result(
                    f"Skipping mid-level default: {raw_level}"
                )
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
    _STAFF_TOKENS = (
        "staff",
        "principal",
        "director",
        "vp",
        "chief",
        "head",
        "lead",
        "leader",
        "distinguished",
    )
    _LEADER_RE = re.compile(r"\bleader\b", re.IGNORECASE)
    _ACCOUNT_MANAGER_PATTERNS = (
        re.compile(r"\bnamed account manager\b", re.IGNORECASE),
        re.compile(r"\baccount manager\b", re.IGNORECASE),
    )

    def _is_valid_title(self, title: str | None) -> bool:
        """Check if a title value is valid (not None, not HTML, not empty)."""
        if not title or not isinstance(title, str):
            return False
        stripped = title.strip()
        # Skip if title looks like HTML
        if stripped.startswith("<") and ">" in stripped:
            return False
        # Skip if title is just whitespace/special chars after stripping tags
        if len(stripped) < 3:
            return False
        return True

    def _has_word_token(self, title_lower: str, tokens: tuple[str, ...]) -> bool:
        return any(re.search(rf"\b{re.escape(token)}\b", title_lower) for token in tokens)

    def _is_account_manager_title(self, title_lower: str) -> bool:
        if not any(pattern.search(title_lower) for pattern in self._ACCOUNT_MANAGER_PATTERNS):
            return False
        if self._has_word_token(
            title_lower,
            ("senior", "sr", "principal", "staff", "director", "vp", "chief", "head", "lead", "distinguished"),
        ):
            return False
        return True

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        # Try multiple sources for the job title, in order of reliability
        title = context.extracted_title
        if not self._is_valid_title(title):
            title = context.handler_extracted_title
        if not self._is_valid_title(title):
            title = context.hints.get("title")
        if not self._is_valid_title(title):
            title = context.get_raw_field("job_title", "title")

        if not self._is_valid_title(title):
            return self._make_skip_result("No title available")

        title_lower = title.lower()

        if self._is_account_manager_title(title_lower):
            return self._make_result(
                "mid",
                f"Account manager title maps to mid level: {title}",
                is_valid=True,
                confidence=0.80,
            )

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
        if self._has_word_token(title_lower, self._STAFF_TOKENS):
            return self._make_result(
                "staff",
                f"Staff/Executive level in title: {title}",
                is_valid=True,
                confidence=0.85,
            )

        if self._LEADER_RE.search(title_lower):
            return self._make_result(
                "senior",
                f"Leader in title: {title}",
                is_valid=True,
                confidence=0.80,
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

    # Phrases that indicate the level indicator describes someone else's role, not this job
    # e.g., "Reporting to the Senior Manager" - Senior describes the manager, not this job
    _SKIP_CONTEXT_PHRASES = (
        "reporting to",
        "report to",
        "reports to",
        "work with",
        "works with",
        "working with",
        "collaborate with",
        "partner with",
        "alongside",
        "under the",
    )

    _MINIMUM_YEARS_RE = re.compile(
        r"(?:minimum of|at least|atleast|min(?:imum)?)\s+(?P<years>\d{1,2})\s*(?:years?|yrs?)",
        re.IGNORECASE,
    )
    _RANGE_YEARS_RE = re.compile(
        r"(?P<min>\d{1,2})\s*[-–]\s*(?P<max>\d{1,2})\s*(?:years?|yrs?)",
        re.IGNORECASE,
    )
    _PLUS_YEARS_RE = re.compile(
        r"(?P<years>\d{1,2})\s*\+?\s*(?:years?|yrs?)\s+(?:of\s+)?(?:professional\s+|work\s+|relevant\s+)?experience",
        re.IGNORECASE,
    )

    def _is_describing_other_role(self, content: str, match_start: int) -> bool:
        """Check if the level match describes someone else's role rather than this job."""
        # Look at the 50 characters before the match
        prefix_start = max(0, match_start - 50)
        prefix = content[prefix_start:match_start].lower()

        for phrase in self._SKIP_CONTEXT_PHRASES:
            if phrase in prefix:
                return True
        return False

    def _level_from_years(self, years: int) -> str | None:
        if 3 <= years <= 5:
            return "mid"
        if years >= 6:
            return "senior"
        return None

    def _extract_experience_years(self, content: str) -> int | None:
        for match in self._MINIMUM_YEARS_RE.finditer(content):
            return int(match.group("years"))

        for match in self._RANGE_YEARS_RE.finditer(content):
            window = content[max(0, match.start() - 20):match.end() + 40].lower()
            if "experience" in window or "exp" in window:
                return int(match.group("min"))

        for match in self._PLUS_YEARS_RE.finditer(content):
            return int(match.group("years"))

        return None

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
                match = None
            else:
                # Skip if this level indicator describes someone else's role
                # (e.g., "Reporting to the Senior Manager" - Senior describes the manager)
                if self._is_describing_other_role(first_section, match.start()):
                    match = None

            if match:
                normalized = _normalize_level(level_text)
                if normalized:
                    return self._make_result(
                        normalized,
                        f"Level from content: '{level_text}' -> {normalized}",
                        is_valid=True,
                        confidence=0.60,
                        debug_info={"match_position": match.start()},
                    )

        experience_years = self._extract_experience_years(content)
        if experience_years is not None:
            normalized = self._level_from_years(experience_years)
            if normalized:
                return self._make_result(
                    normalized,
                    f"Level from experience: {experience_years}+ years -> {normalized}",
                    is_valid=True,
                    confidence=0.55,
                    debug_info={"years": experience_years},
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
