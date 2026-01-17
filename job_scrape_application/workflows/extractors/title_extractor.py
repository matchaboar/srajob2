"""
Title extraction strategies and extractor.
"""

from __future__ import annotations

import html
import orjson
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
        "description & requirements",
        "description &amp; requirements",
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
        "open positions",
        "all open positions",
        "openings",
        "all openings",
        "role",
        "apply now",
        "apply",
        "home",
        "homepage",
        "requirements",
        "qualifications",
        "responsibilities",
    }
)

# Markdown heading pattern
_HEADING_RE = re.compile(r"^[ \t]*#{1,3}\s+(?P<title>.+)$", re.MULTILINE)

# Location suffixes that should be stripped from job titles when they appear after " - "
# e.g., "Software Engineer - San Francisco" -> "Software Engineer"
# This is a conservative list of common patterns that clearly indicate location, not product/team names
_LOCATION_SUFFIX_PATTERNS = frozenset({
    # US cities commonly seen in job titles
    "san francisco", "new york", "los angeles", "seattle", "austin", "boston",
    "chicago", "denver", "atlanta", "miami", "dallas", "houston", "phoenix",
    "portland", "san diego", "san jose", "washington dc", "dc",
    # Other major cities
    "london", "toronto", "vancouver", "sydney", "melbourne", "dublin",
    "amsterdam", "berlin", "paris", "munich", "zurich", "tel aviv",
    # US state abbreviations
    "ca", "ny", "wa", "tx", "ma", "co", "il", "ga", "fl",
    # Generic location indicators
    "remote", "hybrid", "onsite", "on-site", "on site",
})

# Company name patterns that should be stripped from job titles
# e.g., "Product Manager - Acme Corp" -> "Product Manager"
_COMPANY_SUFFIX_INDICATORS = frozenset({
    "inc", "inc.", "llc", "ltd", "corp", "corporation", "company", "co",
    "limited", "gmbh", "ag", "sa", "plc",
})

# Pattern to match " at Company Name" or " @ Company Name" suffix at the end of titles
# Matches: "Software Engineer at Acme Corp", "PM at Palo Alto Networks", "Engineer @ Google"
# Does NOT match: "Engineer at scale" (lowercase company), "Work at home" (generic phrase)
_AT_COMPANY_SUFFIX_RE = re.compile(
    r"\s+(?:at|@)\s+(?P<company>[A-Z][A-Za-z0-9](?:[A-Za-z0-9 ]*[A-Za-z0-9])?)$"
)

_JOB_ID_COMPANY_SUFFIX_RE = re.compile(
    r"\s*[-–—]\s*(?P<job_id>\d{4,})\s*[-–—]\s*(?P<company>.+?)\s*$"
)


_CODE_BLOCK_RE = re.compile(r"```(?:json)?\s*(?P<blob>.*?)```", re.DOTALL | re.IGNORECASE)
_PRE_TAG_RE = re.compile(r"<pre>(?P<blob>.*?)</pre>", re.DOTALL | re.IGNORECASE)


def _load_json_blob(blob: str) -> dict | None:
    if not blob:
        return None
    try:
        parsed = orjson.loads(blob)
    except orjson.JSONDecodeError:
        try:
            parsed = orjson.loads(blob.replace("\\_", "_"))
        except orjson.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


def _extract_json_from_text(text: str) -> dict | None:
    if not text or not isinstance(text, str):
        return None
    code_match = _CODE_BLOCK_RE.search(text)
    if code_match:
        parsed = _load_json_blob(code_match.group("blob"))
        if parsed:
            return parsed
    pre_match = _PRE_TAG_RE.search(text)
    if pre_match:
        parsed = _load_json_blob(pre_match.group("blob"))
        if parsed:
            return parsed
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        return _load_json_blob(text[start : end + 1])
    return None


def _strip_at_company_suffix(title: str) -> str:
    """Strip ' at Company Name' suffix from titles.

    Handles patterns like:
    - "Software Engineer at Palo Alto Networks" -> "Software Engineer"
    - "PM at Acme Corp" -> "PM"

    But preserves legitimate uses like:
    - "Engineer at scale" (lowercase)
    - "Look at our team" (generic phrase)

    Args:
        title: The title string to clean

    Returns:
        The cleaned title without the company suffix
    """
    if not title:
        return title

    match = _AT_COMPANY_SUFFIX_RE.search(title)
    if match:
        return title[:match.start()].strip()

    return title


def _strip_location_suffix(title: str) -> str:
    """Strip location suffixes like " - San Francisco" when clearly a location."""
    if not title or " - " not in title:
        return title

    prefix, suffix = title.rsplit(" - ", 1)
    if _is_strippable_title_suffix(suffix):
        return prefix.strip()

    return title


def _is_strippable_title_suffix(suffix: str) -> bool:
    """Check if a title suffix should be stripped.

    Only strip suffixes that are clearly locations or company name patterns.
    Product/team names like "Flink SQL" or "Kora Storage" should be preserved.

    Args:
        suffix: The suffix part after " - " in a title

    Returns:
        True if the suffix should be stripped, False if it should be kept
    """
    suffix_lower = suffix.strip().lower()

    # Check for exact location matches
    if suffix_lower in _LOCATION_SUFFIX_PATTERNS:
        return True

    # Check for company name indicators (e.g., "Acme Inc", "Tech Corp")
    suffix_words = suffix_lower.split()
    if suffix_words and suffix_words[-1] in _COMPANY_SUFFIX_INDICATORS:
        return True

    # Default: keep the suffix (it's likely a product/team name)
    return False


def _normalize_whitespace(value: str) -> str:
    """Normalize whitespace in a string: strip and collapse multiple spaces."""
    # Strip leading/trailing whitespace and collapse internal whitespace
    return " ".join(value.split())


def _normalize_company_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _strip_job_id_company_suffix(title: str, company_hint: str | None) -> str:
    """Strip trailing job ID + company suffix like " - 16762 - Bloomberg"."""
    if not title:
        return title
    match = _JOB_ID_COMPANY_SUFFIX_RE.search(title)
    if not match:
        return title
    if not company_hint:
        return title
    suffix_company = match.group("company").strip()
    normalized_suffix = _normalize_company_key(suffix_company)
    normalized_hint = _normalize_company_key(company_hint)
    if not normalized_suffix or not normalized_hint:
        return title
    if normalized_suffix not in normalized_hint and normalized_hint not in normalized_suffix:
        return title
    return title[: match.start()].strip()


def _is_valid_title(value: str | None) -> tuple[bool, str]:
    """Validate a title value."""
    if not value:
        return False, "Empty title"

    # Normalize whitespace: strip and collapse multiple spaces
    value = _normalize_whitespace(value)
    if len(value) < 3:
        return False, f"Title too short: {len(value)} chars"

    if len(value) > 200:
        return False, f"Title too long: {len(value)} chars"

    # Check for generic/invalid titles
    # Strip trailing punctuation (colon, period, etc.) before comparison
    lower = value.lower().strip().rstrip(":.")
    if lower in _INVALID_TITLES:
        return False, f"Generic title rejected: {value}"

    # Check for URL-as-title
    if lower.startswith(("http://", "https://", "www.")):
        return False, "URL as title rejected"

    # Check for markdown artifacts
    if value.startswith(("#", "[", "*", "`")):
        # These are likely leftover markdown
        if re.match(r"^[#\[\]*`]+\s*$", value):
            return False, "Markdown artifact rejected"

    # Reject code fence markers (```) and similar
    if value.strip() in ("```", "```json", "```html", "```xml", "---"):
        return False, "Markdown code fence rejected"

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
            data = _extract_json_from_text(context.raw_markdown or context.raw_html)
        if not isinstance(data, dict):
            return self._make_skip_result("No structured data available")

        candidates = [data]
        job_posting_info = data.get("jobPostingInfo")
        if isinstance(job_posting_info, dict):
            candidates.append(job_posting_info)

        # Try common title keys in priority order
        for candidate in candidates:
            for key in ("title", "name", "jobTitle", "job_title", "positionTitle", "position"):
                value = candidate.get(key)
                if isinstance(value, str) and value.strip():
                    cleaned = _normalize_whitespace(html.unescape(value))
                    # Clean up " | Location | Company" patterns (e.g., Netflix)
                    if " | " in cleaned:
                        cleaned = cleaned.split(" | ", 1)[0].strip()
                    cleaned = _strip_location_suffix(cleaned)
                    # Strip " at Company Name" suffix (e.g., "PM at Palo Alto Networks")
                    cleaned = _strip_at_company_suffix(cleaned)
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

        # Normalize whitespace in title
        handler_title = _normalize_whitespace(handler_title)
        handler_title = _strip_location_suffix(handler_title)
        # Strip " at Company Name" suffix (e.g., "PM at Palo Alto Networks")
        handler_title = _strip_at_company_suffix(handler_title)

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

        cleaned = normalize_title_from_bar(_normalize_whitespace(raw_title))
        if not cleaned:
            cleaned = _normalize_whitespace(raw_title)
        cleaned = _strip_location_suffix(cleaned)
        # Strip " at Company Name" suffix (e.g., "PM at Palo Alto Networks")
        cleaned = _strip_at_company_suffix(cleaned)
        company_hint = context.get_raw_field(
            "company", "company_name", "employer", "organization"
        )
        if not company_hint:
            hint_company = context.hints.get("company")
            if isinstance(hint_company, str):
                company_hint = hint_company
        cleaned = _strip_job_id_company_suffix(cleaned, company_hint)

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

        # Clean up: remove trailing " | Location" pattern (always strip after pipe)
        if " | " in raw_title:
            raw_title = raw_title.split(" | ", 1)[0].strip()
        else:
            raw_title = _strip_location_suffix(raw_title)
        # Strip " at Company Name" suffix (e.g., "PM at Palo Alto Networks")
        raw_title = _strip_at_company_suffix(raw_title)

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
