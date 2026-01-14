"""
Remote status extraction strategies and extractor.
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


# Remote detection patterns
_REMOTE_RE = re.compile(r"\b(remote(-first)?|fully\s+remote)\b", re.IGNORECASE)
_HYBRID_RE = re.compile(r"\bhybrid\b", re.IGNORECASE)
_ONSITE_RE = re.compile(r"\b(on-?site|in-?office|office-based)\b", re.IGNORECASE)


class ExplicitRemoteFlagStrategy(ExtractionStrategy[bool]):
    """Extract remote status from explicit remote field in raw data."""

    name = "explicit_remote_flag"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        raw_remote = context.get_raw_field("remote", "is_remote", "isRemote", "remoteAllowed")

        if raw_remote is None:
            return self._make_skip_result("No remote field in raw row")

        # Handle boolean values
        if isinstance(raw_remote, bool):
            return self._make_result(
                raw_remote,
                f"Explicit boolean remote={raw_remote}",
                is_valid=True,
                confidence=0.95,
            )

        # Handle string values
        if isinstance(raw_remote, str):
            lowered = raw_remote.lower().strip()
            if lowered in {"true", "yes", "remote", "hybrid", "fully remote", "1"}:
                return self._make_result(
                    True,
                    f"Remote string value: {raw_remote}",
                    is_valid=True,
                    confidence=0.90,
                )
            if lowered in {"false", "no", "onsite", "on-site", "office", "0"}:
                return self._make_result(
                    False,
                    f"Non-remote string value: {raw_remote}",
                    is_valid=True,
                    confidence=0.90,
                )

        return self._make_skip_result(f"Could not parse remote value: {raw_remote}")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"


class LocationRemoteStrategy(ExtractionStrategy[bool]):
    """Detect remote from location field containing 'remote'."""

    name = "location_remote"
    priority = StrategyPriority.EXPLICIT_FIELD

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        # Check extracted location
        location = context.extracted_location
        if not location:
            # Try raw row location
            location = context.get_raw_field("location", "city")
            if isinstance(location, dict):
                location = location.get("name") or location.get("location")

        if not location or not isinstance(location, str):
            return self._make_skip_result("No location available")

        loc_lower = location.lower()

        # "Remote" in location is a strong signal
        if "remote" in loc_lower:
            return self._make_result(
                True,
                f"Location contains 'remote': {location}",
                is_valid=True,
                confidence=0.95,
            )

        # If location is a specific place (not "Unknown"), it's likely not remote
        if loc_lower not in {"unknown", "various", "multiple", "anywhere"}:
            return self._make_result(
                False,
                f"Location is specific place: {location}",
                is_valid=True,
                confidence=0.70,
            )

        return self._make_skip_result("Location ambiguous for remote detection")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"


class TitleRemoteStrategy(ExtractionStrategy[bool]):
    """Detect remote from job title containing 'remote'."""

    name = "title_remote"
    priority = StrategyPriority.CONTENT_PATTERN

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        # Check extracted title
        title = context.extracted_title
        if not title:
            title = context.get_raw_field("job_title", "title")

        if not title or not isinstance(title, str):
            return self._make_skip_result("No title available")

        title_lower = title.lower()

        if _REMOTE_RE.search(title_lower):
            return self._make_result(
                True,
                f"Title contains remote keyword: {title}",
                is_valid=True,
                confidence=0.85,
            )

        return self._make_skip_result("No remote keyword in title")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"


class HintedRemoteStrategy(ExtractionStrategy[bool]):
    """Extract remote from parse_markdown_hints() result."""

    name = "hinted_remote"
    priority = StrategyPriority.HEURISTIC

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        remote = context.hints.get("remote")

        if remote is None:
            return self._make_skip_result("No remote in hints")

        if isinstance(remote, bool):
            return self._make_result(
                remote,
                f"Remote from hints: {remote}",
                is_valid=True,
                confidence=0.70,
            )

        if isinstance(remote, str):
            lowered = remote.lower()
            if lowered in {"true", "yes", "remote", "1"}:
                return self._make_result(
                    True,
                    f"Remote string hint: {remote}",
                    is_valid=True,
                    confidence=0.65,
                )
            if lowered in {"false", "no", "onsite", "0"}:
                return self._make_result(
                    False,
                    f"Non-remote string hint: {remote}",
                    is_valid=True,
                    confidence=0.65,
                )

        return self._make_skip_result(f"Could not parse hint remote: {remote}")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"


class ContentRemotePatternStrategy(ExtractionStrategy[bool]):
    """Detect remote from content patterns (less reliable)."""

    name = "content_remote_pattern"
    priority = StrategyPriority.CONTENT_PATTERN + 50  # Lower priority than title

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        content = context.description_body or context.normalized_markdown
        if not content:
            return self._make_skip_result("No content available")

        # Look for remote patterns in first part of content (more reliable)
        first_section = content[:1000]

        # Check for explicit remote mentions
        remote_matches = list(_REMOTE_RE.finditer(first_section))
        if remote_matches:
            return self._make_result(
                True,
                f"Content contains remote pattern at position {remote_matches[0].start()}",
                is_valid=True,
                confidence=0.60,
                debug_info={"match": remote_matches[0].group()},
            )

        # Note: We don't infer non-remote from lack of remote keywords
        # as many jobs don't explicitly state their remote policy
        return self._make_skip_result("No clear remote pattern in content")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"


class RemoteCompanyStrategy(ExtractionStrategy[bool]):
    """Check if company is known to be remote-first."""

    name = "remote_company"
    priority = StrategyPriority.HEURISTIC + 50

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        company = context.extracted_company
        if not company:
            company = context.get_raw_field("company", "company_name")

        if not company or not isinstance(company, str):
            return self._make_skip_result("No company available")

        # Check if company is known to be remote-first
        from ...constants import is_remote_company

        try:
            if is_remote_company(company):
                return self._make_result(
                    True,
                    f"Company '{company}' is known remote-first",
                    is_valid=True,
                    confidence=0.75,
                )
        except Exception:
            pass

        return self._make_skip_result(f"Company '{company}' not in remote company list")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"


class DefaultRemoteStrategy(ExtractionStrategy[bool]):
    """Default to False (not remote) if no other signal found."""

    name = "default_remote"
    priority = StrategyPriority.FALLBACK

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        return self._make_result(
            False,
            "Default to non-remote (no clear remote signals found)",
            is_valid=True,
            confidence=0.30,
        )

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid default"


class RemoteExtractor(FieldExtractor[bool]):
    """
    Extracts remote status using multiple strategies in priority order.

    Strategies (in order of priority):
    1. explicit_remote_flag (100) - From explicit remote field
    2. location_remote (300) - From location containing 'remote'
    3. title_remote (500) - From title containing 'remote'
    4. content_remote_pattern (550) - From content patterns
    5. hinted_remote (600) - From parse_markdown_hints()
    6. remote_company (650) - From known remote-first companies
    7. default_remote (900) - Default to False
    """

    field_name = "remote"

    def _register_strategies(self) -> list[ExtractionStrategy[bool]]:
        return [
            ExplicitRemoteFlagStrategy(),
            LocationRemoteStrategy(),
            TitleRemoteStrategy(),
            ContentRemotePatternStrategy(),
            HintedRemoteStrategy(),
            RemoteCompanyStrategy(),
            DefaultRemoteStrategy(),
        ]
