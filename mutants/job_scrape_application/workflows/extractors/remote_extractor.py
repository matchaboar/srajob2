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


class SchemaOrgRemoteStrategy(ExtractionStrategy[bool]):
    """Detect remote from Schema.org jobLocationType in content.

    Schema.org defines jobLocationType values:
    - TELECOMMUTE: Remote/work-from-home
    This is authoritative structured data that should be trusted.
    """

    name = "schema_org_remote"
    priority = StrategyPriority.STRUCTURED_DATA + 10  # Just after explicit_remote_flag

    # Pattern to find jobLocationType in Schema.org JSON-LD
    _JOB_LOCATION_TYPE_RE = re.compile(
        r'"jobLocationType"\s*:\s*"([^"]+)"',
        re.IGNORECASE,
    )

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        location_type = self._extract_location_type(context)
        if not location_type:
            return self._make_skip_result("No jobLocationType found in Schema.org data")

        if location_type != "TELECOMMUTE":
            # Other jobLocationType values don't indicate non-remote
            return self._make_skip_result(f"jobLocationType '{location_type}' is not TELECOMMUTE")

        if not context.structured_data:
            return self._make_result(
                True,
                "Schema.org jobLocationType is TELECOMMUTE",
                is_valid=True,
                confidence=0.95,  # High confidence - this is authoritative data
            )

        structured = context.structured_data if isinstance(context.structured_data, dict) else None
        if not structured:
            return self._make_result(
                True,
                "Schema.org jobLocationType is TELECOMMUTE",
                is_valid=True,
                confidence=0.90,
            )

        job_location = structured.get("jobLocation")
        if self._is_multi_location(job_location):
            return self._make_result(
                True,
                "Schema.org TELECOMMUTE with multiple locations",
                is_valid=True,
                confidence=0.90,
            )

        if self._location_mentions_remote(job_location):
            return self._make_result(
                True,
                "Schema.org TELECOMMUTE with remote location text",
                is_valid=True,
                confidence=0.92,
            )

        return self._make_result(
            True,
            "Schema.org jobLocationType is TELECOMMUTE",
            is_valid=True,
            confidence=0.85,
        )

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"

    def _extract_location_type(self, context: ExtractionContext) -> str | None:
        if context.structured_data:
            raw_value = context.structured_data.get("jobLocationType")
            if isinstance(raw_value, list) and raw_value:
                raw_value = raw_value[0]
            if isinstance(raw_value, str) and raw_value.strip():
                return raw_value.strip().upper()

        content = context.raw_markdown or context.normalized_markdown
        if not content:
            return None

        normalized_content = content.replace('\\"', '"')
        match = self._JOB_LOCATION_TYPE_RE.search(normalized_content)
        if not match:
            return None

        return match.group(1).upper()

    def _location_mentions_remote(self, job_location: object) -> bool:
        for text in self._iter_location_text(job_location):
            if isinstance(text, str) and "remote" in text.lower():
                return True
        return False

    def _is_multi_location(self, job_location: object) -> bool:
        if isinstance(job_location, list):
            if len(job_location) > 1:
                return True
            if job_location:
                return self._is_multi_location(job_location[0])

        if isinstance(job_location, dict):
            address = job_location.get("address")
            if isinstance(address, dict):
                locality = address.get("addressLocality")
                if isinstance(locality, str) and self._looks_like_multi_locality(
                    locality,
                    address.get("addressRegion"),
                    address.get("addressCountry"),
                ):
                    return True
        return False

    def _looks_like_multi_locality(
        self,
        locality: str,
        address_region: object,
        address_country: object,
    ) -> bool:
        if "," not in locality:
            return False

        parts = [part.strip() for part in locality.split(",") if part.strip()]
        if len(parts) <= 1:
            return False

        region = address_region.strip() if isinstance(address_region, str) else ""
        country = address_country.strip() if isinstance(address_country, str) else ""
        for part in parts[1:]:
            if part and part in {region, country}:
                continue
            if re.fullmatch(r"[A-Z]{2,3}", part):
                continue
            return True
        return False

    def _iter_location_text(self, job_location: object) -> list[str]:
        texts: list[str] = []
        if isinstance(job_location, str):
            return [job_location]
        if isinstance(job_location, list):
            for item in job_location:
                texts.extend(self._iter_location_text(item))
            return texts
        if isinstance(job_location, dict):
            name = job_location.get("name")
            if isinstance(name, str):
                texts.append(name)
            address = job_location.get("address")
            if isinstance(address, dict):
                for key in ("addressLocality", "addressRegion", "addressCountry"):
                    value = address.get(key)
                    if isinstance(value, str):
                        texts.append(value)
        return texts


class GreenhouseMetadataRemoteStrategy(ExtractionStrategy[bool]):
    """Detect remote from Greenhouse metadata 'Workplace Type' field.

    Greenhouse API includes metadata array with structured info like:
    {"name": "Workplace Type", "value": "Remote"} or "Hybrid"
    """

    name = "greenhouse_metadata_remote"
    priority = StrategyPriority.STRUCTURED_DATA + 20  # After Schema.org

    # Pattern to find Workplace Type in Greenhouse metadata
    _WORKPLACE_TYPE_RE = re.compile(
        r'"name"\s*:\s*"Workplace\s+Type"\s*,\s*"value"\s*:\s*"([^"]+)"',
        re.IGNORECASE,
    )

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        workplace_type = self._extract_workplace_type(context)
        if not workplace_type:
            return self._make_skip_result("No Workplace Type found in Greenhouse metadata")

        workplace_type_lower = workplace_type.lower()

        if "hybrid" in workplace_type_lower:
            return self._make_skip_result(
                f"Greenhouse Workplace Type is '{workplace_type}' (hybrid)"
            )

        if "onsite" in workplace_type_lower or "on-site" in workplace_type_lower:
            return self._make_skip_result(
                f"Greenhouse Workplace Type is '{workplace_type}' (onsite)"
            )

        if "remote" in workplace_type_lower:
            return self._make_result(
                True,
                f"Greenhouse Workplace Type is '{workplace_type}'",
                is_valid=True,
                confidence=0.90,  # High confidence - explicit metadata
            )

        return self._make_skip_result(f"Workplace Type '{workplace_type}' is ambiguous")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"

    def _extract_workplace_type(self, context: ExtractionContext) -> str | None:
        for source in (context.get_raw_field("metadata"), context.structured_data):
            metadata = None
            if isinstance(source, dict):
                metadata = source.get("metadata")
            elif isinstance(source, list):
                metadata = source
            if isinstance(metadata, list):
                for item in metadata:
                    if not isinstance(item, dict):
                        continue
                    name = item.get("name")
                    if isinstance(name, str) and name.strip().lower() == "workplace type":
                        value = item.get("value")
                        if isinstance(value, str) and value.strip():
                            return value.strip()

        for key in ("workplaceType", "workplace_type"):
            raw_value = context.get_raw_field(key)
            if isinstance(raw_value, str) and raw_value.strip():
                return raw_value.strip()

        content = context.raw_markdown or context.normalized_markdown
        if not content:
            return None

        match = self._WORKPLACE_TYPE_RE.search(content)
        if not match:
            return None

        return match.group(1).strip()


class LocationRemoteStrategy(ExtractionStrategy[bool]):
    """Detect remote from location field containing 'remote'.

    NOTE: This strategy only uses location for POSITIVE remote inference.
    Having a specific physical location does NOT mean the job is not remote -
    many remote-friendly companies have offices and list physical locations
    for jobs that can also be done remotely (hybrid).
    """

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

        # Don't infer remote=False from specific locations - many hybrid jobs
        # have physical locations but still allow remote work
        return self._make_skip_result(
            f"Location '{location}' present but not inferring remote status"
        )

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
    """Extract remote from parse_markdown_hints() result.

    NOTE: This strategy only uses hints for POSITIVE remote inference (True).
    When hints say remote=False, we skip because this is often inferred from
    physical location presence, which is unreliable - many hybrid jobs have
    physical locations but still allow remote work.
    """

    name = "hinted_remote"
    priority = StrategyPriority.HEURISTIC

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        remote = context.hints.get("remote")

        if remote is None:
            return self._make_skip_result("No remote in hints")

        if isinstance(remote, bool):
            if remote:
                location_hint = context.hints.get("location")
                location_list = context.hints.get("locations")
                if (
                    isinstance(location_hint, str)
                    and "remote" not in location_hint.lower()
                    and isinstance(location_list, list)
                ):
                    has_remote = any(
                        isinstance(loc, str) and "remote" in loc.lower()
                        for loc in location_list
                    )
                    has_non_remote = any(
                        isinstance(loc, str) and "remote" not in loc.lower()
                        for loc in location_list
                    )
                    if has_remote and has_non_remote:
                        return self._make_skip_result(
                            "Skipping remote hint due to mixed remote/non-remote locations"
                        )
                return self._make_result(
                    True,
                    f"Remote from hints: {remote}",
                    is_valid=True,
                    confidence=0.70,
                )
            # Skip False hints - they're often inferred from physical location
            # presence which is unreliable for hybrid jobs
            return self._make_skip_result(
                "Skipping hints remote=False (unreliable inference from location)"
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
            # Skip "false", "no", "onsite" - unreliable negative inference
            if lowered in {"false", "no", "onsite", "0"}:
                return self._make_skip_result(
                    f"Skipping negative hint '{remote}' (unreliable)"
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

    # Pattern to extract company_name from JSON in markdown
    # Handles escaped underscores in markdown (company\_name or company\\_name)
    _COMPANY_NAME_RE = re.compile(
        r'"company(?:\\+_|_)name"\s*:\s*"([^"]+)"',
        re.IGNORECASE,
    )

    def extract(self, context: ExtractionContext) -> StrategyResult[bool]:
        workplace_type = self._extract_workplace_type(context)
        workplace_note = ""
        if workplace_type:
            workplace_lower = workplace_type.lower()
            if _HYBRID_RE.search(workplace_lower) or _ONSITE_RE.search(workplace_lower):
                workplace_note = f" (workplace type '{workplace_type}')"

        company = context.extracted_company
        if not company:
            company = context.get_raw_field("company", "company_name")

        # Try to extract from JSON in markdown (for Greenhouse API responses)
        # Do this before hints because hints can have false positives
        if not company and context.raw_markdown:
            match = self._COMPANY_NAME_RE.search(context.raw_markdown)
            if match:
                company = match.group(1)

        # Also check seed_hints for company
        if not company:
            company = context.seed_hints.get("company")

        # Check hints for company (lower priority due to potential false positives)
        if not company:
            company = context.hints.get("company")

        if not company or not isinstance(company, str):
            return self._make_skip_result("No company available")

        # Check if company is known to be remote-first
        from ...constants import is_remote_company

        try:
            if is_remote_company(company):
                return self._make_result(
                    True,
                    f"Company '{company}' is known remote-first{workplace_note}",
                    is_valid=True,
                    confidence=0.70 if workplace_note else 0.75,
                )
        except Exception:
            pass

        return self._make_skip_result(f"Company '{company}' not in remote company list")

    def validate(self, value: bool) -> tuple[bool, str]:
        return True, "Valid boolean"

    def _extract_workplace_type(self, context: ExtractionContext) -> str | None:
        for source in (context.get_raw_field("metadata"), context.structured_data):
            metadata = None
            if isinstance(source, dict):
                metadata = source.get("metadata")
            elif isinstance(source, list):
                metadata = source
            if isinstance(metadata, list):
                for item in metadata:
                    if not isinstance(item, dict):
                        continue
                    name = item.get("name")
                    if isinstance(name, str) and name.strip().lower() == "workplace type":
                        value = item.get("value")
                        if isinstance(value, str) and value.strip():
                            return value.strip()

        for key in ("workplaceType", "workplace_type"):
            raw_value = context.get_raw_field(key)
            if isinstance(raw_value, str) and raw_value.strip():
                return raw_value.strip()

        content = context.raw_markdown or context.normalized_markdown
        if not content:
            return None

        match = GreenhouseMetadataRemoteStrategy._WORKPLACE_TYPE_RE.search(content)
        if not match:
            return None

        return match.group(1).strip()


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
    2. schema_org_remote (110) - From Schema.org jobLocationType
    3. greenhouse_metadata_remote (120) - From Greenhouse Workplace Type
    4. location_remote (300) - From location containing 'remote'
    5. title_remote (500) - From title containing 'remote'
    6. content_remote_pattern (550) - From content patterns
    7. hinted_remote (600) - From parse_markdown_hints()
    8. remote_company (650) - From known remote-first companies
    9. default_remote (900) - Default to False
    """

    field_name = "remote"

    def _register_strategies(self) -> list[ExtractionStrategy[bool]]:
        return [
            ExplicitRemoteFlagStrategy(),
            SchemaOrgRemoteStrategy(),
            GreenhouseMetadataRemoteStrategy(),
            LocationRemoteStrategy(),
            TitleRemoteStrategy(),
            ContentRemotePatternStrategy(),
            HintedRemoteStrategy(),
            RemoteCompanyStrategy(),
            DefaultRemoteStrategy(),
        ]
