"""Extract step: Pull field values from parsed content.

This step uses the modular extractor system to extract job fields
from parsed content. Each field has specialized extraction strategies.
"""

from __future__ import annotations

import logging
from typing import Any

from ..types import ParsedContent, ExtractedFields
from ...extractors.context import ExtractionContext
from ...extractors.base import FieldExtractor
from ...extractors.title_extractor import JobTitleExtractor
from ...extractors.company_extractor import CompanyExtractor
from ...extractors.location_extractor import LocationExtractor
from ...extractors.remote_extractor import RemoteExtractor
from ...extractors.level_extractor import LevelExtractor
from ...extractors.compensation_extractor import CompensationExtractor
from ...extractors.posted_at_extractor import PostedAtExtractor
from ...extractors.description_extractor import DescriptionExtractor

logger = logging.getLogger(__name__)

# Extractor registry
_EXTRACTORS: dict[str, type[FieldExtractor]] = {
    "title": JobTitleExtractor,
    "company": CompanyExtractor,
    "location": LocationExtractor,
    "remote": RemoteExtractor,
    "level": LevelExtractor,
    "compensation": CompensationExtractor,
    "posted_at": PostedAtExtractor,
    "description": DescriptionExtractor,
}

# Singleton instances
_extractor_instances: dict[str, FieldExtractor] | None = None


def _get_extractors() -> dict[str, FieldExtractor]:
    """Get or create extractor instances."""
    global _extractor_instances
    if _extractor_instances is None:
        _extractor_instances = {
            name: cls() for name, cls in _EXTRACTORS.items()
        }
    return _extractor_instances


def extract_fields(
    parsed: ParsedContent,
    site_configs: list[dict[str, Any]] | None = None,
) -> ExtractedFields:
    """
    Extract job fields from parsed content.

    Args:
        parsed: Parsed content from parse step
        site_configs: Site-specific extraction configurations

    Returns:
        ExtractedFields with raw extracted values
    """
    # Build extraction context
    ctx = ExtractionContext.from_scrape_result(
        url=parsed.url,
        markdown=parsed.markdown_body or "",
        raw_row=None,
        site_configs=site_configs or [],
        debug=False,
    )

    # Add parsed data to context
    if parsed.hints:
        ctx.hints = parsed.hints
    if parsed.json_ld_posting:
        ctx.structured_data = parsed.json_ld_posting
        ctx.json_payload = parsed.json_ld_posting

    # Run extractors
    extractors = _get_extractors()
    extraction_strategies: dict[str, str] = {}

    # Extract title
    title = None
    title_result = extractors["title"].extract(ctx)
    if title_result.final_value:
        title = title_result.final_value
        extraction_strategies["title"] = title_result.winning_strategy or "unknown"
        ctx.extracted_title = title

    # Extract company
    company = None
    company_result = extractors["company"].extract(ctx)
    if company_result.final_value:
        company = company_result.final_value
        extraction_strategies["company"] = company_result.winning_strategy or "unknown"
        ctx.extracted_company = company

    # Extract location
    location = None
    locations_raw: list[str] = []
    location_result = extractors["location"].extract(ctx)
    if location_result.final_value:
        location = location_result.final_value
        extraction_strategies["location"] = location_result.winning_strategy or "unknown"
        ctx.extracted_location = location

    # Also capture raw locations list from hints if available
    # This preserves the original split locations from parse_markdown_hints
    if parsed.hints and isinstance(parsed.hints.get("locations"), list):
        locations_raw = [
            loc for loc in parsed.hints["locations"]
            if isinstance(loc, str) and loc.strip()
        ]

    # Extract remote status
    is_remote = None
    remote_result = extractors["remote"].extract(ctx)
    if remote_result.final_value is not None:
        is_remote = remote_result.final_value
        extraction_strategies["remote"] = remote_result.winning_strategy or "unknown"
        ctx.extracted_remote = is_remote

    # Extract level
    level = None
    level_result = extractors["level"].extract(ctx)
    if level_result.final_value:
        level = level_result.final_value
        extraction_strategies["level"] = level_result.winning_strategy or "unknown"

    # Extract compensation
    compensation_text = None
    compensation_min = None
    compensation_max = None
    comp_result = extractors["compensation"].extract(ctx)
    if comp_result.final_value:
        comp_value = comp_result.final_value
        if isinstance(comp_value, dict):
            compensation_min = comp_value.get("min")
            compensation_max = comp_value.get("max")
            compensation_text = comp_value.get("text")
        elif isinstance(comp_value, (int, float)) and comp_value > 0:
            # Single compensation value - use as max
            compensation_max = int(comp_value)
        extraction_strategies["compensation"] = comp_result.winning_strategy or "unknown"

    # Extract posted_at
    posted_at = None
    posted_at_unknown = False
    posted_result = extractors["posted_at"].extract(ctx)
    if posted_result.final_value is not None:
        posted_value = posted_result.final_value
        if isinstance(posted_value, dict):
            posted_at = posted_value.get("timestamp")
            posted_at_unknown = posted_value.get("unknown", False)
        else:
            posted_at = posted_value
        extraction_strategies["posted_at"] = posted_result.winning_strategy or "unknown"

    # Extract description
    description = None
    desc_result = extractors["description"].extract(ctx)
    if desc_result.final_value:
        description = desc_result.final_value
        extraction_strategies["description"] = desc_result.winning_strategy or "unknown"

    return ExtractedFields(
        title=title,
        company=company,
        location=location,
        locations_raw=locations_raw,
        compensation_text=compensation_text,
        compensation_min=compensation_min,
        compensation_max=compensation_max,
        posted_at=posted_at,
        posted_at_unknown=posted_at_unknown,
        level=level,
        is_remote=is_remote,
        description=description,
        extraction_strategies=extraction_strategies,
    )
