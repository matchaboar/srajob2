"""Parse step: Convert raw content to structured format.

This step handles:
- Extracting domain from URL
- Splitting markdown into body and metadata
- Parsing JSON-LD job postings
- Extracting key-value hints from content
- Identifying the appropriate site handler
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from ..types import RawScrapeInput, ParsedContent
from ...helpers.scrape_utils import (
    parse_markdown_hints,
    split_description_metadata,
    strip_known_nav_blocks,
)
from ...site_handlers import get_site_handler

logger = logging.getLogger(__name__)


def parse_content(raw_input: RawScrapeInput) -> ParsedContent:
    """
    Parse raw scrape content into structured format.

    Args:
        raw_input: Raw data from scrape operation

    Returns:
        ParsedContent with structured data
    """
    # Extract domain from URL
    domain = _extract_domain(raw_input.url)

    # Get site handler for URL
    handler = get_site_handler(raw_input.url)
    handler_name = handler.__class__.__name__ if handler else None

    # Parse markdown content
    markdown_body = None
    markdown_metadata = None
    hints: dict[str, str] = {}

    if raw_input.markdown:
        # Strip navigation blocks
        cleaned_markdown = strip_known_nav_blocks(raw_input.markdown)

        # Split body from metadata footer
        body, metadata = split_description_metadata(cleaned_markdown)
        markdown_body = body
        markdown_metadata = metadata

        # Extract hints from markdown (key: value pairs)
        hints = parse_markdown_hints(raw_input.markdown)

    # Use JSON-LD if provided
    json_ld_posting = raw_input.json_ld

    # Also check events for JSON-LD
    if not json_ld_posting and raw_input.events:
        json_ld_posting = _extract_json_ld_from_events(raw_input.events)

    return ParsedContent(
        url=raw_input.url,
        domain=domain,
        markdown_body=markdown_body,
        markdown_metadata=markdown_metadata,
        json_ld_posting=json_ld_posting,
        raw_html=raw_input.raw_html,
        hints=hints,
        handler_name=handler_name,
    )


def _extract_domain(url: str) -> str | None:
    """Extract domain from URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower() if parsed.netloc else None
    except Exception:
        return None


def _extract_json_ld_from_events(events: list) -> dict | None:
    """Extract JSON-LD JobPosting from events list."""
    if not events:
        return None

    for event in events:
        if not isinstance(event, dict):
            continue

        # Check for @type: JobPosting
        if event.get("@type") == "JobPosting":
            return event

        # Check nested structures
        if isinstance(event.get("data"), dict):
            data = event["data"]
            if data.get("@type") == "JobPosting":
                return data

    return None
