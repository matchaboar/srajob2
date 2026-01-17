"""
Extraction context containing all input data for field extraction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from ..site_handlers.base import BaseSiteHandler


@dataclass
class ExtractionContext:
    """
    Complete context for field extraction.

    Contains all input data that strategies can use to extract field values.
    This is created once at the start of extraction and passed to all strategies.
    """

    # Primary URL for this job
    url: str

    # Raw content from SpiderCloud
    raw_markdown: str = ""
    raw_html: str = ""

    # Processed content (after handler normalization)
    normalized_markdown: str = ""
    description_body: str = ""  # Main description without metadata footer
    metadata_block: str = ""  # Footer metadata section

    # Structured data (from JSON-LD, API responses, embedded JSON)
    structured_data: dict[str, Any] | None = None
    json_payload: dict[str, Any] | None = None

    # Handler info
    handler: BaseSiteHandler | None = None
    handler_name: str = ""
    site_type: str | None = None

    # Title extracted by handler's normalize_markdown()
    handler_extracted_title: str | None = None

    # Hints from parse_markdown_hints() - includes title, level, location, etc.
    hints: dict[str, Any] = field(default_factory=dict)

    # Seed hints from JSON embedded in description
    seed_hints: dict[str, Any] = field(default_factory=dict)

    # Site-specific configs (regex patterns, overrides)
    site_configs: list[dict[str, Any]] = field(default_factory=list)

    # Debug mode - if True, all strategies run even after finding valid result
    debug: bool = False

    # Domain info (extracted from URL)
    domain: str = ""

    # Raw row data (for accessing original field values)
    raw_row: dict[str, Any] = field(default_factory=dict)

    # Previously extracted values (for cross-field dependencies)
    # These are populated as extraction progresses
    extracted_title: str | None = None
    extracted_company: str | None = None
    extracted_location: str | None = None
    extracted_remote: bool | None = None

    @classmethod
    def from_scrape_result(
        cls,
        url: str,
        markdown: str,
        raw_html: str = "",
        handler: BaseSiteHandler | None = None,
        structured_data: dict[str, Any] | None = None,
        raw_row: dict[str, Any] | None = None,
        site_configs: list[dict[str, Any]] | None = None,
        debug: bool = False,
    ) -> ExtractionContext:
        """
        Factory method to create context from SpiderCloud scrape result.

        This handles all the preprocessing that needs to happen before extraction:
        - Domain extraction from URL
        - Handler normalization
        - Description/metadata splitting
        - Hint parsing
        """
        from ..helpers.scrape_utils import (
            parse_markdown_hints,
            split_description_metadata,
            strip_known_nav_blocks,
            _extract_job_detail_seed_from_json,
            _strip_embedded_theme_json,
        )

        # Extract domain from URL
        domain = ""
        try:
            parsed = urlparse(url)
            domain = (parsed.hostname or "").lower()
            if domain.startswith("www."):
                domain = domain[4:]
        except Exception:
            pass

        # Normalize markdown through handler if available
        normalized = markdown
        handler_title = None
        if handler:
            try:
                result = handler.normalize_markdown(markdown)
                if isinstance(result, tuple) and len(result) >= 2:
                    normalized, handler_title = result
                else:
                    normalized = result
            except Exception:
                pass

        # Extract seed hints from embedded JSON
        seed_description = None
        seed_hints: dict[str, Any] = {}
        try:
            seed_description, seed_hints_result = _extract_job_detail_seed_from_json(
                normalized
            )
            if seed_hints_result and isinstance(seed_hints_result, dict):
                seed_hints = seed_hints_result
            if seed_description:
                normalized = seed_description
        except Exception:
            pass

        # Clean and split description
        cleaned = strip_known_nav_blocks(normalized)
        cleaned = _strip_embedded_theme_json(cleaned)
        description_body = cleaned
        metadata_block = ""
        try:
            description_body, metadata_block = split_description_metadata(cleaned)
        except Exception:
            pass

        # Parse hints from markdown
        hints: dict[str, Any] = {}
        try:
            hints = parse_markdown_hints(cleaned)
        except Exception:
            pass

        # Merge seed hints into hints (seed hints have lower priority)
        if seed_hints:
            for key in ("title", "company", "locations", "location", "remote"):
                if not hints.get(key) and seed_hints.get(key):
                    hints[key] = seed_hints[key]

        return cls(
            url=url,
            raw_markdown=markdown,
            raw_html=raw_html,
            normalized_markdown=normalized,
            description_body=description_body,
            metadata_block=metadata_block,
            structured_data=structured_data,
            json_payload=structured_data,  # Alias for convenience
            handler=handler,
            handler_name=handler.name if handler else "",
            site_type=getattr(handler, "site_type", None) if handler else None,
            handler_extracted_title=handler_title,
            hints=hints,
            seed_hints=seed_hints,
            site_configs=site_configs or [],
            domain=domain,
            raw_row=raw_row or {},
            debug=debug,
        )

    def get_raw_field(self, *keys: str, default: Any = None) -> Any:
        """
        Get a field from raw_row, trying multiple key names.

        Example:
            ctx.get_raw_field("job_title", "title", default="")
        """
        for key in keys:
            value = self.raw_row.get(key)
            if value is not None:
                return value
        return default

    def update_extracted(
        self,
        *,
        title: str | None = None,
        company: str | None = None,
        location: str | None = None,
        remote: bool | None = None,
    ) -> None:
        """
        Update extracted values for cross-field dependencies.

        Call this after extracting a field to make it available
        to subsequent extractions.
        """
        if title is not None:
            self.extracted_title = title
        if company is not None:
            self.extracted_company = company
        if location is not None:
            self.extracted_location = location
        if remote is not None:
            self.extracted_remote = remote
