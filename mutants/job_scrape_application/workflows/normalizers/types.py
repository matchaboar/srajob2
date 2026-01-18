"""Types for the job normalization pipeline.

These types represent the data flowing through each step of the pipeline:
Raw scrape data → Parsed content → Extracted fields → Normalized job

No "patch" or "heuristic" concepts - just clear step-based processing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RawScrapeInput:
    """Input data from a scrape operation.

    This is the raw data before any processing.
    """

    url: str
    markdown: str | None = None
    raw_html: str | None = None
    json_ld: dict[str, Any] | None = None
    events: list[Any] = field(default_factory=list)
    existing_row: dict[str, Any] | None = None  # For re-processing existing jobs
    site_configs: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ParsedContent:
    """Content after the parse step.

    Structured data extracted from raw markdown/HTML.
    """

    url: str
    domain: str | None = None
    markdown_body: str | None = None
    markdown_metadata: str | None = None
    json_ld_posting: dict[str, Any] | None = None
    raw_html: str | None = None
    hints: dict[str, str] = field(default_factory=dict)  # Key-value hints from content
    handler_name: str | None = None


@dataclass
class ExtractedFields:
    """Fields extracted from parsed content.

    Raw extracted values before normalization.
    """

    title: str | None = None
    company: str | None = None
    location: str | None = None
    locations_raw: list[str] = field(default_factory=list)
    compensation_text: str | None = None
    compensation_min: int | None = None
    compensation_max: int | None = None
    posted_at: int | None = None
    posted_at_unknown: bool = False
    level: str | None = None
    is_remote: bool | None = None
    description: str | None = None

    # Extraction metadata for debugging
    extraction_strategies: dict[str, str] = field(default_factory=dict)


@dataclass
class NormalizedJob:
    """Fully normalized job data ready for storage.

    All fields are in their final format.
    """

    url: str
    title: str
    company: str
    location: str | None = None
    locations: list[str] = field(default_factory=list)
    location_states: list[str] = field(default_factory=list)
    location_search: str | None = None
    countries: list[str] = field(default_factory=list)
    country: str | None = None
    compensation_min: int | None = None
    compensation_max: int | None = None
    compensation_text: str | None = None
    posted_at: int | None = None
    posted_at_unknown: bool = False
    level: str | None = None
    is_remote: bool = False
    description: str | None = None

    # Metadata
    provider: str | None = None
    handler: str | None = None
    normalization_version: int = 1


@dataclass
class StepTrace:
    """Trace of a single processing step."""

    step_name: str
    input_summary: str
    output_summary: str
    details: dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0


@dataclass
class PipelineTrace:
    """Full trace of the normalization pipeline."""

    steps: list[StepTrace] = field(default_factory=list)
    total_duration_ms: float = 0.0
    success: bool = True
    errors: list[str] = field(default_factory=list)

    def add_step(self, step: StepTrace) -> None:
        """Add a step trace."""
        self.steps.append(step)
        self.total_duration_ms += step.duration_ms

    def add_error(self, error: str) -> None:
        """Add an error and mark as failed."""
        self.errors.append(error)
        self.success = False


@dataclass
class NormalizationResult:
    """Result of the normalization pipeline."""

    job: NormalizedJob | None = None
    trace: PipelineTrace | None = None
    success: bool = True
    errors: list[str] = field(default_factory=list)

    # For backwards compatibility during migration
    # These will be removed once all callers use the new API
    update_fields: dict[str, Any] = field(default_factory=dict)
    records: list[dict[str, str]] = field(default_factory=list)


# Version constant - replaces HEURISTIC_VERSION
# Set to 5 for backwards compatibility with existing data
NORMALIZATION_VERSION = 5
