"""Job normalization pipeline.

Replaces the old "heuristic patch" system with a clear step-based pipeline:

    Raw Scrape → Parse → Extract → Normalize → Validate → Normalized Job

Usage:
    from job_scrape_application.workflows.normalizers import normalize_job, normalize_job_from_row

    # For new scrapes
    result = normalize_job(RawScrapeInput(url="...", markdown="..."))

    # For re-processing existing rows
    result = normalize_job_from_row(existing_row)

    if result.success:
        normalized_job = result.job
        update_fields = result.update_fields  # For backwards compatibility
"""

from .types import (
    # Input/output types
    RawScrapeInput,
    ParsedContent,
    ExtractedFields,
    NormalizedJob,
    NormalizationResult,
    # Tracing types
    PipelineTrace,
    StepTrace,
    # Constants
    NORMALIZATION_VERSION,
)
from .pipeline import (
    normalize_job,
    normalize_job_from_row,
    build_job_update,  # Backwards-compatible adapter
)

__all__ = [
    # Main entry points
    "normalize_job",
    "normalize_job_from_row",
    "build_job_update",  # Backwards-compatible adapter
    # Types
    "RawScrapeInput",
    "ParsedContent",
    "ExtractedFields",
    "NormalizedJob",
    "NormalizationResult",
    "PipelineTrace",
    "StepTrace",
    # Constants
    "NORMALIZATION_VERSION",
]
