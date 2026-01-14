"""
Modular job field extraction system.

This module provides a strategy-based extraction system for job fields.
Each field has its own extractor with multiple strategies that are tried
in priority order. Debug mode allows all strategies to run and produces
a trace showing why each strategy succeeded or failed.

Example usage:

    from job_scrape_application.workflows.extractors import (
        ExtractionContext,
        extract_job_fields,
    )

    # Create context from scrape result
    context = ExtractionContext.from_scrape_result(
        url="https://example.com/job/123",
        markdown=scraped_markdown,
        handler=site_handler,
        debug=True,  # Enable debug mode to run all strategies
    )

    # Extract all fields
    results = extract_job_fields(context)

    # Access results
    title = results["title"].final_value
    location = results["location"].final_value

    # Debug: see all strategy results
    for field, result in results.items():
        print(result.to_debug_dict())
"""

from __future__ import annotations

from typing import Any

from .base import (
    ExtractionResult,
    ExtractionStrategy,
    FieldExtractor,
    StrategyPriority,
    StrategyResult,
)
from .context import ExtractionContext
from .title_extractor import JobTitleExtractor
from .company_extractor import CompanyExtractor
from .location_extractor import LocationExtractor
from .remote_extractor import RemoteExtractor
from .level_extractor import LevelExtractor
from .compensation_extractor import CompensationExtractor
from .posted_at_extractor import PostedAtExtractor
from .description_extractor import DescriptionExtractor
from .integration import (
    extract_job_from_scrape,
    build_heuristic_patch_from_extractors,
)


# All extractors in the order they should be run
# (some extractors depend on others, so order matters)
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

# Singleton instances (created on first use)
_extractor_instances: dict[str, FieldExtractor] | None = None


def _get_extractors() -> dict[str, FieldExtractor]:
    """Get or create extractor instances."""
    global _extractor_instances
    if _extractor_instances is None:
        _extractor_instances = {
            name: cls() for name, cls in _EXTRACTORS.items()
        }
    return _extractor_instances


def extract_job_fields(
    context: ExtractionContext,
    *,
    fields: list[str] | None = None,
    run_all: bool | None = None,
) -> dict[str, ExtractionResult]:
    """
    Extract all (or specified) job fields using modular extractors.

    Args:
        context: ExtractionContext with all input data
        fields: Optional list of field names to extract. If None, extracts all.
        run_all: If True, run all strategies even after finding valid result.
                 If None, uses context.debug setting.

    Returns:
        Dictionary mapping field names to ExtractionResult objects.
        Each ExtractionResult contains:
        - final_value: The extracted value (or None)
        - winning_strategy: Name of the strategy that produced the value
        - all_results: List of all strategy results (useful for debugging)
    """
    extractors = _get_extractors()

    if run_all is None:
        run_all = context.debug

    if fields is None:
        fields = list(extractors.keys())

    results: dict[str, ExtractionResult] = {}

    for field in fields:
        if field not in extractors:
            continue

        extractor = extractors[field]
        result = extractor.extract(context, run_all=run_all)
        results[field] = result

        # Update context with extracted values for cross-field dependencies
        if result.final_value is not None:
            if field == "title":
                context.extracted_title = result.final_value
            elif field == "company":
                context.extracted_company = result.final_value
            elif field == "location":
                context.extracted_location = result.final_value
            elif field == "remote":
                context.extracted_remote = result.final_value

    return results


def extract_field(
    context: ExtractionContext,
    field: str,
    *,
    run_all: bool | None = None,
) -> ExtractionResult:
    """
    Extract a single field.

    Args:
        context: ExtractionContext with all input data
        field: Field name to extract (e.g., "title", "location")
        run_all: If True, run all strategies even after finding valid result.

    Returns:
        ExtractionResult for the field.

    Raises:
        ValueError: If the field name is not recognized.
    """
    extractors = _get_extractors()

    if field not in extractors:
        raise ValueError(f"Unknown field: {field}. Valid fields: {list(extractors.keys())}")

    if run_all is None:
        run_all = context.debug

    extractor = extractors[field]
    return extractor.extract(context, run_all=run_all)


def get_debug_trace(results: dict[str, ExtractionResult]) -> dict[str, Any]:
    """
    Convert extraction results to a debug trace dictionary.

    This is useful for logging or displaying the full extraction trace.

    Args:
        results: Dictionary of extraction results from extract_job_fields()

    Returns:
        Dictionary with debug information for each field.
    """
    return {
        field: result.to_debug_dict()
        for field, result in results.items()
    }


def format_debug_trace(results: dict[str, ExtractionResult]) -> str:
    """
    Format extraction results as a human-readable debug string.

    Args:
        results: Dictionary of extraction results from extract_job_fields()

    Returns:
        Multi-line string with debug information.
    """
    import json

    trace = get_debug_trace(results)
    return json.dumps(trace, indent=2, default=str)


# Public API
__all__ = [
    # Core classes
    "ExtractionContext",
    "ExtractionResult",
    "ExtractionStrategy",
    "FieldExtractor",
    "StrategyPriority",
    "StrategyResult",
    # Extractors
    "JobTitleExtractor",
    "CompanyExtractor",
    "LocationExtractor",
    "RemoteExtractor",
    "LevelExtractor",
    "CompensationExtractor",
    "PostedAtExtractor",
    "DescriptionExtractor",
    # Functions
    "extract_job_fields",
    "extract_field",
    "get_debug_trace",
    "format_debug_trace",
    # Integration functions
    "extract_job_from_scrape",
    "build_heuristic_patch_from_extractors",
]
