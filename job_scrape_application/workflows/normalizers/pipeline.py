"""Job normalization pipeline.

Main entry point for normalizing scraped job data.

Pipeline steps:
1. Parse - Convert raw content to structured format
2. Extract - Pull field values from parsed content
3. Normalize - Standardize formats (location, URL, etc.)
4. Validate - Check required fields and bounds

Each step is independent and testable.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .types import (
    RawScrapeInput,
    ParsedContent,
    ExtractedFields,
    NormalizedJob,
    NormalizationResult,
    PipelineTrace,
    StepTrace,
    NORMALIZATION_VERSION,
)
from .steps import parse_step, extract_step, normalize_step, validate_step

logger = logging.getLogger(__name__)


def normalize_job(
    raw_input: RawScrapeInput,
    *,
    trace_enabled: bool = False,
) -> NormalizationResult:
    """
    Normalize raw scrape data into a structured job.

    This is the single entry point for job normalization.
    Replaces the old _build_job_detail_heuristic_patch() and
    build_heuristic_patch_from_extractors() functions.

    Pipeline steps:
    1. Parse - Convert raw content to structured format
    2. Extract - Pull field values from parsed content
    3. Normalize - Standardize formats (location, URL, etc.)
    4. Validate - Check required fields and bounds

    Args:
        raw_input: Raw scrape data
        trace_enabled: If True, record detailed trace for debugging

    Returns:
        NormalizationResult containing the normalized job or errors
    """
    start_time = time.perf_counter()
    trace = PipelineTrace() if trace_enabled else None
    errors: list[str] = []

    try:
        # Step 1: Parse raw content
        parsed = _run_step(
            "parse",
            lambda: parse_step.parse_content(raw_input),
            trace,
        )

        # Step 2: Extract fields from parsed content
        extracted = _run_step(
            "extract",
            lambda: extract_step.extract_fields(parsed, raw_input.site_configs),
            trace,
        )

        # Step 3: Normalize extracted values
        normalized = _run_step(
            "normalize",
            lambda: normalize_step.normalize_fields(extracted, parsed),
            trace,
        )

        # Step 4: Validate the result
        validation_errors = _run_step(
            "validate",
            lambda: validate_step.validate_job(normalized),
            trace,
        )
        errors.extend(validation_errors)

        if trace:
            trace.total_duration_ms = (time.perf_counter() - start_time) * 1000

        # Build backwards-compatible update fields
        update_fields = _build_update_fields(normalized, raw_input.existing_row)

        return NormalizationResult(
            job=normalized,
            trace=trace,
            success=len(errors) == 0,
            errors=errors,
            update_fields=update_fields,
            records=_build_records(extracted, parsed),
        )

    except Exception as e:
        logger.exception("Normalization pipeline failed: %s", e)
        if trace:
            trace.add_error(str(e))
            trace.total_duration_ms = (time.perf_counter() - start_time) * 1000
        return NormalizationResult(
            job=None,
            trace=trace,
            success=False,
            errors=[str(e)],
        )


def normalize_job_from_row(
    row: dict[str, Any],
    *,
    site_configs: list[dict[str, Any]] | None = None,
    trace_enabled: bool = False,
) -> NormalizationResult:
    """
    Normalize an existing job row (for re-processing).

    This is a convenience wrapper for normalize_job() that accepts
    the existing row format used by the database.

    Args:
        row: Existing job row from database
        site_configs: Site-specific configurations
        trace_enabled: If True, record detailed trace

    Returns:
        NormalizationResult
    """
    raw_input = RawScrapeInput(
        url=row.get("url") or "",
        markdown=row.get("description") or "",
        existing_row=row,
        site_configs=site_configs or [],
    )
    return normalize_job(raw_input, trace_enabled=trace_enabled)


def _run_step(
    step_name: str,
    step_fn: callable,
    trace: PipelineTrace | None,
):
    """Run a pipeline step with optional tracing."""
    start_time = time.perf_counter()
    result = step_fn()
    duration_ms = (time.perf_counter() - start_time) * 1000

    if trace:
        trace.add_step(StepTrace(
            step_name=step_name,
            input_summary=f"Step {step_name} input",
            output_summary=f"Step {step_name} completed",
            duration_ms=duration_ms,
        ))

    return result


def _build_update_fields(
    normalized: NormalizedJob,
    existing_row: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build backwards-compatible update fields dict.

    This creates the dict format expected by existing callers
    (replaces the old "patch" dict).
    """
    update: dict[str, Any] = {}

    # Track attempts for existing rows - always set these for backwards compat
    if existing_row:
        attempts = int(existing_row.get("heuristicAttempts") or 0) + 1
        update["heuristicAttempts"] = attempts
        update["heuristicLastTried"] = int(time.time() * 1000)
        update["heuristicVersion"] = NORMALIZATION_VERSION

    # Title - only update if we have a good one and existing is bad
    if normalized.title:
        existing_title = (existing_row or {}).get("title") or (existing_row or {}).get("jobTitle") or ""
        if _should_override_title(existing_title):
            update["title"] = normalized.title
            update["jobTitle"] = normalized.title

    # Location fields
    if normalized.location:
        update["location"] = normalized.location

    if normalized.locations:
        update["locations"] = normalized.locations

    if normalized.location_states:
        update["locationStates"] = normalized.location_states

    if normalized.location_search:
        update["locationSearch"] = normalized.location_search

    if normalized.countries:
        update["countries"] = normalized.countries

    if normalized.country:
        update["country"] = normalized.country

    # Company
    if normalized.company:
        update["company"] = normalized.company

    # Compensation - use totalCompensation for backwards compat
    existing_comp = (existing_row or {}).get("totalCompensation") or 0
    if normalized.compensation_max is not None and normalized.compensation_max > 0:
        if not existing_comp or existing_comp <= 0:
            # New compensation found - update it
            update["totalCompensation"] = int(normalized.compensation_max)
            update["compensationUnknown"] = False
            update["compensationReason"] = "extractor:normalization_pipeline"
        else:
            # Compensation already exists and we found one too - mark as known
            update["compensationUnknown"] = False
    elif existing_comp and existing_comp > 0:
        # No new compensation found but existing is valid - mark as known
        update["compensationUnknown"] = False
    else:
        # No compensation at all - propagate existing compensationUnknown
        existing_comp_unknown = (existing_row or {}).get("compensationUnknown")
        if existing_comp_unknown is not None:
            update["compensationUnknown"] = existing_comp_unknown

    # Remote - use "remote" key for backwards compat
    from ...constants import is_remote_company

    scraper_remote = (existing_row or {}).get("remote")
    company = normalized.company or (existing_row or {}).get("company") or ""
    company_remote = is_remote_company(company)

    if company_remote:
        update["remote"] = True
    elif normalized.is_remote is not None:
        # Don't override scraper's authoritative remote=True
        if scraper_remote is not True:
            if normalized.is_remote and scraper_remote is not True:
                update["remote"] = True
            elif not normalized.is_remote and scraper_remote is not False:
                update["remote"] = False

    # Level
    if normalized.level:
        update["level"] = normalized.level

    # Posted at
    if normalized.posted_at is not None:
        update["postedAt"] = normalized.posted_at

    if normalized.posted_at_unknown:
        update["postedAtUnknown"] = normalized.posted_at_unknown

    # Description
    if normalized.description:
        update["description"] = normalized.description

    return update


def _should_override_title(value: str) -> bool:
    """Check if title value should be overridden."""
    import re

    if not value:
        return True
    lowered = value.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", lowered).strip()

    # Generic/placeholder titles
    if normalized in {
        "the role", "our team", "the team", "role", "job description",
        "description", "description and requirements", "description requirements",
    }:
        return True
    if lowered in {"unknown", "n/a", "na", "untitled"}:
        return True

    # Titles that look like requirements, not job titles
    if re.search(r"\b\d+\+?\s+years?\b", lowered):
        return True
    if re.search(r"\byears?\s+(?:of\s+)?experience\b", lowered):
        return True
    if re.search(r"\byears?\s+working\b", lowered):
        return True
    if re.search(
        r"\bexperience\s+(?:in|with|providing|working|leading|managing|developing|designing|supporting)\b",
        lowered,
    ):
        return True
    if re.search(r"\bability\s+to\b", lowered):
        return True
    if re.search(r"\bknowledge\s+of\b", lowered):
        return True

    # Title looks like a sentence
    if lowered.endswith((".", "!", "?")):
        return True

    # Too long to be a title
    if len(lowered.split()) > 14:
        return True

    return False


def _build_records(
    extracted: ExtractedFields,
    parsed: ParsedContent,
) -> list[dict[str, str]]:
    """Build extraction records for analytics/debugging."""
    records: list[dict[str, str]] = []
    domain = parsed.domain or "default"

    for field_name, strategy in extracted.extraction_strategies.items():
        records.append({
            "domain": domain,
            "field": field_name,
            "regex": f"extractor:{strategy}",
        })

    return records


# =============================================================================
# Backwards-compatible adapter (drop-in replacement for old heuristic function)
# =============================================================================

def build_job_update(
    row: dict[str, Any],
    configs: list[dict[str, Any]] | None = None,
    now_ms: int | None = None,
    *,
    use_extractors: bool = True,  # Ignored - kept for backward compatibility
    **kwargs: Any,  # Accept any additional kwargs for backward compatibility
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """
    Build job update fields from existing row.

    This is a drop-in replacement for _build_job_detail_heuristic_patch().
    Returns the same (patch, records) tuple format for backwards compatibility.

    Args:
        row: Existing job row from database
        configs: Site-specific configurations (unused, kept for signature compat)
        now_ms: Current timestamp in milliseconds (unused, kept for signature compat)
        use_extractors: Ignored, kept for backward compatibility
        **kwargs: Additional arguments ignored for backward compatibility

    Returns:
        Tuple of (update_fields dict, records list)
    """
    result = normalize_job_from_row(row, site_configs=configs)
    return result.update_fields, result.records
