"""Validate step: Check required fields and bounds.

This step validates the normalized job data:
- Required field presence
- Value bounds (compensation limits, string lengths)
- URL format validation
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from ..types import NormalizedJob

logger = logging.getLogger(__name__)

# Validation constants
MIN_TITLE_LENGTH = 3
MAX_TITLE_LENGTH = 500
MIN_DESCRIPTION_WORDS = 10
MAX_COMPENSATION = 10_000_000  # $10M cap
MIN_COMPENSATION = 0


def validate_job(job: NormalizedJob) -> list[str]:
    """
    Validate normalized job data.

    Args:
        job: Normalized job to validate

    Returns:
        List of validation error messages (empty if valid)
    """
    errors: list[str] = []

    # Required fields
    if not job.url:
        errors.append("Missing required field: url")

    if not job.title:
        errors.append("Missing required field: title")
    elif len(job.title) < MIN_TITLE_LENGTH:
        errors.append(f"Title too short: {len(job.title)} < {MIN_TITLE_LENGTH}")
    elif len(job.title) > MAX_TITLE_LENGTH:
        errors.append(f"Title too long: {len(job.title)} > {MAX_TITLE_LENGTH}")

    if not job.company:
        errors.append("Missing required field: company")

    # URL validation
    if job.url and not _is_valid_url(job.url):
        errors.append(f"Invalid URL format: {job.url[:100]}")

    # Compensation bounds
    if job.compensation_min is not None:
        if job.compensation_min < MIN_COMPENSATION:
            errors.append(f"Compensation min below zero: {job.compensation_min}")
        if job.compensation_min > MAX_COMPENSATION:
            errors.append(f"Compensation min exceeds cap: {job.compensation_min}")

    if job.compensation_max is not None:
        if job.compensation_max < MIN_COMPENSATION:
            errors.append(f"Compensation max below zero: {job.compensation_max}")
        if job.compensation_max > MAX_COMPENSATION:
            errors.append(f"Compensation max exceeds cap: {job.compensation_max}")

    # Compensation ordering
    if (
        job.compensation_min is not None
        and job.compensation_max is not None
        and job.compensation_min > job.compensation_max
    ):
        errors.append(
            f"Compensation min > max: {job.compensation_min} > {job.compensation_max}"
        )

    # Level validation
    valid_levels = {"junior", "mid", "senior", "staff", "principal", "executive"}
    if job.level and job.level.lower() not in valid_levels:
        # This is a warning, not an error - we allow through but log
        logger.warning("Unusual level value: %s", job.level)

    return errors


def _is_valid_url(url: str) -> bool:
    """Check if URL has valid format."""
    if not url:
        return False

    try:
        parsed = urlparse(url)
        return bool(parsed.scheme and parsed.netloc)
    except Exception:
        return False
