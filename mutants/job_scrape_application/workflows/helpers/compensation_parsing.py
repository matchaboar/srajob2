"""Compensation parsing utilities for job scraping.

This module provides functions for extracting and normalizing compensation
information from job postings.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from .regex_patterns import (
    NUMBER_TOKEN_PATTERN,
    RETIREMENT_PLAN_PATTERN,
)

# Compensation bounds and defaults
DEFAULT_TOTAL_COMPENSATION = 0
MIN_TOTAL_COMPENSATION = 30_000
MAX_TOTAL_COMPENSATION = 5_000_000
HOURLY_TO_ANNUAL_MULTIPLIER = 2080
UNKNOWN_COMPENSATION_REASON = "pending markdown structured extraction"


def normalize_compensation_value(value: Any) -> Optional[int]:
    """Normalize a compensation value to an integer within valid bounds.

    Args:
        value: The compensation value to normalize (int or float expected)

    Returns:
        The normalized compensation as an integer, or None if invalid/out of bounds
    """
    if not isinstance(value, (int, float)):
        return None
    comp = int(value)
    if comp <= MIN_TOTAL_COMPENSATION or comp >= MAX_TOTAL_COMPENSATION:
        return None
    return comp


def parse_compensation(value: Any, *, with_meta: bool = False) -> int | tuple[int, bool]:
    """Parse compensation from various input formats.

    Handles numeric values directly, and extracts numbers from strings
    (e.g., "$150,000 - $200,000" or "150K").

    Args:
        value: The compensation value to parse (int, float, or str)
        with_meta: If True, returns a tuple of (value, is_unknown) instead of just value

    Returns:
        If with_meta is False: The parsed compensation as an integer (0 if invalid)
        If with_meta is True: A tuple of (compensation, is_unknown_flag)
    """
    if isinstance(value, (int, float)) and value > 0:
        normalized = normalize_compensation_value(value)
        if normalized is not None:
            return (normalized, False) if with_meta else normalized
        return (0, True) if with_meta else 0
    if isinstance(value, str):
        cleaned = value.replace("\u00a0", " ")
        has_retirement_token = re.search(RETIREMENT_PLAN_PATTERN, cleaned, flags=re.IGNORECASE) is not None
        if has_retirement_token:
            cleaned = re.sub(RETIREMENT_PLAN_PATTERN, " ", cleaned, flags=re.IGNORECASE)
        numbers = re.findall(NUMBER_TOKEN_PATTERN, cleaned)
        if numbers:
            try:
                parsed = max(float(num.replace(",", "")) for num in numbers)
                if parsed > 0:
                    if has_retirement_token and parsed < 1000:
                        return (0, True) if with_meta else 0
                    normalized = normalize_compensation_value(parsed)
                    if normalized is not None:
                        return (normalized, False) if with_meta else normalized
                    return (0, True) if with_meta else 0
            except ValueError:
                pass
    return (0, True) if with_meta else 0


__all__ = [
    # Constants
    "DEFAULT_TOTAL_COMPENSATION",
    "MIN_TOTAL_COMPENSATION",
    "MAX_TOTAL_COMPENSATION",
    "HOURLY_TO_ANNUAL_MULTIPLIER",
    "UNKNOWN_COMPENSATION_REASON",
    # Functions
    "normalize_compensation_value",
    "parse_compensation",
]
