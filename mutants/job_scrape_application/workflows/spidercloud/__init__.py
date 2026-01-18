"""SpiderCloud scraping module.

This module provides the SpiderCloud integration for job scraping.
"""

from .types import (
    # Exceptions
    CaptchaDetectedError,
    CaptchaRetriesExceededError,
    # Dataclasses
    CaptchaMatch,
    SpiderCloudDependencies,
    ScrapeResult,
    BatchScrapeResult,
    ListingExtractionResult,
    # Constants
    SPIDERCLOUD_BATCH_SIZE,
    CAPTCHA_RETRY_LIMIT,
    CAPTCHA_PROXY_SEQUENCE,
    STRUCTURED_POSTED_AT_MAX_AGE_DAYS,
    STRUCTURED_DESCRIPTION_CHROME_MARKERS,
    MAX_TITLE_CHARS,
    JOB_TITLE_KEYWORDS,
)

__all__ = [
    # Exceptions
    "CaptchaDetectedError",
    "CaptchaRetriesExceededError",
    # Dataclasses
    "CaptchaMatch",
    "SpiderCloudDependencies",
    "ScrapeResult",
    "BatchScrapeResult",
    "ListingExtractionResult",
    # Constants
    "SPIDERCLOUD_BATCH_SIZE",
    "CAPTCHA_RETRY_LIMIT",
    "CAPTCHA_PROXY_SEQUENCE",
    "STRUCTURED_POSTED_AT_MAX_AGE_DAYS",
    "STRUCTURED_DESCRIPTION_CHROME_MARKERS",
    "MAX_TITLE_CHARS",
    "JOB_TITLE_KEYWORDS",
]
