"""Pydantic models shared across workflows."""

from .fetchfox import FetchFoxPriority, FetchFoxScrapeRequest, MAX_FETCHFOX_VISITS
from .greenhouse import (
    GreenhouseBoardResponse,
    GreenhouseJob,
    GreenhouseJobLocation,
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)

__all__ = [
    "FetchFoxPriority",
    "FetchFoxScrapeRequest",
    "MAX_FETCHFOX_VISITS",
    "GreenhouseBoardResponse",
    "GreenhouseJob",
    "GreenhouseJobLocation",
    "extract_greenhouse_job_urls",
    "load_greenhouse_board",
]
