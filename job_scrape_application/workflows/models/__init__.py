"""Pydantic models shared across workflows."""

from .fetchfox import FetchFoxPriority, FetchFoxScrapeRequest, MAX_FETCHFOX_VISITS

__all__ = [
    "FetchFoxPriority",
    "FetchFoxScrapeRequest",
    "MAX_FETCHFOX_VISITS",
]

