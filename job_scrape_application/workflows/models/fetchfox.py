from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


MAX_FETCHFOX_VISITS = 40


class FetchFoxPriority(BaseModel):
    """Priority block passed to FetchFox crawl API.

    Mirrors the cURL example from the FetchFox docs: the `skip` array contains URLs we
    have already stored as jobs in Convex, so the crawler should avoid visiting them.
    """

    skip: List[str] = Field(
        default_factory=list,
        description=(
            "Job detail URLs already persisted for this site; always send the full set so FetchFox skips them."
        ),
    )
    only: Optional[List[str]] = None
    high: Optional[List[str]] = None
    low: Optional[List[str]] = None


class FetchFoxScrapeRequest(BaseModel):
    pattern: Optional[str] = None
    start_urls: List[str]
    max_depth: int = 5
    max_visits: int = Field(
        default=MAX_FETCHFOX_VISITS,
        description="Hard cap per run to avoid excessive crawling; forced to 20 for scraper workloads.",
    )
    max_extracts: Optional[int] = Field(
        default=None,
        ge=1,
        description="Maximum number of items to extract and return from FetchFox; optional passthrough.",
    )
    template: Dict[str, str]
    content_transform: Literal["text_only", "full_html", "slim_html", "reduce"] = Field(
        default="slim_html",
        description=(
            "How FetchFox sends page context to the AI: 'text_only' strips HTML, "
            "'full_html' sends the complete document, 'slim_html' keeps high-value tags (default), "
            "'reduce' learns a reduction program from the page and template for reuse."
        ),
    )
    priority: FetchFoxPriority

    @field_validator("max_visits")
    @classmethod
    def cap_visits(cls, value: int) -> int:
        return min(MAX_FETCHFOX_VISITS, value)
