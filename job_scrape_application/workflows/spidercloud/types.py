"""SpiderCloud module types and dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional


@dataclass(frozen=True)
class CaptchaMatch:
    """Result of detecting a captcha in SpiderCloud response."""

    marker: str
    match_text: str | None = None


class CaptchaDetectedError(Exception):
    """Raised when a SpiderCloud response looks like a captcha wall."""

    def __init__(
        self,
        marker: str,
        markdown: str | None = None,
        events: Optional[List[Any]] = None,
        match_text: str | None = None,
    ):
        super().__init__(marker)
        self.marker = marker
        self.markdown = markdown or ""
        self.events = events or []
        self.match_text = match_text


class CaptchaRetriesExceededError(Exception):
    """Raised when captcha retries are exhausted for a SpiderCloud scrape."""

    def __init__(
        self,
        url: str,
        marker: str,
        match_text: str | None,
        attempts: int,
        proxy: str | None,
        markdown: str | None = None,
        events: Optional[List[Any]] = None,
    ) -> None:
        message = (
            "Captcha retries exhausted"
            f" url={url} attempts={attempts} marker={marker} match={match_text} proxy={proxy}"
        )
        super().__init__(message)
        self.url = url
        self.marker = marker
        self.match_text = match_text
        self.attempts = attempts
        self.proxy = proxy
        self.markdown = markdown or ""
        self.events = events or []


@dataclass
class SpiderCloudDependencies:
    """Dependencies injected into SpiderCloud scraper."""

    mask_secret: Callable[[Optional[str]], Optional[str]]
    sanitize_headers: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]]
    build_request_snapshot: Callable[..., Dict[str, Any]]
    log_dispatch: Callable[..., None]
    log_sync_response: Callable[..., None]
    trim_scrape_for_convex: Callable[[Dict[str, Any]], Dict[str, Any]]
    settings: Any
    fetch_seen_urls_for_site: Callable[
        [str, Optional[str], Optional[List[str]]], Awaitable[List[str]]
    ]


@dataclass
class ScrapeResult:
    """Result of scraping a single URL via SpiderCloud."""

    url: str
    success: bool
    markdown: str | None = None
    raw_html: str | None = None
    events: List[Any] = field(default_factory=list)
    error: str | None = None
    status_code: int | None = None
    cost_credits: float | None = None
    cost_usd: float | None = None
    retryable: bool = False


@dataclass
class BatchScrapeResult:
    """Result of scraping a batch of URLs."""

    results: List[ScrapeResult] = field(default_factory=list)
    failed_items: List[Dict[str, Any]] = field(default_factory=list)
    total_cost_credits: float = 0.0
    total_cost_usd: float = 0.0


@dataclass
class ListingExtractionResult:
    """Result of extracting job URLs from a listing page."""

    job_urls: List[str] = field(default_factory=list)
    next_page_url: str | None = None
    total_jobs: int | None = None
    raw_payload: Dict[str, Any] | None = None


# Constants
SPIDERCLOUD_BATCH_SIZE = 10
CAPTCHA_RETRY_LIMIT = 0
CAPTCHA_PROXY_SEQUENCE = ("residential", "isp")
STRUCTURED_POSTED_AT_MAX_AGE_DAYS = 365
MAX_TITLE_CHARS = 500

STRUCTURED_DESCRIPTION_CHROME_MARKERS = (
    "saved jobs",
    "recently viewed jobs",
    "job alerts",
    "sign up for job alerts",
    "join our talent community",
    "view all of our available opportunities",
    "view all jobs",
    "cookie settings",
    "job_description.share",
    "job_description.share.html",
    "loginorregister",
    "mail_outline",
    "get future jobs matching this search",
)

JOB_TITLE_KEYWORDS = {
    "accountant",
    "analyst",
    "architect",
    "associate",
    "backend",
    "business",
    "cloud",
    "consultant",
    "data",
    "design",
    "designer",
    "developer",
    "development",
    "devops",
    "engineer",
    "engineering",
    "finance",
    "frontend",
    "fullstack",
    "growth",
    "hr",
    "infrastructure",
    "intern",
    "ios",
    "legal",
    "manager",
    "marketing",
    "mobile",
    "operations",
    "people",
    "platform",
    "principal",
    "product",
    "program",
    "project",
    "qa",
    "quality",
    "recruiter",
    "research",
    "sales",
    "scientist",
    "security",
    "senior",
    "sre",
    "staff",
    "support",
}
