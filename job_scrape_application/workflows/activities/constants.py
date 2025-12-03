from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ...components.models import MAX_FETCHFOX_VISITS

# Firecrawl-related configuration shared across activities.
MAX_FIRECRAWL_VISITS = MAX_FETCHFOX_VISITS
FIRECRAWL_CACHE_MAX_AGE_MS = 600_000
FIRECRAWL_STATUS_EXPIRATION_MS = 24 * 60 * 60 * 1000
FIRECRAWL_STATUS_WARN_MS = 23 * 60 * 60 * 1000
HTTP_RETRY_BASE_SECONDS = 30
CONVEX_MUTATION_TIMEOUT_SECONDS = 3


class FirecrawlJobKind(StrEnum):
    GREENHOUSE_LISTING = "greenhouse_listing"
    SITE_CRAWL = "site_crawl"


class FirecrawlWebhookEventType(StrEnum):
    """Webhook events we request from Firecrawl.

    Firecrawl's batch scrapes normally emit the `batch_scrape.*` lifecycle
    events below. For single Greenhouse board fetches we only subscribe to the
    terminal callbacks Firecrawl sends as the bare `completed`/`failed` events.
    """

    COMPLETED = "completed"
    FAILED = "failed"
    BATCH_SCRAPE_STARTED = "batch_scrape.started"
    BATCH_SCRAPE_PAGE = "batch_scrape.page"
    BATCH_SCRAPE_COMPLETED = "batch_scrape.completed"
    BATCH_SCRAPE_FAILED = "batch_scrape.failed"


@dataclass(frozen=True)
class FirecrawlWebhookEvents:
    greenhouse_listing: tuple[FirecrawlWebhookEventType, ...]
    site_crawl: tuple[FirecrawlWebhookEventType, ...]

    def for_kind(self, kind: FirecrawlJobKind) -> tuple[FirecrawlWebhookEventType, ...]:
        """Return the configured event list for the given job kind."""

        if kind == FirecrawlJobKind.GREENHOUSE_LISTING:
            return self.greenhouse_listing
        return self.site_crawl


FIRECRAWL_WEBHOOK_EVENTS = FirecrawlWebhookEvents(
    greenhouse_listing=(
        FirecrawlWebhookEventType.COMPLETED,
        FirecrawlWebhookEventType.FAILED,
    ),
    site_crawl=(
        FirecrawlWebhookEventType.BATCH_SCRAPE_STARTED,
        FirecrawlWebhookEventType.BATCH_SCRAPE_PAGE,
        FirecrawlWebhookEventType.BATCH_SCRAPE_COMPLETED,
        FirecrawlWebhookEventType.BATCH_SCRAPE_FAILED,
    ),
)
