from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except ImportError:
    # Temporal workflow sandbox may not have third-party modules available;
    # fall back silently when dotenv isn't installed.
    def load_dotenv(*args: object, **kwargs: object) -> bool:  # type: ignore[return-type]
        return False


load_dotenv()


@dataclass
class Settings:
    temporal_address: str = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace: str = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue: str = os.getenv("TEMPORAL_TASK_QUEUE", "scraper-task-queue")
    firecrawl_webhook_recheck_hours: int = int(os.getenv("FIRECRAWL_WEBHOOK_RECHECK_HOURS", "23"))
    firecrawl_webhook_timeout_hours: int = int(os.getenv("FIRECRAWL_WEBHOOK_TIMEOUT_HOURS", "24"))

    # Convex deployment URL for the ConvexClient (e.g., https://your-app.convex.cloud)
    convex_url: str | None = os.getenv("CONVEX_URL")

    # Legacy HTTP router base (still accepted by some scripts/tests)
    convex_http_url: str | None = os.getenv("CONVEX_HTTP_URL")

    # API key for FetchFox SDK
    fetchfox_api_key: str | None = os.getenv("FETCHFOX_API_KEY")

    # API key for Firecrawl SDK (preferred scraper)
    firecrawl_api_key: str | None = os.getenv("FIRECRAWL_API_KEY")

    # API key for SpiderCloud (streaming markdown scraping)
    spider_api_key: str | None = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")


settings = Settings()
