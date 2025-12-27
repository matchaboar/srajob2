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


def _env_flag(name: str, default: str = "false") -> bool:
    value = os.getenv(name, default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    temporal_address: str = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace: str = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue: str = os.getenv("TEMPORAL_TASK_QUEUE", "scraper-task-queue")
    job_details_task_queue: str | None = os.getenv("TEMPORAL_JOB_DETAILS_TASK_QUEUE")
    worker_role: str = os.getenv("TEMPORAL_WORKER_ROLE", "all")
    firecrawl_webhook_recheck_hours: int = int(os.getenv("FIRECRAWL_WEBHOOK_RECHECK_HOURS", "23"))
    firecrawl_webhook_timeout_hours: int = int(os.getenv("FIRECRAWL_WEBHOOK_TIMEOUT_HOURS", "24"))
    webhook_wait_logger_interval_seconds: int = int(
        os.getenv("WEBHOOK_WAIT_LOG_INTERVAL_SECONDS", "60")
    )
    schedule_audit_verbose: bool = _env_flag("SCHEDULE_AUDIT_VERBOSE", "false")
    monitor_rpc_timeout_seconds: int = int(
        os.getenv("TEMPORAL_MONITOR_RPC_TIMEOUT_SECONDS", "10")
    )

    # Convex deployment URL for the ConvexClient (e.g., https://your-app.convex.cloud)
    convex_url: str | None = os.getenv("CONVEX_URL")

    # Legacy HTTP router base (e.g., https://your-app.convex.site)
    convex_http_url: str | None = os.getenv("CONVEX_HTTP_URL")

    # API key for FetchFox SDK
    fetchfox_api_key: str | None = os.getenv("FETCHFOX_API_KEY")

    # API key for Firecrawl SDK (preferred scraper)
    firecrawl_api_key: str | None = os.getenv("FIRECRAWL_API_KEY")

    # API key for SpiderCloud (streaming markdown scraping)
    spider_api_key: str | None = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")

    # PostHog logging (OTLP) configuration
    posthog_project_api_key: str | None = os.getenv("POSTHOG_PROJECT_API_KEY")
    posthog_logs_endpoint: str | None = os.getenv("POSTHOG_LOGS_ENDPOINT")
    posthog_region: str | None = os.getenv("POSTHOG_REGION")
    posthog_project_id: str | None = os.getenv("POSTHOG_PROJECT_ID")
    posthog_exception_autocapture: bool = _env_flag("POSTHOG_EXCEPTION_AUTOCAPTURE", "true")
    posthog_disabled: bool = _env_flag("POSTHOG_DISABLED", "false") or _env_flag(
        "POSTHOG_DISABLE", "false"
    )


settings = Settings()
