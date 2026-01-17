"""Archived deprecated activity functions.

These functions are kept for backward compatibility but should not be used in new code.
Use the step functions from job_scrape_application.workflows.activities.step instead.
"""

from __future__ import annotations

# Re-export queue management activities
from .queue_management import (
    complete_scrape_urls,
    lease_scrape_url_batch,
    fail_listing_batch_urls,
    _is_spidercloud_listing_url,
    record_scrape_url_attempts,
)

# Re-export site management activities
from .site_management import (
    fetch_sites,
    lease_site,
    complete_site,
    fail_site,
    _looks_like_convex_id,
    _strip_none_values,
)

# Re-export convex operations activities
from .convex_operations import (
    filter_existing_job_urls,
    compute_urls_to_scrape,
    filter_new_job_urls,
    lookup_job_id_for_url,
    _convex_site_id,
    _convex_http_base_url,
)

# Re-export logging activities
from .logging_activities import (
    record_workflow_run,
    record_workflow_checkpoint,
    record_throughput_metrics,
    _build_log_message,
    _coerce_workflow_id,
    _short_preview,
)

# Re-export temporal @activity.defn functions
from .temporal_activities import (
    # Activity functions
    _scrape_spidercloud_greenhouse,
    scrape_site,
    start_firecrawl_webhook_scrape,
    crawl_site_fetchfox,
    scrape_site_fetchfox,
    scrape_site_firecrawl,
    fetch_greenhouse_listing,
    fetch_greenhouse_listing_firecrawl,
    process_spidercloud_job_batch,
    process_spidercloud_listing_batch,
    scrape_greenhouse_jobs,
    scrape_greenhouse_jobs_firecrawl,
    fetch_pending_firecrawl_webhooks,
    get_firecrawl_webhook_status,
    mark_firecrawl_webhook_processed,
    collect_firecrawl_job_result,
    store_scrape,
    process_pending_job_details_batch,
    batch_store_scrapes_background,
    # Helper functions
    select_scraper_for_site,
    _extract_job_urls_from_scrape,
)

__all__ = [
    # Queue management
    "complete_scrape_urls",
    "lease_scrape_url_batch",
    "fail_listing_batch_urls",
    "_is_spidercloud_listing_url",
    "record_scrape_url_attempts",
    # Site management
    "fetch_sites",
    "lease_site",
    "complete_site",
    "fail_site",
    "_looks_like_convex_id",
    "_strip_none_values",
    # Convex operations
    "filter_existing_job_urls",
    "compute_urls_to_scrape",
    "filter_new_job_urls",
    "lookup_job_id_for_url",
    "_convex_site_id",
    "_convex_http_base_url",
    # Logging
    "record_workflow_run",
    "record_workflow_checkpoint",
    "record_throughput_metrics",
    "_build_log_message",
    "_coerce_workflow_id",
    "_short_preview",
    # Temporal activity functions
    "_scrape_spidercloud_greenhouse",
    "scrape_site",
    "start_firecrawl_webhook_scrape",
    "crawl_site_fetchfox",
    "scrape_site_fetchfox",
    "scrape_site_firecrawl",
    "fetch_greenhouse_listing",
    "fetch_greenhouse_listing_firecrawl",
    "process_spidercloud_job_batch",
    "process_spidercloud_listing_batch",
    "scrape_greenhouse_jobs",
    "scrape_greenhouse_jobs_firecrawl",
    "fetch_pending_firecrawl_webhooks",
    "get_firecrawl_webhook_status",
    "mark_firecrawl_webhook_processed",
    "collect_firecrawl_job_result",
    "store_scrape",
    "process_pending_job_details_batch",
    "batch_store_scrapes_background",
    # Helper functions
    "select_scraper_for_site",
    "_extract_job_urls_from_scrape",
]
