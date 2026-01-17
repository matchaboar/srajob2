"""DBOS workflows for job scraping."""

from __future__ import annotations

from .enqueue_scheduled_listings import ScheduleResult, enqueue_scheduled_listings
from .process_pending_heuristics import process_pending_job_details_batch
from .process_spidercloud_job_batch import process_spidercloud_job_batch
from .scheduled_listing_enqueue import scheduled_listing_enqueue
from .scrape_job_detail_batch import DetailScrapeResult, scrape_job_detail_batch
from .scrape_listing_batch import ListingScrapeResult, scrape_listing_batch
from .store_scrape import store_scrape

__all__ = [
    "DetailScrapeResult",
    "ListingScrapeResult",
    "ScheduleResult",
    "enqueue_scheduled_listings",
    "process_pending_job_details_batch",
    "process_spidercloud_job_batch",
    "scheduled_listing_enqueue",
    "scrape_job_detail_batch",
    "scrape_listing_batch",
    "store_scrape",
]
