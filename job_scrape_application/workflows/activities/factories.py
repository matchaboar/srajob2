from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from ...config import settings
from ...components.models import extract_greenhouse_job_urls, load_greenhouse_board
from ..helpers.scrape_utils import (
    build_firecrawl_schema,
    build_job_template,
    extract_raw_body_from_fetchfox_result,
    fetch_seen_urls_for_site,
    normalize_fetchfox_items,
    normalize_firecrawl_items,
    trim_scrape_for_convex,
)
from ..scrapers import (
    BaseScraper,
    FetchfoxDependencies,
    FetchfoxScraper,
    FirecrawlDependencies,
    FirecrawlScraper,
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from .constants import FIRECRAWL_CACHE_MAX_AGE_MS

Site = Dict[str, Any]


def build_fetchfox_scraper(
    *,
    build_request_snapshot: Callable[..., Dict[str, Any]],
    log_provider_dispatch: Callable[..., None],
    log_sync_response: Callable[..., None],
) -> FetchfoxScraper:
    return FetchfoxScraper(
        FetchfoxDependencies(
            fetch_seen_urls_for_site=fetch_seen_urls_for_site,
            build_job_template=build_job_template,
            build_request_snapshot=build_request_snapshot,
            log_provider_dispatch=log_provider_dispatch,
            log_sync_response=log_sync_response,
            normalize_fetchfox_items=normalize_fetchfox_items,
            trim_scrape_for_convex=trim_scrape_for_convex,
            settings=settings,
            load_greenhouse_board=load_greenhouse_board,
            extract_greenhouse_job_urls=extract_greenhouse_job_urls,
            extract_raw_body_from_fetchfox_result=extract_raw_body_from_fetchfox_result,
        )
    )


def build_firecrawl_scraper(
    *,
    start_firecrawl_webhook_scrape: Callable[[Site], Any],
    log_scrape_error: Callable[[Dict[str, Any]], Any],
    build_request_snapshot: Callable[..., Dict[str, Any]],
    log_provider_dispatch: Callable[..., None],
    log_sync_response: Callable[..., None],
    firecrawl_cls: Any,
) -> FirecrawlScraper:
    return FirecrawlScraper(
        FirecrawlDependencies(
            start_firecrawl_webhook_scrape=start_firecrawl_webhook_scrape,
            build_request_snapshot=build_request_snapshot,
            settings=settings,
            firecrawl_cls=firecrawl_cls,
            build_firecrawl_schema=build_firecrawl_schema,
            log_provider_dispatch=log_provider_dispatch,
            log_sync_response=log_sync_response,
            trim_scrape_for_convex=trim_scrape_for_convex,
            normalize_firecrawl_items=normalize_firecrawl_items,
            log_scrape_error=log_scrape_error,
            load_greenhouse_board=load_greenhouse_board,
            extract_greenhouse_job_urls=extract_greenhouse_job_urls,
            firecrawl_cache_max_age_ms=FIRECRAWL_CACHE_MAX_AGE_MS,
        )
    )


def build_spidercloud_scraper(
    *,
    mask_secret: Callable[..., Any],
    sanitize_headers: Callable[..., Any],
    build_request_snapshot: Callable[..., Dict[str, Any]],
    log_provider_dispatch: Callable[..., None],
    log_sync_response: Callable[..., None],
    trim_scrape_for_convex: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> SpiderCloudScraper:
    return SpiderCloudScraper(
        SpidercloudDependencies(
            mask_secret=mask_secret,
            sanitize_headers=sanitize_headers,
            build_request_snapshot=build_request_snapshot,
            log_dispatch=log_provider_dispatch,
            log_sync_response=log_sync_response,
            trim_scrape_for_convex=trim_scrape_for_convex,
            settings=settings,
            fetch_seen_urls_for_site=fetch_seen_urls_for_site,
        )
    )


async def select_scraper_for_site(
    site: Site,
    *,
    make_fetchfox: Callable[[], BaseScraper],
    make_firecrawl: Callable[[], BaseScraper],
    make_spidercloud: Callable[[], BaseScraper],
) -> Tuple[BaseScraper, Optional[list[str]]]:
    """Return the scraper instance and any precomputed skip URLs for a site."""

    site_type = (site.get("type") or "general").lower()
    preferred = (site.get("scrapeProvider") or "").lower()
    if site_type == "greenhouse" and not preferred:
        preferred = "spidercloud"

    factories: Dict[str, Callable[[], BaseScraper]] = {
        "fetchfox": make_fetchfox,
        "firecrawl": make_firecrawl,
        "spidercloud": make_spidercloud,
    }

    if preferred not in factories:
        preferred = "fetchfox"

    if preferred == "spidercloud":
        if settings.spider_api_key:
            return factories["spidercloud"](), None
        if settings.firecrawl_api_key:
            skip_urls = await fetch_seen_urls_for_site(site["url"], site.get("pattern"))
            return factories["firecrawl"](), skip_urls
        preferred = "fetchfox"

    if preferred == "firecrawl":
        if settings.firecrawl_api_key:
            skip_urls = await fetch_seen_urls_for_site(site["url"], site.get("pattern"))
            return factories["firecrawl"](), skip_urls
        # Fall back to fetchfox if no Firecrawl key
        preferred = "fetchfox"

    scraper = factories[preferred]()
    if preferred == "fetchfox" and settings.fetchfox_api_key:
        return scraper, None
    if preferred == "fetchfox" and not settings.fetchfox_api_key and settings.spider_api_key:
        return factories["spidercloud"](), None
    if preferred == "fetchfox" and not settings.fetchfox_api_key and settings.firecrawl_api_key:
        skip_urls = await fetch_seen_urls_for_site(site["url"], site.get("pattern"))
        return factories["firecrawl"](), skip_urls

    return scraper, None
