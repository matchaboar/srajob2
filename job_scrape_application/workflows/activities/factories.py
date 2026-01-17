from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from firecrawl import Firecrawl

from ...config import settings
from ...components.models import extract_greenhouse_job_urls, load_greenhouse_board
from ..helpers.provider import (
    build_request_snapshot,
    log_provider_dispatch,
    log_sync_response,
    mask_secret,
    sanitize_headers,
)
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
from .step import log_scrape_error

if TYPE_CHECKING:
    from .types import Site

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


def select_scraper_for_site(
    site: Site,
    *,
    make_fetchfox: Callable[[], BaseScraper],
    make_firecrawl: Callable[[], BaseScraper],
    make_spidercloud: Callable[[], BaseScraper],
) -> Tuple[BaseScraper, Optional[list[str]]]:
    """Return the scraper instance and any precomputed skip URLs for a site."""

    site_type = (site.get("type") or "general").lower()
    preferred = (site.get("scrapeProvider") or "").lower()
    if site_type in {"greenhouse", "avature"} and not preferred:
        preferred = "spidercloud"
    firecrawl_enabled = settings.enable_firecrawl and bool(settings.firecrawl_api_key)
    fetchfox_enabled = settings.enable_fetchfox and bool(settings.fetchfox_api_key)

    if not preferred:
        if settings.spider_api_key:
            preferred = "spidercloud"
        elif firecrawl_enabled:
            preferred = "firecrawl"
        else:
            preferred = "fetchfox"

    factories: Dict[str, Callable[[], BaseScraper]] = {
        "fetchfox": make_fetchfox,
        "fetchfox_spidercloud": make_fetchfox,
        "firecrawl": make_firecrawl,
        "spidercloud": make_spidercloud,
    }

    if preferred not in factories:
        preferred = "fetchfox"

    if preferred == "spidercloud":
        if settings.spider_api_key:
            return factories["spidercloud"](), None
        if firecrawl_enabled:
            skip_urls = fetch_seen_urls_for_site(site["url"], site.get("pattern"))
            return factories["firecrawl"](), skip_urls
        preferred = "fetchfox"

    if preferred == "firecrawl":
        if firecrawl_enabled:
            skip_urls = fetch_seen_urls_for_site(site["url"], site.get("pattern"))
            return factories["firecrawl"](), skip_urls
        # Fall back to fetchfox if no Firecrawl key
        preferred = "fetchfox"

    scraper = factories[preferred]()
    if preferred == "fetchfox" and fetchfox_enabled:
        return scraper, None
    if preferred == "fetchfox" and not fetchfox_enabled and settings.spider_api_key:
        return factories["spidercloud"](), None
    if preferred == "fetchfox" and not fetchfox_enabled and firecrawl_enabled:
        skip_urls = fetch_seen_urls_for_site(site["url"], site.get("pattern"))
        return factories["firecrawl"](), skip_urls

    return scraper, None


# ============================================================================
# Convenience wrapper functions for creating scrapers with default dependencies
# These are the functions that tests commonly monkeypatch
# ============================================================================


def _make_fetchfox_scraper() -> FetchfoxScraper:
    """Create a FetchfoxScraper with default dependencies."""
    return build_fetchfox_scraper(
        build_request_snapshot=build_request_snapshot,
        log_provider_dispatch=log_provider_dispatch,
        log_sync_response=log_sync_response,
    )


def _make_firecrawl_scraper() -> FirecrawlScraper:
    """Create a FirecrawlScraper with default dependencies.

    DEPRECATED: Firecrawl scraper is deprecated and not used in DBOS workflows.
    Use SpiderCloud scraper instead.

    Raises:
        NotImplementedError: Always raised. Firecrawl is deprecated.
    """
    raise NotImplementedError(
        "FirecrawlScraper is deprecated. Use SpiderCloudScraper instead. "
        "If you need Firecrawl for testing, import from "
        "job_scrape_application.workflows.activities._archive.temporal_activities"
    )


def _make_spidercloud_scraper() -> SpiderCloudScraper:
    """Create a SpiderCloudScraper with default dependencies."""
    return build_spidercloud_scraper(
        mask_secret=mask_secret,
        sanitize_headers=sanitize_headers,
        build_request_snapshot=build_request_snapshot,
        log_provider_dispatch=log_provider_dispatch,
        log_sync_response=log_sync_response,
        trim_scrape_for_convex=trim_scrape_for_convex,
    )


def select_scraper_for_site_with_defaults(
    site: Site,
) -> Tuple[BaseScraper, Optional[List[str]]]:
    """Return the scraper instance and any precomputed skip URLs for a site.

    Uses the default _make_* functions which can be patched in tests.
    """
    scraper, skip_urls = select_scraper_for_site(
        site,
        make_fetchfox=_make_fetchfox_scraper,
        make_firecrawl=_make_firecrawl_scraper,
        make_spidercloud=_make_spidercloud_scraper,
    )

    # Allow callers/tests to monkeypatch fetch_seen_urls_for_site and still forward skip URLs
    if isinstance(scraper, FirecrawlScraper) and not skip_urls:
        url = site.get("url")
        if url:
            skip_urls = fetch_seen_urls_for_site(url, site.get("pattern"))

    return scraper, skip_urls
