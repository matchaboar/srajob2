"""DBOS step function for enqueuing listing sites from Convex."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from dbos import DBOS

from ...workflows.site_handlers import get_site_handler
from ..queue import enqueue_scrape_urls
from ..sqlite import now_ms

SITES_REFRESH_SECONDS = 300

_SITES_CACHE: tuple[int, list[dict[str, object]]] | None = None


def _dedupe_urls(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _limit_listing_urls(urls: list[str], limit: int | None) -> list[str]:
    if not urls:
        return []
    deduped = _dedupe_urls(urls)
    if not limit or limit <= 0:
        return deduped
    filtered: list[str] = []
    for url in deduped:
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
        except Exception:
            params = {}
        page_val = None
        for key in ("page", "from", "start", "offset"):
            value = params.get(key, [None])[0]
            if value is None:
                continue
            try:
                page_val = int(value)
            except Exception:
                page_val = None
            if page_val is not None:
                break
        if page_val is None or page_val <= limit:
            filtered.append(url)
    if len(filtered) <= limit:
        return filtered
    return filtered[:limit]


def reset_sites_cache() -> None:
    """Reset the sites cache."""
    global _SITES_CACHE
    _SITES_CACHE = None


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def enqueue_listing_sites() -> int:
    """Fetch enabled sites from Convex and enqueue their listing URLs.

    Returns:
        int: Number of URLs queued
    """
    from ...services.convex_client import convex_query

    global _SITES_CACHE
    now = now_ms()
    sites: list[dict[str, object]] | None = None
    if _SITES_CACHE is not None:
        fetched_at, cached_sites = _SITES_CACHE
        if now - fetched_at < SITES_REFRESH_SECONDS * 1000:
            sites = cached_sites
    if sites is None:
        fetched = convex_query("router:listSites", {"enabledOnly": True})

        if not isinstance(fetched, list):
            return 0
        sites = [site for site in fetched if isinstance(site, dict)]
        _SITES_CACHE = (now, sites)
    queued = 0
    for site in sites:
        url = site.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        site_type_raw = site.get("type")
        site_type: str | None = site_type_raw if isinstance(site_type_raw, str) else None
        pagination_limit = site.get("paginationLimit")
        if isinstance(pagination_limit, (int, float)):
            pagination_limit = max(0, int(pagination_limit))
        else:
            pagination_limit = 0
        handler = get_site_handler(url, site_type)
        listing_urls = [url.strip()]
        if handler:
            pagination_urls = handler.get_pagination_urls_from_listing(url)
            if pagination_urls:
                listing_urls.extend(pagination_urls)
        listing_urls = _limit_listing_urls(listing_urls, pagination_limit)
        if not listing_urls:
            continue
        payload = {
            "urls": listing_urls,
            "sourceUrl": url,
            "provider": site.get("scrapeProvider") or "spidercloud",
            "siteId": site.get("_id"),
            "pattern": site.get("pattern"),
            "urlTypes": ["listing"] * len(listing_urls),
        }
        result = enqueue_scrape_urls(payload)
        if isinstance(result, dict) and isinstance(result.get("queued"), int):
            queued += int(result["queued"])
    return queued
