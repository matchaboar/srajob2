"""DBOS workflow for scraping listing URLs and enqueuing job detail URLs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbos import DBOS

from ..activities.step import (
    emit_scrape_telemetry_step,
    filter_new_job_urls,
    log_scrape_error,
    record_scrape_url_attempts,
    resolve_pagination_limit_step,
    scrape_listing_urls,
)
from ..helpers.job_url_extractor import extract_job_urls_from_scrape as extract_job_urls_from_payload
from ..helpers.step import fetch_seen_urls_for_site
from ..helpers.link_extractors import gather_strings, normalize_url
from ..site_handlers import get_site_handler
from ...dbos_runtime.step import (
    complete_scrape_urls_step,
    enqueue_scrape_urls_step,
)
from ..result import Result, Success
from ..spidercloud.types import UnrenderedHttpException


@dataclass
class ListingScrapeResult:
    """Result of a listing batch scrape."""

    queued: int
    completed: int
    source_url: str


@dataclass
class ListingBatchInput:
    """Parsed input for listing batch workflow."""

    entries: list[dict[str, Any]]
    source_url: str
    groups: dict[tuple[str, str | None], list[str]]
    posted_at_groups: dict[tuple[str, str | None], dict[str, int]]


def _parse_listing_batch(batch: dict[str, Any]) -> ListingBatchInput:
    """Parse and validate listing batch input (deterministic)."""
    entries: list[dict[str, Any]] = []
    groups: dict[tuple[str, str | None], list[str]] = {}
    posted_at_groups: dict[tuple[str, str | None], dict[str, int]] = {}
    source_url_hint = ""

    for row in batch.get("urls", []):
        if not isinstance(row, dict):
            continue
        url_val = row.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            continue

        cleaned_url = url_val.strip()
        entries.append(row)

        source_val = row.get("sourceUrl", "")
        if isinstance(source_val, str) and source_val and not source_url_hint:
            source_url_hint = source_val

        pattern_val = row.get("pattern")
        pattern: str | None = pattern_val if isinstance(pattern_val, str) else None

        key = (source_val if isinstance(source_val, str) else "", pattern)
        groups.setdefault(key, []).append(cleaned_url)

        posted_at_val = row.get("postedAt")
        if isinstance(posted_at_val, (int, float)):
            mapping = posted_at_groups.setdefault(key, {})
            mapping[normalize_url(cleaned_url) or cleaned_url] = int(posted_at_val)

    return ListingBatchInput(
        entries=entries,
        source_url=source_url_hint,
        groups=groups,
        posted_at_groups=posted_at_groups,
    )


def _extract_job_urls_from_scrape(scrape_payload: dict[str, Any]) -> list[str]:
    """Extract job URLs from scrape response (deterministic)."""
    urls: list[str] = []
    items = scrape_payload.get("items")
    if not isinstance(items, dict):
        return urls
    source_url = scrape_payload.get("sourceUrl")
    handler = get_site_handler(source_url) if isinstance(source_url, str) and source_url else None

    # Check job_urls field first (populated by SpiderCloud scraper)
    job_urls = items.get("job_urls")
    if isinstance(job_urls, list):
        for url in job_urls:
            if isinstance(url, str) and url.strip():
                urls.append(url.strip())

    # Check normalized items for URLs
    normalized = items.get("normalized") or items.get("normalizedSample")
    if isinstance(normalized, list):
        for item in normalized:
            if not isinstance(item, dict):
                continue
            url = item.get("url") or item.get("job_url") or item.get("absolute_url")
            if isinstance(url, str) and url.strip() and url not in urls:
                urls.append(url.strip())

    # Also check raw items
    raw = items.get("raw")
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            url = item.get("url") or item.get("job_url")
            if isinstance(url, str) and url.strip() and url not in urls:
                urls.append(url.strip())

    if urls:
        if handler:
            has_detail = any(not handler.is_listing_url(url) for url in urls)
            if has_detail:
                return urls
        elif isinstance(source_url, str) and source_url:
            normalized_source = normalize_url(source_url) or source_url
            for url in urls:
                normalized_url = normalize_url(url) or url
                if normalized_url != normalized_source:
                    return urls
        else:
            return urls

    try:
        return extract_job_urls_from_payload(scrape_payload)
    except Exception:
        return urls


def _filter_valid_job_urls(
    urls: list[str],
    source_url: str,
    pattern: str | None,
) -> tuple[list[str], list[str]]:
    """Filter and validate job URLs (deterministic).

    Returns:
        Tuple of (valid_job_urls, invalid_urls)
    """
    if not urls:
        return [], []

    handler = get_site_handler(source_url) if source_url else None
    valid_urls: list[str] = []
    invalid_urls: list[str] = []
    seen: set[str] = set()

    for url in urls:
        # Deduplicate
        if url in seen:
            continue
        seen.add(url)

        # Skip listing URLs - we only want detail URLs
        if handler and handler.is_listing_url(url):
            continue

        # Apply handler's URL filtering/normalization
        if handler:
            filtered = handler.filter_job_urls([url])
            if filtered:
                candidate = filtered[0]
                if handler.name == "greenhouse":
                    candidate = handler.get_api_uri(candidate, source_url=source_url) or candidate
                elif handler.name in {"microsoft_careers", "workday"}:
                    candidate = handler.get_api_uri(candidate) or candidate
                valid_urls.append(candidate)
            else:
                invalid_urls.append(url)
        else:
            valid_urls.append(url)

    return valid_urls, invalid_urls


def _raise_if_unrendered_listing(scrape_payload: dict[str, Any], source_url: str) -> None:
    handler = get_site_handler(source_url) if source_url else None
    if not handler or not handler.is_listing_url(source_url):
        return
    items = scrape_payload.get("items")
    if not isinstance(items, dict):
        return
    raw_items = items.get("raw")
    if raw_items is None:
        return
    for candidate in gather_strings(raw_items):
        if not isinstance(candidate, str):
            continue
        if "<" not in candidate or ">" not in candidate:
            continue
        if handler.is_unrendered_listing_html(candidate):
            raise UnrenderedHttpException(source_url)


@DBOS.workflow()
async def scrape_listing_batch(
    batch: dict[str, Any],
) -> Result[ListingScrapeResult]:
    """Scrape listing URLs and enqueue extracted job detail URLs.

    This workflow:
    1. Parses the input batch
    2. Records scrape attempts for telemetry
    3. Calls SpiderCloud to scrape listing pages
    4. Extracts job URLs from the response
    5. Filters out already-seen URLs
    6. Enqueues new job URLs for detail scraping
    7. Marks listing URLs as completed

    Args:
        batch: Dict with "urls" list containing listing URL entries

    Returns:
        Success[ListingScrapeResult] on success, Failure on non-retryable error
    """
    # Parse input (deterministic)
    parsed = _parse_listing_batch(batch)

    if not parsed.groups:
        return Success(ListingScrapeResult(queued=0, completed=0, source_url=parsed.source_url))

    # Record attempts for telemetry
    attempt_entries: list[dict[str, Any]] = []
    for entry in parsed.entries:
        attempt_entry: dict[str, Any] = {
            "url": entry.get("url"),
            "sourceUrl": entry.get("sourceUrl"),
        }
        provider = entry.get("provider")
        if isinstance(provider, str):
            attempt_entry["provider"] = provider
        attempts_val = entry.get("attempts")
        if isinstance(attempts_val, (int, float)):
            attempt_entry["attempts"] = attempts_val
        attempt_entries.append(attempt_entry)
    record_scrape_url_attempts(attempt_entries)

    queued_total = 0
    completed_entries: list[dict[str, Any]] = []
    failed_entries: list[dict[str, Any]] = []

    # Process each group
    for (source_url, pattern), urls in parsed.groups.items():
        try:
            # Step: Scrape listing URLs
            posted_at_by_url = parsed.posted_at_groups.get((source_url, pattern))
            scrape_result = await scrape_listing_urls(
                urls=urls,
                source_url=source_url,
                pattern=pattern,
                posted_at_by_url=posted_at_by_url,
            )

            # Deterministic: Extract URLs from response
            scrape_payload = scrape_result.get("scrape") or scrape_result
            if not isinstance(scrape_payload, dict):
                scrape_payload = {}

            if source_url:
                _raise_if_unrendered_listing(scrape_payload, source_url)

            extracted_urls = _extract_job_urls_from_scrape(scrape_payload)

            # Deterministic: Filter and validate
            job_urls, invalid_urls = _filter_valid_job_urls(
                extracted_urls, source_url, pattern
            )

            site_id = None
            for entry in parsed.entries:
                if entry.get("sourceUrl") == source_url:
                    site_id = entry.get("siteId")
                    break

            if not job_urls:
                log_scrape_error(
                    {
                        "error": "Zero job detail URLs extracted from listing scrape",
                        "event": "scrape.listing.zero_urls",
                        "sourceUrl": source_url,
                        "siteId": site_id,
                        "metadata": {
                            "listingUrlCount": len(urls),
                            "extractedUrlCount": len(extracted_urls),
                            "invalidUrlCount": len(invalid_urls),
                            "pattern": pattern,
                            "extractedUrlSample": extracted_urls[:5],
                            "invalidUrlSample": invalid_urls[:5],
                        },
                    }
                )
                # No URLs extracted - mark as failed
                for entry in parsed.entries:
                    if entry.get("sourceUrl") == source_url:
                        failed_entries.append({
                            "url": entry.get("url"),
                            "id": entry.get("_id"),
                        })
                continue

            # Step: Get pagination limit
            if site_id:
                try:
                    resolve_pagination_limit_step(site_id)
                except Exception:
                    pass

            # Step: Filter out seen URLs
            if source_url:
                try:
                    seen_urls = fetch_seen_urls_for_site(source_url, pattern, job_urls)
                    if seen_urls:
                        seen_set = set(seen_urls)
                        job_urls = [u for u in job_urls if u not in seen_set]
                except Exception:
                    pass

            # Step: Filter out URLs already in jobs table
            if job_urls:
                try:
                    new_urls = filter_new_job_urls(job_urls)
                    job_urls = new_urls
                except Exception:
                    pass

            if extracted_urls and not job_urls:
                emit_scrape_telemetry_step(
                    event="all_jobs_skipped",
                    level="warn",
                    site_url=source_url or "",
                    data={
                        "reason": "all_jobs_skipped",
                        "sourceUrl": source_url,
                        "siteId": site_id,
                        "pattern": pattern,
                        "extractedUrlCount": len(extracted_urls),
                    },
                )

            # Step: Enqueue detail URLs
            if job_urls:
                enqueue_result = enqueue_scrape_urls_step(
                    urls=job_urls,
                    source_url=source_url,
                    provider="spidercloud",
                    site_id=site_id,
                    pattern=pattern,
                    url_types=["detail"] * len(job_urls),
                )
                queued_count = enqueue_result.get("queued", 0)
                queued_total += queued_count

            # Mark as completed
            for entry in parsed.entries:
                if entry.get("sourceUrl") == source_url:
                    completed_entries.append({
                        "url": entry.get("url"),
                        "id": entry.get("_id"),
                    })

        except UnrenderedHttpException:
            raise
        except Exception as exc:
            # Mark group as failed
            for entry in parsed.entries:
                if entry.get("sourceUrl") == source_url:
                    failed_entries.append({
                        "url": entry.get("url"),
                        "id": entry.get("_id"),
                    })

            # Emit telemetry for failure
            try:
                emit_scrape_telemetry_step(
                    event="scrape.listing.workflow_error",
                    level="error",
                    site_url=source_url,
                    data={"error": str(exc)[:200]},
                )
            except Exception:
                pass

    # Step: Complete listing URLs
    if completed_entries:
        complete_scrape_urls_step(
            items=completed_entries,
            status="completed",
        )

    if failed_entries:
        complete_scrape_urls_step(
            items=failed_entries,
            status="failed",
            error="scrape.listing.zero_urls",
        )

    return Success(ListingScrapeResult(
        queued=queued_total,
        completed=len(completed_entries),
        source_url=parsed.source_url,
    ))
