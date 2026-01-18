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


@DBOS.pure_func
def _parse_listing_batch(batch: dict[str, Any]) -> ListingBatchInput:
    """Parse and validate listing batch input (deterministic)."""
    def _extract_row(row: Any) -> tuple[dict[str, Any], str, tuple[str, str | None], Any] | None:
        if not isinstance(row, dict):
            return None
        url_val = row.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            return None
        cleaned_url = url_val.strip()
        source_val = row.get("sourceUrl", "")
        pattern_val = row.get("pattern")
        pattern: str | None = pattern_val if isinstance(pattern_val, str) else None
        key = (source_val if isinstance(source_val, str) else "", pattern)
        return row, cleaned_url, key, source_val

    listing_rows: list[tuple[dict[str, Any], str, tuple[str, str | None], Any]] = [
        pair
        for row in batch.get("urls", [])
        for pair in [_extract_row(row)]
        if pair is not None
    ]
    entries = [row for row, _, _, _ in listing_rows]
    source_url_hint = next(
        (
            source_val
            for _, _, _, source_val in listing_rows
            if isinstance(source_val, str) and source_val
        ),
        "",
    )
    keys_in_order = list(dict.fromkeys(key for _, _, key, _ in listing_rows))
    groups = {
        key: [cleaned_url for _, cleaned_url, key_val, _ in listing_rows if key_val == key]
        for key in keys_in_order
    }
    posted_at_pairs = [
        (key, normalize_url(cleaned_url) or cleaned_url, int(posted_at_val))
        for row, cleaned_url, key, _ in listing_rows
        for posted_at_val in [row.get("postedAt")]
        if isinstance(posted_at_val, (int, float))
    ]
    posted_at_keys = list(dict.fromkeys(key for key, _, _ in posted_at_pairs))
    posted_at_groups = {
        key: {
            url: posted_at
            for key_val, url, posted_at in posted_at_pairs
            if key_val == key
        }
        for key in posted_at_keys
    }

    return ListingBatchInput(
        entries=entries,
        source_url=source_url_hint,
        groups=groups,
        posted_at_groups=posted_at_groups,
    )


@DBOS.pure_func
def _extract_job_urls_from_scrape(scrape_payload: dict[str, Any]) -> list[str]:
    """Extract job URLs from scrape response (deterministic)."""
    items = scrape_payload.get("items")
    if not isinstance(items, dict):
        return []
    source_url = scrape_payload.get("sourceUrl")
    handler = get_site_handler(source_url) if isinstance(source_url, str) and source_url else None

    # Check job_urls field first (populated by SpiderCloud scraper)
    job_urls = items.get("job_urls")
    if isinstance(job_urls, list):
        job_url_candidates = [
            url.strip()
            for url in job_urls
            if isinstance(url, str) and url.strip()
        ]
    else:
        job_url_candidates = []

    # Check normalized items for URLs
    normalized = items.get("normalized") or items.get("normalizedSample")
    if isinstance(normalized, list):
        normalized_candidates = [
            url.strip()
            for item in normalized
            if isinstance(item, dict)
            for url in [item.get("url") or item.get("job_url") or item.get("absolute_url")]
            if isinstance(url, str) and url.strip()
        ]
    else:
        normalized_candidates = []

    normalized_urls = [
        url
        for url in dict.fromkeys(normalized_candidates)
        if url not in job_url_candidates
    ]

    # Also check raw items
    raw = items.get("raw")
    if isinstance(raw, list):
        raw_candidates = [
            url.strip()
            for item in raw
            if isinstance(item, dict)
            for url in [item.get("url") or item.get("job_url")]
            if isinstance(url, str) and url.strip()
        ]
    else:
        raw_candidates = []

    raw_urls = [
        url
        for url in dict.fromkeys(raw_candidates)
        if url not in job_url_candidates and url not in normalized_urls
    ]

    urls = job_url_candidates + normalized_urls + raw_urls
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


@DBOS.pure_func
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
    unique_urls = list(dict.fromkeys(urls))

    def _classify_url(url: str) -> tuple[str, str] | None:
        if handler and handler.is_listing_url(url):
            return None
        if handler:
            filtered = handler.filter_job_urls([url])
            if not filtered:
                return ("invalid", url)
            candidate = filtered[0]
            if handler.name == "greenhouse":
                candidate = handler.get_api_uri(candidate, source_url=source_url) or candidate
            elif handler.name in {"microsoft_careers", "workday"}:
                candidate = handler.get_api_uri(candidate) or candidate
            return ("valid", candidate)
        return ("valid", url)

    results = [
        result
        for url in unique_urls
        for result in [_classify_url(url)]
        if result is not None
    ]
    valid_urls = [value for kind, value in results if kind == "valid"]
    invalid_urls = [value for kind, value in results if kind == "invalid"]
    return valid_urls, invalid_urls


@DBOS.pure_func
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
