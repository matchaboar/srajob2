"""DBOS workflow for scraping job detail URLs and storing to Convex."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbos import DBOS

from ..activities.step import (
    emit_scrape_telemetry_step,
    filter_new_job_urls,
    ingest_jobs_from_scrape_step,
    log_scrape_error,
    record_scrape_url_attempts,
    scrape_job_details,
    store_job_descriptions_step,
)
from ..helpers.link_extractors import normalize_url
from ...dbos_runtime.step import (
    complete_scrape_urls_step,
)
from ..result import Failure, Result, Success


@dataclass
class DetailScrapeResult:
    """Result of a detail batch scrape."""

    stored: int
    invalid: int
    failed: int
    source_url: str


@dataclass
class DetailBatchInput:
    """Parsed input for detail batch workflow."""

    entries: list[dict[str, Any]]
    source_url: str
    urls: list[str]
    url_to_entry: dict[str, dict[str, Any]]
    site_id: str | None
    pattern: str | None
    posted_at_by_url: dict[str, int]
    provider: str | None
    workflow_name: str | None


@DBOS.pure_func
def _parse_detail_batch(batch: dict[str, Any]) -> DetailBatchInput:
    """Parse and validate detail batch input (deterministic)."""
    def _extract_row(row: Any) -> tuple[dict[str, Any], str] | None:
        if not isinstance(row, dict):
            return None
        url_val = row.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            return None
        url_type = row.get("urlType")
        if isinstance(url_type, str) and url_type.lower() == "listing":
            return None
        return row, url_val.strip()

    detail_rows: list[tuple[dict[str, Any], str]] = [
        pair
        for row in batch.get("urls", [])
        for pair in [_extract_row(row)]
        if pair is not None
    ]
    entries = [row for row, _ in detail_rows]
    urls = [cleaned_url for _, cleaned_url in detail_rows]
    url_to_entry = {cleaned_url: row for row, cleaned_url in detail_rows}
    source_url_hint = next(
        (
            source_val
            for row, _ in detail_rows
            for source_val in [row.get("sourceUrl")]
            if isinstance(source_val, str) and source_val
        ),
        "",
    )
    site_id = next(
        (
            site_id_val.strip()
            for row, _ in detail_rows
            for site_id_val in [row.get("siteId")]
            if isinstance(site_id_val, str) and site_id_val.strip()
        ),
        None,
    )
    pattern = next(
        (
            pattern_val
            for row, _ in detail_rows
            for pattern_val in [row.get("pattern")]
            if isinstance(pattern_val, str)
        ),
        None,
    )
    provider = next(
        (
            provider_val.strip()
            for row, _ in detail_rows
            for provider_val in [row.get("provider")]
            if isinstance(provider_val, str) and provider_val.strip()
        ),
        None,
    )
    workflow_name = next(
        (
            workflow_val.strip()
            for row, _ in detail_rows
            for workflow_val in [row.get("workflowName") or row.get("workflow_name")]
            if isinstance(workflow_val, str) and workflow_val.strip()
        ),
        None,
    )
    posted_at_by_url = {
        (normalize_url(cleaned_url) or cleaned_url): int(posted_at_val)
        for row, cleaned_url in detail_rows
        for posted_at_val in [row.get("postedAt")]
        if isinstance(posted_at_val, (int, float))
    }

    return DetailBatchInput(
        entries=entries,
        source_url=source_url_hint,
        urls=urls,
        url_to_entry=url_to_entry,
        site_id=site_id,
        pattern=pattern,
        posted_at_by_url=posted_at_by_url,
        provider=provider,
        workflow_name=workflow_name,
    )


@DBOS.pure_func
def _normalize_job_fields(scrape_result: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract and normalize job data from scrape response (deterministic)."""
    scrape = scrape_result.get("scrape") or scrape_result
    if not isinstance(scrape, dict):
        return []

    items = scrape.get("items")
    if not isinstance(items, dict):
        return []

    normalized = items.get("normalized") or items.get("normalizedSample")
    if not isinstance(normalized, list):
        return []

    cost_milli_cents_total: int | None = None
    cost_val = scrape.get("costMilliCents")
    if isinstance(cost_val, (int, float)):
        cost_milli_cents_total = int(cost_val)
    else:
        items_cost = items.get("costMilliCents")
        if isinstance(items_cost, (int, float)):
            cost_milli_cents_total = int(items_cost)

    url_count = 0
    sub_urls = scrape.get("subUrls")
    if isinstance(sub_urls, list):
        url_count = len([u for u in sub_urls if isinstance(u, str) and u.strip()])
    if url_count <= 0:
        url_count = len(normalized)
    per_job_cost: int | None = None
    if cost_milli_cents_total is not None and url_count > 0:
        per_job_cost = int(cost_milli_cents_total / max(url_count, 1))

    keys = [
        "title",
        "company",
        "location",
        "description",
        "url",
        "apply_url",
        "posted_at",
        "compensation",
        "level",
        "remote",
        "cost_milli_cents",
    ]

    def _build_job(item: dict[str, Any]) -> dict[str, Any] | None:
        job = {key: item.get(key) for key in keys if item.get(key) is not None}
        if "cost_milli_cents" not in job and per_job_cost is not None:
            job = {**job, "cost_milli_cents": per_job_cost}
        if job.get("title") or job.get("url"):
            return job
        return None

    return [
        job
        for item in normalized
        if isinstance(item, dict)
        for job in [_build_job(item)]
        if job is not None
    ]


@DBOS.pure_func
def _identify_404_urls(scrape_result: dict[str, Any]) -> set[str]:
    """Identify URLs that returned 404 (deterministic)."""
    scrape = scrape_result.get("scrape") or scrape_result
    if not isinstance(scrape, dict):
        return set()

    items = scrape.get("items")
    if not isinstance(items, dict):
        return set()

    failed = items.get("failed")
    if not isinstance(failed, list):
        return set()

    def _is_404(entry: dict[str, Any]) -> bool:
        status = entry.get("status") or entry.get("httpStatus")
        reason = entry.get("reason")
        if isinstance(status, (int, float)) and int(status) == 404:
            return True
        return isinstance(reason, str) and "404" in reason.lower()

    return {
        url_val.strip()
        for entry in failed
        if isinstance(entry, dict)
        for url_val in [entry.get("url")]
        if isinstance(url_val, str) and url_val.strip() and _is_404(entry)
    }


@DBOS.pure_func
def _build_queue_item(url: str, parsed: DetailBatchInput) -> dict[str, Any]:
    entry = parsed.url_to_entry.get(url, {})
    row_id = entry.get("_id") or entry.get("id")
    extras = [
        ("id", row_id) if isinstance(row_id, str) else None,
        ("siteId", parsed.site_id) if parsed.site_id else None,
        ("provider", parsed.provider) if parsed.provider else None,
        ("sourceUrl", parsed.source_url) if parsed.source_url else None,
    ]
    extra_fields = {
        key: value
        for pair in extras
        if pair is not None
        for key, value in [pair]
    }
    return {
        "url": url,
        "urlType": "detail",
        **extra_fields,
    }


@DBOS.workflow()
async def scrape_job_detail_batch(
    batch: dict[str, Any],
    persist_scrapes: bool = True,
) -> Result[DetailScrapeResult]:
    """Scrape job detail URLs and store normalized data to Convex.

    This workflow:
    1. Parses the input batch
    2. Records scrape attempts for telemetry
    3. Filters out URLs already in the jobs table
    4. Calls SpiderCloud to scrape job detail pages
    5. Normalizes job fields
    6. Stores jobs to Convex
    7. Marks URLs as completed/failed/invalid

    Args:
        batch: Dict with "urls" list containing detail URL entries
        persist_scrapes: Whether to store scrapes to Convex (default True)

    Returns:
        Success[DetailScrapeResult] on success, Failure on non-retryable error
    """
    # Parse input (deterministic)
    parsed = _parse_detail_batch(batch)

    if not parsed.urls:
        log_scrape_error(
            {
                "error": "Zero job detail URLs to scrape",
                "event": "scrape.detail.zero_urls",
                "sourceUrl": parsed.source_url,
                "siteId": parsed.site_id,
                "metadata": {
                    "urlCount": 0,
                    "entryCount": len(parsed.entries),
                    "pattern": parsed.pattern,
                },
            }
        )
        return Success(DetailScrapeResult(
            stored=0, invalid=0, failed=0, source_url=parsed.source_url
        ))

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

    # Step: Filter out URLs already in jobs table
    urls_to_scrape = parsed.urls
    skipped_existing: list[str] = []

    try:
        new_urls = filter_new_job_urls(urls_to_scrape)
        new_urls_set = set(new_urls)
        skipped_existing = [u for u in urls_to_scrape if u not in new_urls_set]
        urls_to_scrape = [u for u in urls_to_scrape if u in new_urls_set]
    except Exception:
        # On error, proceed with all URLs
        pass

    # Step: Mark skipped URLs as completed
    if skipped_existing:
        skipped_items = [_build_queue_item(url, parsed) for url in skipped_existing]
        complete_scrape_urls_step(
            items=skipped_items,
            status="completed",
            error="already_exists_in_jobs",
        )

    if not urls_to_scrape:
        emit_scrape_telemetry_step(
            event="all_jobs_skipped",
            level="warn",
            site_url=parsed.source_url or "",
            data={
                "reason": "all_jobs_skipped",
                "sourceUrl": parsed.source_url,
                "siteId": parsed.site_id,
                "totalUrlCount": len(parsed.urls),
                "skippedExistingCount": len(skipped_existing),
                "skippedExistingSample": skipped_existing[:5],
            },
        )
        return Success(DetailScrapeResult(
            stored=0,
            invalid=0,
            failed=0,
            source_url=parsed.source_url,
        ))

    # Step: Scrape job details
    try:
        scrape_result = await scrape_job_details(
            urls=urls_to_scrape,
            source_url=parsed.source_url,
            pattern=parsed.pattern,
            posted_at_by_url=parsed.posted_at_by_url or None,
            site_id=parsed.site_id,
        )
    except Exception as exc:
        # Scrape failed completely - mark all as failed
        failed_items = [_build_queue_item(url, parsed) for url in urls_to_scrape]
        complete_scrape_urls_step(
            items=failed_items,
            status="failed",
            error=f"scrape_error: {str(exc)[:100]}",
        )
        return Failure(
            error_type="scrape_failed",
            message=str(exc)[:200],
        )

    # Identify 404 URLs (deterministic)
    http_404_urls = _identify_404_urls(scrape_result)

    # Step: Mark 404 URLs as failed
    if http_404_urls:
        failed_items = [_build_queue_item(url, parsed) for url in http_404_urls]
        complete_scrape_urls_step(
            items=failed_items,
            status="failed",
            error="http_404",
        )

    # Filter out 404 URLs from further processing
    remaining_urls = [u for u in urls_to_scrape if u not in http_404_urls]

    if not persist_scrapes:
        # Just return the scrape without storing
        return Success(DetailScrapeResult(
            stored=0,
            invalid=0,
            failed=len(http_404_urls),
            source_url=parsed.source_url,
        ))

    # Normalize job fields (deterministic)
    jobs = _normalize_job_fields(scrape_result)

    stored_count = 0
    invalid_count = 0
    completed_urls: list[str] = []
    invalid_urls: list[str] = []

    # Step: Store jobs to Convex
    if jobs:
        try:
            # Store jobs using ingest step (handles description preview truncation)
            ingest_jobs_from_scrape_step(jobs, site_id=parsed.site_id)
            stored_count = len(jobs)
            store_job_descriptions_step(
                jobs,
                source_url=parsed.source_url,
                provider=parsed.provider,
                workflow_name=parsed.workflow_name,
            )
            # All remaining URLs processed
            completed_urls = remaining_urls
        except Exception as exc:
            # Storage failed - emit telemetry
            try:
                emit_scrape_telemetry_step(
                    event="scrape.detail.storage_error",
                    level="error",
                    site_url=parsed.source_url,
                    data={"error": str(exc)[:200]},
                )
            except Exception:
                pass

            # Mark as failed
            failed_items = [_build_queue_item(url, parsed) for url in remaining_urls]
            complete_scrape_urls_step(
                items=failed_items,
                status="failed",
                error=f"storage_error: {str(exc)[:100]}",
            )
            return Failure(
                error_type="storage_failed",
                message=str(exc)[:200],
            )

    # Step: Mark completed URLs
    if completed_urls:
        completed_items = [_build_queue_item(url, parsed) for url in completed_urls]
        complete_scrape_urls_step(
            items=completed_items,
            status="completed",
        )

    # Step: Mark invalid URLs
    if invalid_urls:
        invalid_items = [_build_queue_item(url, parsed) for url in invalid_urls]
        complete_scrape_urls_step(
            items=invalid_items,
            status="invalid",
            error="invalid_job_data",
        )

    return Success(DetailScrapeResult(
        stored=stored_count,
        invalid=invalid_count,
        failed=len(http_404_urls),
        source_url=parsed.source_url,
    ))
