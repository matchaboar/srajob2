from __future__ import annotations

import os
import sys
import time
from typing import Any, Callable, Dict, List

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts
from job_scrape_application.workflows.activities import _extract_job_urls_from_scrape  # type: ignore
from job_scrape_application.workflows.site_handlers import get_site_handler
from job_scrape_application.workflows import webhook_workflow as ww


def _timer(label: str, fn: Callable[[], Any]) -> tuple[str, float]:
    start = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - start
    print(f"[timing] {label}: {elapsed:.4f}s")
    return label, elapsed


@pytest.mark.asyncio
async def test_deadlock_timing_simulation():
    """
    Simulate workflow-like CPU tasks and emit timings to help debug deadlocks.
    Use DEADLOCK_TIMING_SCALE to scale input sizes.
    """

    scale_raw = os.getenv("DEADLOCK_TIMING_SCALE")
    scale = max(1, int(scale_raw)) if scale_raw else None
    base_sizes = {
        "skipped": 20000,
        "entries": 6000,
        "urls": 12000,
    }
    sizes = {
        key: (value * scale if scale else value)
        for key, value in base_sizes.items()
    }

    skipped_raw: List[Any] = [
        f"https://example.com/skip/{i}" if i % 3 else None for i in range(sizes["skipped"])
    ]
    batch_entries = [
        {
            "url": f"https://example.com/jobs?page={i}",
            "_id": f"id-{i}",
            "sourceUrl": "https://example.com/jobs",
            "provider": "spidercloud",
            "siteId": "site-1",
            "attempts": i % 4,
        }
        for i in range(sizes["entries"])
    ]
    urls = [f"https://example.com/jobs?page={i}" for i in range(sizes["urls"])]
    existing_urls = urls[::2]
    posted_at_seed = {urls[i]: 1_700_000_000_000 + i for i in range(0, len(urls), 3)}
    normalized_jobs = [
        {
            "title": f"Engineer {i}",
            "company": "Example",
            "url": f"https://example.com/job/{i}",
            "location": "Remote",
        }
        for i in range(min(len(urls), 10000))
    ]
    handler = get_site_handler("https://example.com/jobs")

    durations: Dict[str, float] = {}

    def _filter_skipped():
        # SpidercloudJobDetailsWorkflow/SpidercloudListingWorkflow: build skipped_urls list from batch payload.
        _ = [u for u in skipped_raw if isinstance(u, str)]

    def _build_completion_items():
        # SpidercloudJobDetailsWorkflow/SpidercloudListingWorkflow: completion payload assembly for leased URLs.
        items = []
        for entry in batch_entries:
            url_val = entry.get("url")
            if isinstance(url_val, str) and url_val.strip():
                items.append(
                    {
                        "url": url_val,
                        "id": entry.get("_id"),
                        "sourceUrl": entry.get("sourceUrl"),
                        "provider": entry.get("provider"),
                        "siteId": entry.get("siteId"),
                        "attempts": int(entry.get("attempts") or 0),
                        "isListingUrl": True,
                    }
                )
        return items

    def _group_listing_entries():
        # SpidercloudListingWorkflow: group listing entries by (sourceUrl, pattern) for scrape batches.
        groups: Dict[tuple[str, str | None], List[str]] = {}
        entry_by_key: Dict[tuple[str, str | None], Dict[str, Any]] = {}
        for entry in batch_entries:
            source_val = entry.get("sourceUrl") if isinstance(entry.get("sourceUrl"), str) else ""
            pattern_val = entry.get("pattern") if isinstance(entry.get("pattern"), str) else None
            key = (source_val, pattern_val)
            entry_by_key.setdefault(key, entry)
            groups.setdefault(key, []).append(entry.get("url"))
        return groups, entry_by_key

    def _classify_listing_urls():
        # SpidercloudListingWorkflow: split listing vs detail URLs before enqueue.
        if not handler:
            return []
        return ["listing" if handler.is_listing_url(u) else "detail" for u in urls]

    def _extract_urls_from_scrape():
        # SpidercloudListingWorkflow: extract job URLs from listing scrape payload.
        payload = {
            "items": {
                "raw": "\n".join(urls[: min(2000, len(urls))]),
                "normalized": [],
            }
        }
        _ = _extract_job_urls_from_scrape(payload)

    async def _compute_urls_to_scrape():
        # GreenhouseScraperWorkflow/ProcessWebhookScrape: diff job URLs vs existing.
        await acts.compute_urls_to_scrape(urls, existing_urls)

    def _build_posted_at_map():
        # GreenhouseScraperWorkflow/SpidercloudListing: posted_at_by_url mapping.
        _ = {u: posted_at_seed[u] for u in urls if u in posted_at_seed}

    def _summarize_scrape_payload():
        # ProcessWebhookScrape: summarize normalized jobs for logging.
        payload = {
            "sourceUrl": "https://example.com/jobs",
            "provider": "firecrawl",
            "items": {"normalized": normalized_jobs},
        }
        _ = ww._summarize_scrape_payload(payload, max_samples=5, max_scan=len(normalized_jobs))  # noqa: SLF001

    def _greenhouse_fallback_dedupe():
        # GreenhouseScraperWorkflow: fallback list comprehension when diff payload malformed.
        job_urls = urls
        existing = existing_urls
        urls_to_scrape = [u for u in job_urls if isinstance(u, str)]
        _ = len({u for u in existing if isinstance(u, str)})
        return urls_to_scrape

    def _sitelease_queued_scrape_payload():
        # SiteLeaseWorkflow: build queued_scrape payload for firecrawl webhook job.
        job_info = {
            "jobId": "job-123",
            "statusUrl": "https://api.firecrawl.dev/v2/jobs/job-123",
            "webhookId": "wh-123",
            "receivedAt": 1_700_000_000_000,
            "kind": "greenhouse_listing",
            "metadata": {"siteUrl": "https://example.com/jobs"},
            "rawStart": {"status": "queued"},
            "providerRequest": {"url": "https://example.com/jobs"},
        }
        site = {
            "_id": "site-1",
            "url": "https://example.com/jobs",
            "pattern": None,
            "type": "general",
        }
        now_ms = 1_700_000_100_000
        job_id_str = str(job_info.get("jobId"))
        _ = {
            "sourceUrl": site.get("url"),
            "pattern": site.get("pattern"),
            "startedAt": job_info.get("receivedAt") or now_ms,
            "completedAt": now_ms,
            "items": {
                "normalized": [],
                "provider": "firecrawl",
                "jobId": job_id_str,
                "statusUrl": job_info.get("statusUrl"),
                "webhookId": job_info.get("webhookId"),
                "queued": True,
                "raw": {
                    "start": job_info.get("rawStart"),
                    "metadata": job_info.get("metadata"),
                },
                "request": {
                    "url": site.get("url"),
                    "pattern": site.get("pattern"),
                    "siteType": site.get("type") or "general",
                },
                "seedUrls": [site.get("url")],
            },
            "provider": "firecrawl",
            "workflowName": "SiteLease",
            "workflowId": "wf-1",
            "runId": "run-1",
            "asyncState": "queued",
            "asyncResponse": {
                "jobId": job_id_str,
                "statusUrl": job_info.get("statusUrl"),
                "webhookId": job_info.get("webhookId"),
                "kind": job_info.get("kind"),
                "receivedAt": job_info.get("receivedAt"),
            },
            "providerRequest": job_info.get("providerRequest"),
            "response": job_info.get("rawStart") or job_info,
        }

    def _build_large_batch_urls():
        # SpidercloudListingWorkflow/SpidercloudJobDetailsWorkflow: iterate large lease batch in workflow.
        raw_urls = [
            {
                "url": f"https://example.com/jobs?page={i}",
                "_id": f"id-{i}",
                "sourceUrl": "https://example.com/jobs",
                "provider": "spidercloud",
                "siteId": "site-1",
                "attempts": i % 3,
            }
            for i in range(len(urls))
        ]
        items = []
        for entry in raw_urls:
            url_val = entry.get("url")
            if isinstance(url_val, str) and url_val.strip():
                items.append({"url": url_val, "id": entry.get("_id")})
        return items

    def _format_workflow_log_message():
        # SpidercloudListingWorkflow/GreenhouseScraperWorkflow: format log messages with data payloads.
        data = {
            "count": len(urls),
            "sample": urls[:25],
            "listingCompleted": len(batch_entries),
            "queued": len(urls) // 2,
            "jobUrls": len(urls),
            "existing": len(existing_urls),
            "toScrape": len(urls) - len(existing_urls),
        }
        msg = f"SpidercloudListing | event=batch.processed | data={data}"
        return msg

    for label, fn in (
        ("filter_skipped_urls", _filter_skipped),
        ("build_completion_items", _build_completion_items),
        ("group_listing_entries", _group_listing_entries),
        ("classify_listing_urls", _classify_listing_urls),
        ("extract_urls_from_scrape", _extract_urls_from_scrape),
        ("build_posted_at_map", _build_posted_at_map),
        ("summarize_scrape_payload", _summarize_scrape_payload),
        ("greenhouse_fallback_dedupe", _greenhouse_fallback_dedupe),
        ("sitelease_queued_scrape_payload", _sitelease_queued_scrape_payload),
        ("build_large_batch_urls", _build_large_batch_urls),
        ("format_workflow_log_message", _format_workflow_log_message),
    ):
        name, elapsed = _timer(label, fn)
        durations[name] = elapsed

    name, elapsed = await _timer_async("compute_urls_to_scrape", _compute_urls_to_scrape)
    durations[name] = elapsed

    assert durations
    assert all(val >= 0 for val in durations.values())


async def _timer_async(label: str, fn: Callable[[], Any]) -> tuple[str, float]:
    start = time.perf_counter()
    await fn()
    elapsed = time.perf_counter() - start
    print(f"[timing] {label}: {elapsed:.4f}s")
    return label, elapsed


def _size_from_env(var: str, default: int) -> int:
    raw = os.getenv(var)
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _large_entries(count: int) -> list[dict[str, Any]]:
    return [
        {
            "url": f"https://example.com/jobs?page={i}",
            "_id": f"id-{i}",
            "sourceUrl": "https://example.com/jobs",
            "provider": "spidercloud",
            "siteId": "site-1",
            "attempts": i % 4,
        }
        for i in range(count)
    ]


def _event_payloads(count: int) -> list[dict[str, Any]]:
    return [
        {
            "_id": f"evt-{i}",
            "event": "job.completed",
            "status": "completed",
            "jobId": f"job-{i // 2}",
            "metadata": {"siteUrl": "https://example.com/jobs"},
            "receivedAt": 1_700_000_000_000 + i,
        }
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_deadlock_timing_large_lease_batch():
    """
    SpidercloudListingWorkflow/SpidercloudJobDetailsWorkflow: simulate very large lease batch sizes.
    Use DEADLOCK_TIMING_LARGE_BATCH to scale.
    """

    batch_size = _size_from_env("DEADLOCK_TIMING_LARGE_BATCH", 50000)
    entries = _large_entries(batch_size)

    def _build_completion_items_large():
        # Build completion payload for huge batches (worst-case workflow loop).
        items = []
        for entry in entries:
            url_val = entry.get("url")
            if isinstance(url_val, str) and url_val.strip():
                items.append({"url": url_val, "id": entry.get("_id")})
        return items

    _timer(f"large_batch_completion_items_{batch_size}", _build_completion_items_large)


@pytest.mark.asyncio
async def test_deadlock_timing_webhook_event_loop():
    """
    ProcessWebhookScrape: simulate per-event loop cost and dedup set growth.
    Use DEADLOCK_TIMING_WEBHOOK_EVENTS to scale.
    """

    event_count = _size_from_env("DEADLOCK_TIMING_WEBHOOK_EVENTS", 20000)
    events = _event_payloads(event_count)

    def _process_events_dedup():
        seen = set()
        processed = 0
        for event in events:
            event_type = (event.get("event") or "").lower()
            job_id = str(event.get("jobId") or "")
            dedup_key = f"{event_type}:{job_id}" if job_id else None
            if dedup_key and dedup_key in seen:
                continue
            if dedup_key:
                seen.add(dedup_key)
            processed += 1
        return processed

    _timer(f"webhook_event_loop_{event_count}", _process_events_dedup)


@pytest.mark.asyncio
async def test_deadlock_timing_replay_like_cost():
    """
    Replay-like cost: re-iterate large structures multiple times (history replay amplification).
    Use DEADLOCK_TIMING_REPLAY_ITERS to scale.
    """

    entries = _large_entries(_size_from_env("DEADLOCK_TIMING_REPLAY_ENTRIES", 20000))
    iters = _size_from_env("DEADLOCK_TIMING_REPLAY_ITERS", 5)

    def _replay_scan():
        total = 0
        for _ in range(iters):
            for entry in entries:
                url_val = entry.get("url")
                if isinstance(url_val, str) and url_val.endswith("0"):
                    total += 1
        return total

    _timer(f"replay_scan_{len(entries)}x{iters}", _replay_scan)
