from __future__ import annotations

import ast
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest


from job_scrape_application.workflows import activities as acts
from job_scrape_application.workflows.activities import _extract_job_urls_from_scrape  # type: ignore
from job_scrape_application.workflows.helpers._archive import workflow_debug as workflow_debug
from job_scrape_application.workflows.helpers._archive import workflow_logging as workflow_logging
from job_scrape_application.workflows.site_handlers import get_site_handler
from job_scrape_application.workflows._archive import temporal_webhook_workflow as ww

try:
    from temporalio import workflow as _workflow
except Exception:  # pragma: no cover - temporal optional for timing tests
    _workflow = None


WORKFLOW_RUN_SLOW_EVENT = {
    "uuid": "019b9f7d-699b-72b2-ba6e-9c287954c824",
    "trace_id": "414141414141414141414141414141414141414141413D3D",
    "span_id": "41414141414141414141413D",
    "body": (
        "workflow.run.slow ({'attempt': 1, 'namespace': 'default', "
        "'run_id': '019b9f7d-560d-72eb-a92c-924fbdba82e1', "
        "'task_queue': 'spidercloud-listing-queue', "
        "'workflow_id': 'wf-spidercloud-listing-3-2026-01-08T21:22:30Z', "
        "'workflow_type': 'SpidercloudListing'})"
    ),
    "attributes": {
        "attempt": "1",
        "code.file.path": (
            "/home/boarcoder/documents/github/srajob2/"
            "job_scrape_application/workflows/worker.py"
        ),
        "code.function.name": "execute_workflow",
        "code.line.number": "417",
        "elapsedMs": "2875",
        "event": "workflow.run.slow",
        "hostname": "DESKTOP-NFS14HP",
        "isReplay": "false",
        "lastActivityAt": "2026-01-08T21:22:32.838094+00:00",
        "lastActivity": "record_workflow_run",
        "runActivityCount": "4",
        "runChildWorkflowCount": "0",
        "runId": "019b9f7d-560d-72eb-a92c-924fbdba82e1",
        "runLastActivityAt": "2026-01-08T21:22:32.838094+00:00",
        "runLastActivity": "record_workflow_run",
        "taskQueue": "spidercloud-listing-queue",
        "taskQueues": "spidercloud-listing-queue(listing)",
        "temporal_workflow": (
            "{\"attempt\":1,\"namespace\":\"default\","
            "\"run_id\":\"019b9f7d-560d-72eb-a92c-924fbdba82e1\","
            "\"task_queue\":\"spidercloud-listing-queue\","
            "\"workflow_id\":\"wf-spidercloud-listing-3-2026-01-08T21:22:30Z\","
            "\"workflow_type\":\"SpidercloudListing\"}"
        ),
        "workerId": "DESKTOP-NFS14HP-166211",
        "workerRole": "listing",
        "workflowId": "wf-spidercloud-listing-3-2026-01-08T21:22:30Z",
        "workflowType": "SpidercloudListing",
    },
    "timestamp": "2026-01-08T21:22:32.953218Z",
    "observed_timestamp": "2026-01-08T21:22:35.035755Z",
    "severity_text": "warn",
    "severity_number": 13,
    "level": "warn",
    "resource_attributes": {
        "service.name": "unknown_service",
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.39.1",
    },
    "instrumentation_scope": "temporalio.workflow@",
    "event_name": "",
    "live_logs_checkpoint": "2026-01-08T21:22:05.022908",
}


def _count_helper_calls(pattern: str, paths: list[Path]) -> int:
    total = 0
    for path in paths:
        total += len(re.findall(pattern, path.read_text(encoding="utf-8")))
    return total


WORKFLOW_CHECKPOINT_CASES: list[dict[str, Any]] = [
    {
        "label": "scraper.before_lease",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:before_lease",
        "data": {
            "leasedCount": 1,
            "maxLeases": 10,
            "provider": "fetchfox",
            "leaseArgs": ["scraper-worker", 300, None, "fetchfox"],
        },
    },
    {
        "label": "scraper.after_lease",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:after_lease",
        "data": {
            "leasedCount": 2,
            "siteUrl": "https://example.com/jobs",
            "siteId": "site-1",
            "pattern": None,
        },
    },
    {
        "label": "scraper.before_activity",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:before_activity",
        "data": {
            "activity": "scrape_site",
            "siteUrl": "https://example.com/jobs",
            "siteId": "site-1",
            "persistScrapes": False,
        },
    },
    {
        "label": "scraper.after_activity",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:after_activity",
        "data": {
            "siteUrl": "https://example.com/jobs",
            "siteId": "site-1",
            "persistScrapes": False,
            "hasScrapeId": False,
            "hasSummary": True,
            "hasRecoveryPayload": False,
        },
    },
    {
        "label": "scraper.before_store_scrape",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:before_store_scrape",
        "data": {
            "siteUrl": "https://example.com/jobs",
            "siteId": "site-1",
            "persistScrapes": False,
        },
    },
    {
        "label": "scraper.before_start_recovery",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:before_start_recovery",
        "data": {
            "siteUrl": "https://example.com/jobs",
            "siteId": "site-1",
            "jobId": "job-123",
        },
    },
    {
        "label": "scraper.before_complete_site",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:before_complete_site",
        "data": {"siteUrl": "https://example.com/jobs", "siteId": "site-1"},
    },
    {
        "label": "scraper.before_fail_site",
        "location": "scrape_workflow:_run_scrape_workflow:ScrapeWorkflow:before_fail_site",
        "data": {"siteUrl": "https://example.com/jobs", "siteId": "site-1"},
    },
    {
        "label": "job_details.complete_urls.start",
        "location": "scrape_workflow:SpidercloudJobDetails.run:complete_urls:start",
        "data": {"status": "completed", "total": 200, "chunkSize": 100, "error": None},
    },
    {
        "label": "job_details.complete_urls.chunk",
        "location": "scrape_workflow:SpidercloudJobDetails.run:complete_urls:chunk",
        "data": {"status": "completed", "chunkIndex": 0, "chunkSize": 100},
    },
    {
        "label": "job_details.complete_urls.done",
        "location": "scrape_workflow:SpidercloudJobDetails.run:complete_urls:done",
        "data": {"status": "completed", "total": 200},
    },
    {
        "label": "job_details.before_lease",
        "location": "scrape_workflow:SpidercloudJobDetails.run:before_lease",
        "data": {"provider": "spidercloud", "urlType": "detail", "limit": 500},
    },
    {
        "label": "job_details.process_skipped_urls",
        "location": "scrape_workflow:SpidercloudJobDetails.run:process_skipped_urls",
        "data": {"skippedCount": 120},
    },
    {
        "label": "job_details.before_log_batch_leased",
        "location": "scrape_workflow:SpidercloudJobDetails.run:before_log_batch_leased",
        "data": {"batchUrls": 800, "skippedCount": 120},
    },
    {
        "label": "job_details.before_process_batch",
        "location": "scrape_workflow:SpidercloudJobDetails.run:before_process_batch",
        "data": {"batchUrls": 800, "skippedCount": 120},
    },
    {
        "label": "job_details.before_log_batch_store",
        "location": "scrape_workflow:SpidercloudJobDetails.run:before_log_batch_store",
        "data": {"stored": 650, "invalid": 50, "failed": 10},
    },
    {
        "label": "job_details.before_log_batch_processed",
        "location": "scrape_workflow:SpidercloudJobDetails.run:before_log_batch_processed",
        "data": {"scrapes": 620},
    },
    {
        "label": "job_details.before_log_batch_processed_empty",
        "location": "scrape_workflow:SpidercloudJobDetails.run:before_log_batch_processed_empty",
        "data": {"urls": 800},
    },
    {
        "label": "job_details.store_scrapes_loop",
        "location": "scrape_workflow:SpidercloudJobDetails.run:store_scrapes_loop",
        "data": {"scrapes": 620, "urls": 800},
    },
    {
        "label": "job_details.complete_urls",
        "location": "scrape_workflow:SpidercloudJobDetails.run:complete_urls",
        "data": {"completed": 600},
    },
    {
        "label": "job_details.complete_invalid",
        "location": "scrape_workflow:SpidercloudJobDetails.run:complete_invalid",
        "data": {"invalid": 40},
    },
    {
        "label": "job_details.complete_failed",
        "location": "scrape_workflow:SpidercloudJobDetails.run:complete_failed",
        "data": {"failed": 20},
    },
    {
        "label": "job_details.fail_cleanup",
        "location": "scrape_workflow:SpidercloudJobDetails.run:fail_cleanup",
        "data": None,
    },
    {
        "label": "job_details.fail_cleanup.complete_urls",
        "location": "scrape_workflow:SpidercloudJobDetails.run:fail_cleanup_complete_urls",
        "data": {"leased": 800},
    },
    {
        "label": "listing.before_lease",
        "location": "scrape_workflow:SpidercloudListing.run:before_lease",
        "data": {"provider": "spidercloud", "urlType": "listing", "limit": 400},
    },
    {
        "label": "listing.process_skipped_urls",
        "location": "scrape_workflow:SpidercloudListing.run:process_skipped_urls",
        "data": {"skippedCount": 50},
    },
    {
        "label": "listing.before_log_batch_leased",
        "location": "scrape_workflow:SpidercloudListing.run:before_log_batch_leased",
        "data": {"batchUrls": 500, "skippedCount": 50},
    },
    {
        "label": "listing.before_process_batch",
        "location": "scrape_workflow:SpidercloudListing.run:before_process_batch",
        "data": {"batchUrls": 500, "skippedCount": 50},
    },
    {
        "label": "listing.before_log_batch_processed",
        "location": "scrape_workflow:SpidercloudListing.run:before_log_batch_processed",
        "data": {"batchUrls": 500, "queued": 350, "listingCompleted": 120},
    },
    {
        "label": "listing.fail_cleanup",
        "location": "scrape_workflow:SpidercloudListing.run:fail_cleanup",
        "data": {"batchUrls": 500},
    },
    {
        "label": "greenhouse.before_lease",
        "location": "greenhouse_workflow:GreenhouseScraperWorkflow.run:before_lease",
        "data": {"provider": "greenhouse", "leaseLimit": 300},
    },
    {
        "label": "greenhouse.fetch_listing",
        "location": "greenhouse_workflow:GreenhouseScraperWorkflow.run:fetch_listing",
        "data": {"siteUrl": "https://example.com", "siteId": "site-1"},
    },
    {
        "label": "greenhouse.filter_existing",
        "location": "greenhouse_workflow:GreenhouseScraperWorkflow.run:filter_existing",
        "data": {"jobUrls": 200},
    },
    {
        "label": "greenhouse.compute_diff",
        "location": "greenhouse_workflow:GreenhouseScraperWorkflow.run:compute_diff",
        "data": {"jobUrls": 200, "existing": 50},
    },
    {
        "label": "greenhouse.scrape_jobs",
        "location": "greenhouse_workflow:GreenhouseScraperWorkflow.run:scrape_jobs",
        "data": {"urlsToScrape": 150},
    },
    {
        "label": "greenhouse.store_scrape",
        "location": "greenhouse_workflow:GreenhouseScraperWorkflow.run:store_scrape",
        "data": {"urlsToScrape": 150},
    },
]


WORKFLOW_LOGGER_CASES: list[dict[str, Any]] = [
    {
        "name": "GreenhouseScraperWorkflow",
        "level": "info",
        "message": (
            "GreenhouseScraperWorkflow | event=site.leased | siteUrl=https://example.com "
            "| message=None | data={'siteId': 'site-1'}"
        ),
    },
    {
        "name": "ScrapeWorkflow",
        "level": "info",
        "message": (
            "ScrapeWorkflow | event=site.leased | {'siteUrl': 'https://example.com', "
            "'message': None, 'data': {'siteId': 'site-1', 'pattern': None}}"
        ),
    },
    {
        "name": "SpidercloudJobDetails",
        "level": "info",
        "message": (
            "SpidercloudJobDetails | event=batch.leased | data={'count': 800, 'skippedCount': 120}"
        ),
    },
    {
        "name": "SpidercloudListing",
        "level": "info",
        "message": (
            "SpidercloudListing | event=batch.processed | data={'queued': 350, "
            "'listingCompleted': 120, 'batchCount': 500}"
        ),
    },
    {
        "name": "SiteLease",
        "level": "info",
        "message": (
            "SiteLease | event=firecrawl.job.started | siteUrl=https://example.com | message=None "
            "| data={'jobId': 'job-1', 'statusUrl': 'https://api.firecrawl.dev/v2/jobs/job-1', "
            "'kind': 'site_crawl'}"
        ),
    },
    {
        "name": "ProcessWebhookScrape",
        "level": "info",
        "message": (
            "ProcessWebhookScrape | event=webhook.ingested | siteUrl=https://example.com | message=None "
            "| data={'eventId': 'evt-1', 'jobId': 'job-1', 'jobsScraped': 2, 'stored': 1}"
        ),
    },
    {
        "name": "RecoverMissingFirecrawlWebhook",
        "level": "info",
        "message": (
            "RecoverMissingFirecrawlWebhook | event=workflow.start | siteUrl=https://example.com "
            "| message=Recovering Firecrawl job job-1 | data={'jobId': 'job-1', "
            "'siteUrl': 'https://example.com'}"
        ),
    },
]


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


@pytest.mark.asyncio
async def test_deadlock_timing_workflow_run_slow_event_processing():
    """
    Simulate workflow.run.slow log processing overhead.
    Use DEADLOCK_TIMING_RUN_SLOW_EVENTS to scale.
    """

    event_count = _size_from_env("DEADLOCK_TIMING_RUN_SLOW_EVENTS", 200)
    event = WORKFLOW_RUN_SLOW_EVENT

    def _process_run_slow_events():
        summaries = []
        for _ in range(event_count):
            body = str(event.get("body") or "")
            raw = body.partition("workflow.run.slow ")[2].strip()
            if raw.startswith("(") and raw.endswith(")"):
                raw = raw[1:-1]
            try:
                body_payload = ast.literal_eval(raw) if raw else {}
            except (SyntaxError, ValueError):
                body_payload = {}
            attributes = event.get("attributes") or {}
            temporal_raw = attributes.get("temporal_workflow")
            temporal_payload = json.loads(temporal_raw) if temporal_raw else {}
            combined = {**body_payload, **temporal_payload}
            combined.update(
                {
                    "elapsedMs": attributes.get("elapsedMs"),
                    "runId": attributes.get("runId"),
                    "workflowId": attributes.get("workflowId"),
                    "workflowType": attributes.get("workflowType"),
                    "taskQueue": attributes.get("taskQueue"),
                }
            )
            summaries.append(json.dumps(combined, sort_keys=True))
        return summaries

    _timer(f"workflow_run_slow_event_processing_{event_count}", _process_run_slow_events)


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


def _blocking_convex_retry_for_deadlock() -> None:
    from job_scrape_application.services import convex_client as convex

    def _busy_wait(seconds: float) -> None:
        if seconds <= 0:
            return
        end = time.perf_counter() + seconds
        while time.perf_counter() < end:
            pass

    start = time.perf_counter()
    for attempt in range(1, convex._MAX_RETRIES + 1):
        elapsed = time.perf_counter() - start
        remaining = convex._TOTAL_BUDGET_SECONDS - elapsed
        if remaining <= 0:
            break
        per_attempt = min(convex._REQUEST_TIMEOUT_SECONDS, remaining)
        # Simulate a Convex request that blocks until timeout.
        _busy_wait(per_attempt)
        if attempt >= convex._MAX_RETRIES:
            break
        elapsed = time.perf_counter() - start
        remaining = convex._TOTAL_BUDGET_SECONDS - elapsed
        if remaining <= 0:
            break
        backoff = min(
            convex._BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)),
            convex._BACKOFF_MAX_SECONDS,
        )
        # Use deterministic backoff (no jitter) for test stability.
        _busy_wait(min(backoff, remaining))
    raise RuntimeError("Convex mutation failed: 500")


if _workflow:

    @_workflow.defn(sandboxed=False)
    class _ConvexRetryDeadlockWorkflow:
        @_workflow.run
        async def run(self) -> str:
            _blocking_convex_retry_for_deadlock()
            return "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", WORKFLOW_CHECKPOINT_CASES, ids=lambda case: case["location"])
async def test_deadlock_timing_workflow_checkpoint_cases(monkeypatch, case):
    class _Info:
        run_id = "run-1"
        workflow_id = "wf-1"
        workflow_type = "ScrapeWorkflow"
        task_queue = "scraper-task-queue"

    def _info():
        return _Info()

    def _now():
        return datetime(2026, 1, 8, 12, 0, 0, tzinfo=timezone.utc)

    async def _execute_local_activity(_name, _args, start_to_close_timeout=None):  # noqa: ARG001
        return None

    monkeypatch.setattr(workflow_debug.workflow, "info", _info)
    monkeypatch.setattr(workflow_debug.workflow, "now", _now)
    monkeypatch.setattr(workflow_debug.workflow, "execute_local_activity", _execute_local_activity)

    async def _run():
        await workflow_debug.workflow_checkpoint(
            case["label"],
            location=case["location"],
            data=case.get("data"),
        )

    await _timer_async(f"workflow_checkpoint_{case['label']}", _run)


@pytest.mark.parametrize("case", WORKFLOW_LOGGER_CASES, ids=lambda case: case["name"])
def test_deadlock_timing_workflow_logger_messages(case):
    logger = workflow_logging.get_workflow_logger()

    def _log():
        level = case["level"]
        message = case["message"]
        if level == "error":
            logger.error(message)
        elif level in {"warn", "warning"}:
            logger.warning(message)
        else:
            logger.info(message)

    _timer(f"workflow_logger_{case['name']}", _log)


@pytest.mark.asyncio
async def test_deadlock_timing_large_json_parse():
    """
    Simulate JSON parsing in workflow code (possible deadlock cause).
    Use DEADLOCK_TIMING_JSON_ROWS/DEADLOCK_TIMING_JSON_REPEATS to scale.
    """

    rows = _size_from_env("DEADLOCK_TIMING_JSON_ROWS", 20000)
    repeats = _size_from_env("DEADLOCK_TIMING_JSON_REPEATS", 3)

    payload = [
        {
            "url": f"https://example.com/jobs/{i}",
            "title": f"Role {i}",
            "metadata": {"idx": i, "tags": [f"tag-{i % 7}", f"tag-{i % 13}"]},
        }
        for i in range(rows)
    ]
    raw = json.dumps(payload)

    def _parse_payload():
        total = 0
        for _ in range(repeats):
            parsed = json.loads(raw)
            total += len(parsed)
        return total

    _timer(f"json_parse_{rows}x{repeats}", _parse_payload)


@pytest.mark.asyncio
async def test_deadlock_timing_large_comprehension():
    """
    Simulate large list/dict comprehensions + sorting in workflow code.
    Use DEADLOCK_TIMING_COMPREHENSION_SIZE to scale.
    """

    size = _size_from_env("DEADLOCK_TIMING_COMPREHENSION_SIZE", 60000)
    entries = _large_entries(size)

    def _build_and_sort():
        by_url = {
            entry.get("url"): {
                "id": entry.get("_id"),
                "attempts": int(entry.get("attempts") or 0),
                "sourceUrl": entry.get("sourceUrl"),
            }
            for entry in entries
            if isinstance(entry.get("url"), str)
        }
        ordered = sorted(by_url.items(), key=lambda item: item[1]["attempts"])
        return ordered[:25]

    _timer(f"comprehension_sort_{size}", _build_and_sort)


@pytest.mark.asyncio
async def test_deadlock_timing_blocking_sleep():
    """
    Simulate blocking I/O (time.sleep) inside workflow code.
    Use DEADLOCK_TIMING_BLOCKING_SECONDS to scale (set >2s to match deadlock threshold).
    """

    raw = os.getenv("DEADLOCK_TIMING_BLOCKING_SECONDS")
    seconds = float(raw) if raw else 0.25

    def _blocking_sleep():
        time.sleep(seconds)

    _timer(f"blocking_sleep_{seconds:.2f}s", _blocking_sleep)


@pytest.mark.asyncio
async def test_deadlock_convex_retry_triggers_deadlock(caplog):
    """
    Simulate a workflow calling Convex directly with Convex-like retry timing.
    Expect Temporal deadlock detection since we block without yielding.
    """

    pytest.importorskip("temporalio")
    import logging
    from temporalio.client import WorkflowFailureError
    from temporalio.exceptions import ApplicationError, TimeoutError
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker
    if _workflow is None:
        pytest.skip("temporalio not installed", allow_module_level=False)

    caplog.set_level(logging.ERROR, logger="temporalio.worker._workflow")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"deadlock-convex-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[_ConvexRetryDeadlockWorkflow],
        )
        async with worker:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await env.client.execute_workflow(
                    _ConvexRetryDeadlockWorkflow.run,
                    id=f"wf-deadlock-convex-{uuid.uuid4().hex[:6]}",
                    task_queue=task_queue,
                    execution_timeout=timedelta(seconds=8),
                )

    cause = excinfo.value.cause
    assert "Potential deadlock detected" in caplog.text
    if isinstance(cause, ApplicationError):
        assert cause.type == "_DeadlockError"
    else:
        assert isinstance(cause, TimeoutError)
