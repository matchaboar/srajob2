from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from .exceptions import WorkflowError

from ..config import settings

with workflow.unsafe.imports_passed_through():
    from .activities import (
        collect_firecrawl_job_result,
        complete_site,
        fail_site,
        fetch_pending_firecrawl_webhooks,
        filter_existing_job_urls,
        get_firecrawl_webhook_status,
        lease_site,
        mark_firecrawl_webhook_processed,
        record_workflow_run,
        record_scratchpad,
        scrape_greenhouse_jobs,
        start_firecrawl_webhook_scrape,
        store_scrape,
    )


DEFAULT_LOCK_SECONDS = 1800
HTTP_RETRY_BASE_SECONDS = 30
FIRECRAWL_WEBHOOK_RECHECK = timedelta(hours=settings.firecrawl_webhook_recheck_hours)
FIRECRAWL_WEBHOOK_TIMEOUT = timedelta(hours=settings.firecrawl_webhook_timeout_hours)


def _summarize_scrape_payload(scrape_payload: Any, *, max_samples: int = 5) -> Dict[str, Any]:
    """Return lightweight counts/samples for scratchpad logging."""

    if not isinstance(scrape_payload, dict):
        return {}

    items = scrape_payload.get("items")
    normalized = items.get("normalized") if isinstance(items, dict) else None
    normalized_jobs = normalized if isinstance(normalized, list) else []

    samples: List[Dict[str, Any]] = []
    for job in normalized_jobs:
        if not isinstance(job, dict):
            continue
        sample = {
            "title": job.get("job_title") or job.get("title"),
            "company": job.get("company"),
            "url": job.get("url"),
            "location": job.get("location"),
        }
        filtered_sample = {k: v for k, v in sample.items() if v}
        if filtered_sample:
            samples.append(filtered_sample)
        if len(samples) >= max_samples:
            break

    return {
        "sourceUrl": scrape_payload.get("sourceUrl"),
        "provider": scrape_payload.get("provider"),
        "normalizedCount": len(normalized_jobs),
        "sample": samples,
    }


@dataclass
class SiteLeaseResult:
    leased: int
    jobs_started: int
    job_ids: List[str]


@workflow.defn(name="SiteLease")
class SiteLeaseWorkflow:
    @workflow.run
    async def run(self) -> SiteLeaseResult:  # type: ignore[override]
        leased = 0
        jobs_started = 0
        job_ids: List[str] = []
        site_urls: List[str] = []
        failure_reasons: List[str] = []
        status = "completed"
        started_at = int(workflow.now().timestamp() * 1000)
        run_info = workflow.info()

        async def _log(
            event: str,
            *,
            message: str | None = None,
            data: Dict[str, Any] | None = None,
            site_url: str | None = None,
            level: str = "info",
        ) -> None:
            try:
                await workflow.execute_activity(
                    record_scratchpad,
                    args=[
                        {
                            "runId": run_info.run_id,
                            "workflowId": run_info.workflow_id,
                            "workflowName": "SiteLease",
                            "siteUrl": site_url,
                            "event": event,
                            "message": message,
                            "data": data,
                            "level": level,
                            "createdAt": int(workflow.now().timestamp() * 1000),
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=60),
                )
            except Exception:
                pass

        await _log("workflow.start", message="SiteLease started")

        try:
            while True:
                site = await workflow.execute_activity(
                    lease_site,
                    args=["scraper-worker", DEFAULT_LOCK_SECONDS, None, "firecrawl"],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
                if not site:
                    break

                leased += 1
                site_urls.append(site["url"])
                await _log(
                    "site.leased",
                    site_url=site["url"],
                    data={"siteId": site.get("_id")},
                )

                try:
                    job_info = await workflow.execute_activity(
                        start_firecrawl_webhook_scrape,
                        args=[site],
                        start_to_close_timeout=timedelta(minutes=2),
                    )
                    if isinstance(job_info, dict):
                        job_id = job_info.get("jobId")
                        if job_id:
                            jobs_started += 1
                            job_id_str = str(job_id)
                            job_ids.append(job_id_str)
                            await _log(
                                "firecrawl.job.started",
                                site_url=site.get("url"),
                                data={
                                    "jobId": job_id_str,
                                    "statusUrl": job_info.get("statusUrl"),
                                    "kind": job_info.get("kind"),
                                },
                            )
                            try:
                                now_ms = int(workflow.now().timestamp() * 1000)
                                queued_scrape = {
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
                                await workflow.execute_activity(
                                    store_scrape,
                                    args=[queued_scrape],
                                    schedule_to_close_timeout=timedelta(minutes=3),
                                    start_to_close_timeout=timedelta(minutes=3),
                                    heartbeat_timeout=timedelta(seconds=30),
                                )
                            except Exception as store_err:  # noqa: BLE001
                                await _log(
                                    "firecrawl.job.store_failed",
                                    site_url=site.get("url"),
                                    message=str(store_err),
                                    level="warn",
                                )
                            recovery_payload = {
                                "jobId": job_id_str,
                                "webhookId": job_info.get("webhookId"),
                                "metadata": job_info.get("metadata"),
                                "siteId": site.get("_id"),
                                "siteUrl": site.get("url"),
                                "statusUrl": job_info.get("statusUrl"),
                                "receivedAt": job_info.get("receivedAt"),
                            }
                            try:
                                await workflow.start_child_workflow(
                                    "RecoverMissingFirecrawlWebhook",
                                    recovery_payload,
                                    id=f"wf-firecrawl-recovery-{job_id}",
                                    task_queue=workflow.info().task_queue,
                                )
                            except Exception as child_err:  # noqa: BLE001
                                await _log(
                                    "recovery.start_failed",
                                    site_url=site.get("url"),
                                    message=str(child_err),
                                    level="warn",
                                )
                except Exception as e:  # noqa: BLE001
                    status = "failed"
                    await workflow.execute_activity(
                        fail_site,
                        args=[{"id": site["_id"], "error": str(e)}],
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                    if isinstance(e, ActivityError) and e.cause:
                        failure_reasons.append(f"{site['url']}: {e.cause}")
                    elif isinstance(e, ApplicationError):
                        failure_reasons.append(f"{site['url']}: {e}")
                    else:
                        failure_reasons.append(f"{site['url']}: {e}")

                    await _log(
                        "site.error",
                        site_url=site.get("url"),
                        message=str(e),
                        level="error",
                    )

            return SiteLeaseResult(leased=leased, jobs_started=jobs_started, job_ids=job_ids)
        finally:
            completed_at = int(workflow.now().timestamp() * 1000)
            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    args=[
                        {
                            "runId": workflow.info().run_id,
                            "workflowId": workflow.info().workflow_id,
                            "workflowName": "SiteLease",
                            "status": status,
                            "startedAt": started_at,
                            "completedAt": completed_at,
                            "siteUrls": site_urls,
                            "sitesProcessed": leased,
                            "jobsScraped": jobs_started,
                            "workerId": "scraper-worker",
                            "taskQueue": "scraper-task-queue",
                            "error": "; ".join(failure_reasons) if failure_reasons else None,
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                # Best-effort logging
                pass

            await _log(
                "workflow.complete",
                message="SiteLease finished",
                data={
                    "status": status,
                    "leased": leased,
                    "jobsStarted": jobs_started,
                },
                level="warn" if status != "completed" else "info",
            )


async def _ingest_firecrawl_result(
    event: Dict[str, Any],
    result: Dict[str, Any],
    *,
    log: Any,
    workflow_name: str,
) -> tuple[int, int, List[str]]:
    """Process a Firecrawl result payload and persist Convex mutations."""

    stored = 0
    jobs_scraped = 0
    site_urls: List[str] = []

    event_id = event.get("_id")
    webhook_error = result.get("error") if isinstance(result, dict) else None

    metadata_raw = event.get("metadata")
    metadata: Dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
    result_dict: Dict[str, Any] = result if isinstance(result, dict) else {}
    site_url = event.get("siteUrl") or metadata.get("siteUrl") or result_dict.get("siteUrl")
    site_id: Optional[str] = result_dict.get("siteId")
    status_value = (result_dict.get("status") or "").lower()
    job_id = result_dict.get("jobId") or event.get("jobId") or metadata.get("jobId")
    job_urls_in_result = result_dict.get("job_urls")
    job_urls_count = len(job_urls_in_result) if isinstance(job_urls_in_result, list) else None
    scrape_summary = _summarize_scrape_payload(result_dict.get("scrape"))
    if site_url:
        site_urls.append(site_url)

    await log(
        "webhook.ingest.start",
        site_url=site_url,
        data={
            "eventId": event_id,
            "jobId": job_id,
            "siteId": site_id,
            "status": status_value or None,
            "kind": result_dict.get("kind"),
            "jobUrlsInPayload": job_urls_count,
            "normalizedCount": scrape_summary.get("normalizedCount") if scrape_summary else None,
            "sampleJobs": scrape_summary.get("sample") if scrape_summary else None,
        },
    )

    if status_value in {"cancelled_expired", "expired_due_to_age"}:
        error_message = webhook_error or "Firecrawl job expired before status was fetched"
        # Record a minimal scrape for audit and mark the site as completed so the workflow can move on.
        now_ms = int(workflow.now().timestamp() * 1000)
        expired_scrape = {
            "sourceUrl": site_url,
            "pattern": None,
            "startedAt": now_ms,
            "completedAt": now_ms,
            "items": {
                "normalized": [],
                "raw": {
                    "status": status_value,
                    "error": error_message,
                },
                "provider": "firecrawl",
            },
            "provider": "firecrawl",
            "workflowName": workflow_name,
            "kind": "firecrawl_expired",
        }
        await workflow.execute_activity(
            store_scrape,
            args=[expired_scrape],
            schedule_to_close_timeout=timedelta(seconds=30),
        )
        if site_id:
            await workflow.execute_activity(
                complete_site,
                args=[site_id],
                schedule_to_close_timeout=timedelta(seconds=30),
            )
        if event_id:
            await workflow.execute_activity(
                mark_firecrawl_webhook_processed,
                args=[event_id, webhook_error or status_value],
                schedule_to_close_timeout=timedelta(seconds=30),
            )
        await log(
            "webhook.cancelled",
            site_url=site_url,
            data={
                "status": status_value,
                "error": error_message,
                "eventId": event_id,
                "jobId": job_id,
            },
            level="warn",
        )
        return stored, jobs_scraped, site_urls

    if result.get("kind") == "greenhouse_listing":
        job_urls = [u for u in result.get("job_urls", []) if isinstance(u, str)]
        urls_to_scrape: List[str] = []
        if job_urls:
            existing = await workflow.execute_activity(
                filter_existing_job_urls,
                args=[job_urls],
                schedule_to_close_timeout=timedelta(seconds=30),
            )
            existing_set: Set[str] = set(existing)
            urls_to_scrape = [u for u in job_urls if u not in existing_set]
            await log(
                "webhook.listing.urls",
                site_url=site_url,
                data={
                    "eventId": event_id,
                    "jobId": job_id,
                    "totalUrls": len(job_urls),
                    "deduped": len(existing_set),
                    "toScrape": len(urls_to_scrape),
                    "sampleNew": urls_to_scrape[:5],
                },
            )

        if urls_to_scrape:
            scrape_res = await workflow.execute_activity(
                scrape_greenhouse_jobs,
                args=[
                    {
                        "urls": urls_to_scrape,
                        "source_url": site_url,
                        "idempotency_key": event_id,
                        "webhook_id": event_id,
                    }
                ],
                start_to_close_timeout=timedelta(minutes=10),
            )
            jobs_scraped += int(scrape_res.get("jobsScraped") or 0) if isinstance(scrape_res, dict) else 0
            scrape_payload = scrape_res.get("scrape") if isinstance(scrape_res, dict) else None
            if scrape_payload:
                scrape_payload.setdefault("workflowName", workflow_name)
                await log(
                    "webhook.listing.scrape",
                    site_url=site_url,
                    data={
                        "eventId": event_id,
                        "jobId": job_id,
                        "jobsScraped": jobs_scraped,
                        "toScrape": len(urls_to_scrape),
                        "scrapeSummary": _summarize_scrape_payload(scrape_payload),
                    },
                )
                await workflow.execute_activity(
                    store_scrape,
                    args=[scrape_payload],
                    schedule_to_close_timeout=timedelta(minutes=3),
                    start_to_close_timeout=timedelta(minutes=3),
                    heartbeat_timeout=timedelta(seconds=30),
                )
                stored += 1
        else:
            request_snapshot = result.get("request") if isinstance(result, dict) else None
            listing_scrape = {
                "sourceUrl": site_url,
                "pattern": None,
                "startedAt": event.get("receivedAt") or int(workflow.now().timestamp() * 1000),
                "completedAt": int(workflow.now().timestamp() * 1000),
                "items": {
                    "normalized": [],
                    "raw": {"job_urls": job_urls, "raw": result.get("raw"), "status": result.get("status")},
                    "provider": "firecrawl",
                    "request": request_snapshot,
                },
                "request": request_snapshot,
                "provider": "firecrawl",
                "workflowName": workflow_name,
                "kind": "greenhouse_listing",
            }
            await workflow.execute_activity(
                store_scrape,
                args=[listing_scrape],
                schedule_to_close_timeout=timedelta(minutes=3),
                start_to_close_timeout=timedelta(minutes=3),
                heartbeat_timeout=timedelta(seconds=30),
            )
            await log(
                "webhook.listing.stored",
                site_url=site_url,
                data={
                    "eventId": event_id,
                    "jobId": job_id,
                    "jobUrls": len(job_urls),
                    "storedKind": "listing_only",
                },
            )
            stored += 1

        if site_id:
            await workflow.execute_activity(
                complete_site,
                args=[site_id],
                schedule_to_close_timeout=timedelta(seconds=30),
            )
        if event_id:
            await workflow.execute_activity(
                mark_firecrawl_webhook_processed,
                args=[event_id, webhook_error],
                schedule_to_close_timeout=timedelta(seconds=30),
            )

        return stored, jobs_scraped, site_urls

    scrape_payload = result.get("scrape")
    if scrape_payload:
        scrape_payload.setdefault("workflowName", workflow_name)
        scrape_summary = _summarize_scrape_payload(scrape_payload)
        await workflow.execute_activity(
            store_scrape,
            args=[scrape_payload],
            schedule_to_close_timeout=timedelta(minutes=3),
            start_to_close_timeout=timedelta(minutes=3),
            heartbeat_timeout=timedelta(seconds=30),
        )
        stored += 1

    jobs_scraped += int(result.get("jobsScraped") or 0)
    if scrape_payload:
        await log(
            "webhook.scrape.stored",
            site_url=site_url,
            data={
                "eventId": event_id,
                "jobId": job_id,
                "jobsScraped": jobs_scraped,
                "scrapeSummary": scrape_summary,
            },
        )
    if site_id:
        await workflow.execute_activity(
            complete_site,
            args=[site_id],
            schedule_to_close_timeout=timedelta(seconds=30),
        )
    if event_id:
        await workflow.execute_activity(
            mark_firecrawl_webhook_processed,
            args=[event_id, webhook_error],
            schedule_to_close_timeout=timedelta(seconds=30),
        )

    await log(
        "webhook.ingested",
        site_url=site_url,
        data={
            "eventId": event_id,
            "jobId": job_id,
            "jobsScraped": jobs_scraped,
            "stored": stored,
        },
    )

    return stored, jobs_scraped, site_urls


@dataclass
class WebhookProcessSummary:
    processed: int
    stored: int
    jobs_scraped: int
    failed: int


@workflow.defn(name="ProcessWebhookScrape")
class ProcessWebhookIngestWorkflow:
    @workflow.run
    async def run(self) -> WebhookProcessSummary:  # type: ignore[override]
        processed = 0
        stored = 0
        jobs_scraped = 0
        failed = 0
        status = "completed"
        failure_reasons: List[str] = []
        site_urls: List[str] = []
        started_at = int(workflow.now().timestamp() * 1000)
        run_info = workflow.info()

        async def _log(
            event: str,
            *,
            message: str | None = None,
            data: Dict[str, Any] | None = None,
            site_url: str | None = None,
            level: str = "info",
        ) -> None:
            try:
                await workflow.execute_activity(
                    record_scratchpad,
                    args=[
                        {
                            "runId": run_info.run_id,
                            "workflowId": run_info.workflow_id,
                            "workflowName": "ProcessWebhookScrape",
                            "siteUrl": site_url,
                            "event": event,
                            "message": message,
                            "data": data,
                            "level": level,
                            "createdAt": int(workflow.now().timestamp() * 1000),
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=60),
                )
            except Exception:
                pass

        await _log("workflow.start", message="ProcessWebhookScrape started")

        def _is_retryable_error(error: Exception) -> bool:
            """Return True when the exception should trigger a workflow retry."""

            def _retry_flag(err: Exception | None) -> bool | None:
                if err is None:
                    return None
                if isinstance(err, WorkflowError):
                    return err.retryable
                if isinstance(err, ApplicationError):
                    return not err.non_retryable
                return None

            candidates = [error]
            if isinstance(error, ActivityError) and getattr(error, "cause", None):
                candidates.append(error.cause)  # type: ignore[attr-defined]

            for candidate in candidates:
                flag = _retry_flag(candidate)
                if flag is not None:
                    return flag

            # Handle transient HTTP rate limits surfaced as plain exceptions
            message = str(error).lower()
            if "payment required" in message or "insufficient credits" in message:
                return False
            if "429" in message or "too many requests" in message:
                return True

            return False

        seen_jobs: Set[str] = set()

        try:
            while True:
                events = await workflow.execute_activity(
                    fetch_pending_firecrawl_webhooks,
                    args=[25, None],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
                if not events:
                    break

                for event in events:
                    if not isinstance(event, dict):
                        continue
                    event_id = event.get("_id")
                    event_type = (event.get("event") or "").lower()
                    metadata_raw = event.get("metadata")
                    metadata_event: Dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
                    job_id = str(event.get("jobId") or metadata_event.get("jobId") or event.get("id") or "")
                    site_url_hint = event.get("siteUrl") or metadata_event.get("siteUrl")
                    site_id_hint = event.get("siteId") or metadata_event.get("siteId")

                    dedup_key = f"{event_type}:{job_id}" if job_id else None
                    if dedup_key and dedup_key in seen_jobs:
                        await _log(
                            "webhook.duplicate",
                            site_url=site_url_hint,
                            data={"jobId": job_id, "eventId": event_id, "event": event_type},
                            level="warn",
                        )
                        if event_id:
                            await workflow.execute_activity(
                                mark_firecrawl_webhook_processed,
                                args=[event_id, "duplicate"],
                                schedule_to_close_timeout=timedelta(seconds=30),
                            )
                        continue
                    if dedup_key:
                        seen_jobs.add(dedup_key)
                    processed += 1
                    await _log(
                        "webhook.received",
                        site_url=site_url_hint,
                        data={
                            "eventId": event_id,
                            "siteId": site_id_hint,
                            "event": event.get("event"),
                            "status": event.get("status"),
                            "jobId": job_id or None,
                            "receivedAt": event.get("receivedAt"),
                            "statusUrl": event.get("statusUrl") or event.get("status_url"),
                        },
                    )
                    try:
                        # Short-circuit explicit failure events
                        if "fail" in event_type:
                            site_id = event.get("siteId") or metadata_event.get("siteId")
                            if site_id:
                                await workflow.execute_activity(
                                    fail_site,
                                    args=[{"id": site_id, "error": event.get("status") or event_type}],
                                    start_to_close_timeout=timedelta(seconds=30),
                                )
                            if event_id:
                                await workflow.execute_activity(
                                    mark_firecrawl_webhook_processed,
                                    args=[event_id, event.get("status") or event_type],
                                    schedule_to_close_timeout=timedelta(seconds=30),
                                )
                            continue

                        result = await workflow.execute_activity(
                            collect_firecrawl_job_result,
                            args=[event],
                            start_to_close_timeout=timedelta(minutes=10),
                            retry_policy=RetryPolicy(
                                initial_interval=timedelta(seconds=HTTP_RETRY_BASE_SECONDS),
                                backoff_coefficient=2.0,
                                maximum_interval=timedelta(minutes=5),
                            ),
                        )

                        result_scrape_summary = (
                            _summarize_scrape_payload(result.get("scrape")) if isinstance(result, dict) else {}
                        )
                        result_job_urls = result.get("job_urls") if isinstance(result, dict) else None
                        result_job_urls_count = (
                            len(result_job_urls) if isinstance(result_job_urls, list) else None
                        )

                        await _log(
                            "webhook.collected",
                            site_url=result.get("siteUrl") if isinstance(result, dict) else site_url_hint,
                            data={
                                "eventId": event_id,
                                "siteId": site_id_hint,
                                "jobId": job_id or None,
                                "kind": result.get("kind"),
                                "status": result.get("status"),
                                "httpStatus": result.get("httpStatus"),
                                "jobsScraped": int(result.get("jobsScraped") or 0)
                                if isinstance(result, dict)
                                else None,
                                "itemsCount": result.get("itemsCount"),
                                "jobUrls": result_job_urls_count,
                                "normalizedCount": result_scrape_summary.get("normalizedCount")
                                if result_scrape_summary
                                else None,
                                "sampleJobs": result_scrape_summary.get("sample") if result_scrape_summary else None,
                            }
                            if isinstance(result, dict)
                            else None,
                        )

                        ingested_stored, ingested_jobs, ingested_sites = await _ingest_firecrawl_result(
                            event,
                            result,
                            log=_log,
                            workflow_name="ProcessWebhookScrape",
                        )
                        stored += ingested_stored
                        jobs_scraped += ingested_jobs
                        site_urls.extend(ingested_sites)
                    except Exception as e:  # noqa: BLE001
                        if _is_retryable_error(e):
                            status = "retry"
                            failure_reasons.append(str(e))
                            await _log(
                                "webhook.retry",
                                site_url=site_url_hint,
                                message=str(e),
                                data={"eventId": event_id, "jobId": job_id or None, "siteId": site_id_hint},
                                level="warn",
                            )
                            raise

                        failed += 1
                        status = "failed"
                        site_id = event.get("siteId") or (event.get("metadata") or {}).get("siteId")
                        if site_id:
                            try:
                                await workflow.execute_activity(
                                    fail_site,
                                    args=[{"id": site_id, "error": str(e)}],
                                    start_to_close_timeout=timedelta(seconds=30),
                                )
                            except Exception:
                                # Avoid masking the original error
                                pass
                        if event_id:
                            try:
                                await workflow.execute_activity(
                                    mark_firecrawl_webhook_processed,
                                    args=[event_id, str(e)],
                                    schedule_to_close_timeout=timedelta(seconds=30),
                                )
                            except Exception:
                                pass
                        failure_reasons.append(str(e))

                        await _log(
                            "webhook.error",
                            site_url=site_url_hint,
                            message=str(e),
                            data={"eventId": event_id, "jobId": job_id or None, "siteId": site_id_hint},
                            level="error",
                        )

            return WebhookProcessSummary(
                processed=processed, stored=stored, jobs_scraped=jobs_scraped, failed=failed
            )
        finally:
            completed_at = int(workflow.now().timestamp() * 1000)
            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    args=[
                        {
                            "runId": workflow.info().run_id,
                            "workflowId": workflow.info().workflow_id,
                            "workflowName": "ProcessWebhookScrape",
                            "status": status,
                            "startedAt": started_at,
                            "completedAt": completed_at,
                            "siteUrls": site_urls,
                            "sitesProcessed": len(site_urls),
                            "jobsScraped": jobs_scraped,
                            "workerId": "scraper-worker",
                            "taskQueue": "scraper-task-queue",
                            "error": "; ".join(failure_reasons) if failure_reasons else None,
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                # Log best-effort; do not fail workflow
                pass

            await _log(
                "workflow.complete",
                message="ProcessWebhookScrape finished",
                data={
                    "status": status,
                    "processed": processed,
                    "stored": stored,
                    "failed": failed,
                    "jobsScraped": jobs_scraped,
                },
                level="warn" if status != "completed" else "info",
            )


@dataclass
class WebhookRecoverySummary:
    checked: int
    recovered: int
    failed: int


@workflow.defn(name="RecoverMissingFirecrawlWebhook")
class RecoverMissingFirecrawlWebhookWorkflow:
    @workflow.run
    async def run(self, job: Dict[str, Any]) -> WebhookRecoverySummary:  # type: ignore[override]
        checked = 0
        recovered = 0
        failed = 0
        status = "completed"
        failure_reasons: List[str] = []
        started_at = int(workflow.now().timestamp() * 1000)
        site_urls: List[str] = []
        jobs_scraped = 0
        run_info = workflow.info()

        async def _log(
            event: str,
            *,
            message: str | None = None,
            data: Dict[str, Any] | None = None,
            site_url: str | None = None,
            level: str = "info",
        ) -> None:
            try:
                await workflow.execute_activity(
                    record_scratchpad,
                    args=[
                        {
                            "runId": run_info.run_id,
                            "workflowId": run_info.workflow_id,
                            "workflowName": "RecoverMissingFirecrawlWebhook",
                            "siteUrl": site_url,
                            "event": event,
                            "message": message,
                            "data": data,
                            "level": level,
                            "createdAt": int(workflow.now().timestamp() * 1000),
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=60),
                )
            except Exception:
                pass

        await _log(
            "workflow.start",
            message=f"Recovering Firecrawl job {job.get('jobId')}",
            data={"jobId": job.get("jobId"), "siteUrl": job.get("siteUrl")},
        )

        job_id = str(job.get("jobId") or job.get("id") or "")
        webhook_id = job.get("webhookId")
        metadata_raw = job.get("metadata")
        metadata: Dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
        site_id = job.get("siteId") or metadata.get("siteId")
        site_url = job.get("siteUrl") or metadata.get("siteUrl")
        received_at = int(job.get("receivedAt") or started_at)

        def _now_ms() -> int:
            return int(workflow.now().timestamp() * 1000)

        recheck_target = received_at + int(FIRECRAWL_WEBHOOK_RECHECK.total_seconds() * 1000)
        timeout_target = received_at + int(FIRECRAWL_WEBHOOK_TIMEOUT.total_seconds() * 1000)

        async def _mark_processed_if_needed(error: str | None = None) -> None:
            if webhook_id:
                try:
                    await workflow.execute_activity(
                        mark_firecrawl_webhook_processed,
                        args=[webhook_id, error],
                        schedule_to_close_timeout=timedelta(seconds=30),
                    )
                except Exception:
                    pass

        async def _webhook_state() -> Dict[str, Any]:
            if not job_id:
                return {}
            try:
                res = await workflow.execute_activity(
                    get_firecrawl_webhook_status,
                    args=[job_id],
                    schedule_to_close_timeout=timedelta(seconds=20),
                )
                return res if isinstance(res, dict) else {}
            except Exception:
                return {}

        def _already_delivered(state: Dict[str, Any]) -> bool:
            return bool(state.get("hasProcessed") or state.get("hasRealEvent"))

        try:
            state = await _webhook_state()
            checked += 1
            if _already_delivered(state):
                await _mark_processed_if_needed(
                    None if state.get("pendingProcessed") else "already_delivered"
                )
                await _log(
                    "recovery.skipped",
                    site_url=site_url,
                    data={"reason": "webhook already delivered", "stage": "initial_check"},
                )
                return WebhookRecoverySummary(checked=checked, recovered=recovered, failed=failed)

            initial_delay_ms = max(0, recheck_target - _now_ms())
            if initial_delay_ms > 0:
                await workflow.sleep(timedelta(milliseconds=initial_delay_ms))

                state = await _webhook_state()
                checked += 1

            if _already_delivered(state):
                await _mark_processed_if_needed(
                    None if state.get("pendingProcessed") else "already_delivered"
                )
                await _log(
                    "recovery.skipped",
                    site_url=site_url,
                    data={
                        "reason": "webhook already delivered",
                        "stage": "post_recheck_wait",
                    },
                )
                return WebhookRecoverySummary(checked=checked, recovered=recovered, failed=failed)

            failure_message: Optional[str] = None
            try:
                result = await workflow.execute_activity(
                    collect_firecrawl_job_result,
                    args=[
                        {
                            "_id": webhook_id or job_id,
                            "jobId": job_id,
                            "id": job_id,
                            "metadata": metadata,
                            "statusUrl": job.get("statusUrl"),
                            "siteId": site_id,
                            "siteUrl": site_url,
                            "event": "recovered",
                            "receivedAt": received_at,
                        }
                    ],
                    start_to_close_timeout=timedelta(minutes=10),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=HTTP_RETRY_BASE_SECONDS),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(minutes=5),
                    ),
                )

                await _log(
                    "recovery.collected",
                    site_url=result.get("siteUrl") if isinstance(result, dict) else site_url,
                    data={"status": result.get("status") if isinstance(result, dict) else None},
                )

                _, ingested_jobs, ingested_sites = await _ingest_firecrawl_result(
                    {
                        "_id": webhook_id or job_id,
                        "jobId": job_id,
                        "metadata": metadata,
                        "siteId": site_id,
                        "siteUrl": site_url,
                    },
                    result,
                    log=_log,
                    workflow_name="RecoverMissingFirecrawlWebhook",
                )
                recovered += 1
                site_urls.extend(ingested_sites)
                jobs_scraped += ingested_jobs
                return WebhookRecoverySummary(checked=checked, recovered=recovered, failed=failed)
            except Exception as e:  # noqa: BLE001
                failure_message = str(e)
                failure_reasons.append(failure_message)
                await _log(
                    "recovery.error",
                    site_url=site_url,
                    message=failure_message,
                    level="warn",
                )

            remaining_ms = max(0, timeout_target - _now_ms())
            if remaining_ms > 0:
                await workflow.sleep(timedelta(milliseconds=remaining_ms))
                state = await _webhook_state()
                if state.get("hasProcessed") or state.get("hasRealEvent"):
                    await _mark_processed_if_needed(None if state.get("pendingProcessed") else "already_delivered")
                    await _log(
                        "recovery.skipped",
                        site_url=site_url,
                        data={"reason": "webhook delivered late"},
                    )
                    return WebhookRecoverySummary(checked=checked, recovered=recovered, failed=failed)

            failed += 1
            status = "failed"
            failure_message = failure_message or "Firecrawl webhook missing after timeout"
            if failure_message not in failure_reasons:
                failure_reasons.append(failure_message)
            if site_id:
                try:
                    await workflow.execute_activity(
                        fail_site,
                        args=[{"id": site_id, "error": failure_message}],
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                except Exception:
                    pass
            await _mark_processed_if_needed(failure_message)
            await _log(
                "recovery.failed",
                site_url=site_url,
                message=failure_message,
                level="error",
            )
            return WebhookRecoverySummary(checked=checked, recovered=recovered, failed=failed)
        finally:
            completed_at = int(workflow.now().timestamp() * 1000)
            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    args=[
                        {
                            "runId": workflow.info().run_id,
                            "workflowId": workflow.info().workflow_id,
                            "workflowName": "RecoverMissingFirecrawlWebhook",
                            "status": status,
                            "startedAt": started_at,
                            "completedAt": completed_at,
                            "siteUrls": site_urls or ([site_url] if site_url else []),
                            "sitesProcessed": len(site_urls) or (1 if site_url else 0),
                            "jobsScraped": jobs_scraped,
                            "workerId": "scraper-worker",
                            "taskQueue": "scraper-task-queue",
                            "error": "; ".join(failure_reasons) if failure_reasons else None,
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass

            await _log(
                "workflow.complete",
                site_url=site_url,
                data={
                    "status": status,
                    "checked": checked,
                    "recovered": recovered,
                    "failed": failed,
                },
                level="warn" if status != "completed" else "info",
            )


# Backward compatibility: old name kept so existing workers/tests keep running.
class ProcessWebhookScrapeWorkflow(ProcessWebhookIngestWorkflow):
    pass
