from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError

# Import activity call prototypes inside workflow via type hints / names
with workflow.unsafe.imports_passed_through():
    from .activities import (
        SPIDERCLOUD_BATCH_SIZE,
        complete_scrape_urls,
        complete_site,
        crawl_site_fetchfox,
        fail_site,
        lease_scrape_url_batch,
        process_spidercloud_listing_batch,
        lease_site,
        process_spidercloud_job_batch,
        fail_listing_batch_urls,
        record_workflow_run,
        scrape_site,
        scrape_site_firecrawl,
        store_scrape,
    )

from ..config import runtime_config, settings
from .helpers.workflow_logging import get_workflow_logger
from .helpers.workflow_debug import workflow_checkpoint


@dataclass
class ScrapeSummary:
    site_count: int
    scrape_ids: List[str]


def summarize_scrape_result(res: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(res, dict):
        return {"provider": "unknown"}

    items = res.get("items") if isinstance(res, dict) else {}
    normalized = items.get("normalized") if isinstance(items, dict) else None
    jobs = len(normalized) if isinstance(normalized, list) else 0
    skipped_urls = res.get("skippedUrls") if isinstance(res, dict) else None
    summary: Dict[str, Any] = {
        "provider": res.get("provider") or items.get("provider") if isinstance(items, dict) else None,
        "queued": items.get("queued") if isinstance(items, dict) else None,
        "jobId": items.get("jobId") if isinstance(items, dict) else None,
        "statusUrl": items.get("statusUrl") if isinstance(items, dict) else None,
        "jobs": jobs,
    }
    if skipped_urls:
        summary["skippedUrls"] = skipped_urls
    if res.get("workflowName"):
        summary["workflowName"] = res.get("workflowName")
    if res.get("costMilliCents"):
        summary["costMilliCents"] = res.get("costMilliCents")
    return {k: v for k, v in summary.items() if v is not None}


def _build_completion_item(entry: dict | str) -> dict | None:
    if isinstance(entry, str):
        url_val = entry
        item: dict[str, Any] = {"url": url_val}
        return item if url_val.strip() else None
    if not isinstance(entry, dict):
        return None
    url_val = entry.get("url")
    if not isinstance(url_val, str) or not url_val.strip():
        return None
    item: dict[str, Any] = {"url": url_val}
    row_id = entry.get("_id")
    if isinstance(row_id, str):
        item["id"] = row_id
    source_val = entry.get("sourceUrl")
    if isinstance(source_val, str):
        item["sourceUrl"] = source_val
    provider_val = entry.get("provider")
    if isinstance(provider_val, str):
        item["provider"] = provider_val
    site_val = entry.get("siteId")
    if isinstance(site_val, str):
        item["siteId"] = site_val
    attempts_val = entry.get("attempts")
    if isinstance(attempts_val, (int, float)):
        item["attempts"] = int(attempts_val)
    return item


def _workflow_now_ms() -> int:
    """
    Return current workflow time in ms; fall back to wall clock when outside a workflow loop.
    """

    try:
        return int(workflow.now().timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


async def _safe_workflow_sleep(seconds: float) -> None:
    """Sleep inside a workflow loop; no-op when running in unit tests."""

    try:
        await workflow.sleep(seconds)
    except Exception as exc:
        if exc.__class__.__name__ == "_NotInWorkflowEventLoopError":
            return
        raise


def _format_failure_reason(exc: Exception) -> str:
    if isinstance(exc, ActivityError):
        parts = [f"activity={exc.activity_type}"]
        if exc.activity_id:
            parts.append(f"activityId={exc.activity_id}")
        cause = exc.cause
        if isinstance(cause, TimeoutError):
            timeout_type = cause.type.name if cause.type else "unknown"
            parts.append(f"timeout={timeout_type}")
            return " ".join(parts)
        if cause is not None:
            parts.append(f"cause={cause.__class__.__name__}")
            if str(cause):
                parts.append(f"message={cause}")
            return " ".join(parts)
        if str(exc):
            parts.append(f"message={exc}")
        return " ".join(parts)
    if isinstance(exc, TimeoutError):
        timeout_type = exc.type.name if exc.type else "unknown"
        return f"timeout={timeout_type}"
    if str(exc):
        return f"{exc.__class__.__name__}: {exc}"
    return exc.__class__.__name__


def _format_batch_url_sample(entries: list | None, *, limit: int = 3) -> str | None:
    if not entries:
        return None
    urls: list[str] = []
    for entry in entries:
        if isinstance(entry, dict):
            url_val = entry.get("url")
            if isinstance(url_val, str) and url_val.strip():
                urls.append(url_val.strip())
        elif isinstance(entry, str) and entry.strip():
            urls.append(entry.strip())
        if len(urls) >= limit:
            break
    if not urls:
        return None
    return ", ".join(urls)


async def _yield_if_needed(iteration: int, *, every: int = 50) -> None:
    """Cooperatively yield so large in-memory loops don't block workflow progress."""

    if iteration > 0 and iteration % every == 0:
        await _safe_workflow_sleep(0)


async def _run_scrape_workflow(
    scrape_activity,
    workflow_name: str,
    *,
    scrape_provider: str | None = "fetchfox",
    activity_timeout: timedelta = timedelta(minutes=10),
    max_leases: int | None = None,
    persist_scrapes: bool = False,
) -> ScrapeSummary:
    scrape_ids: List[str] = []
    leased_count = 0
    site_urls: List[str] = []
    started_at = _workflow_now_ms()
    status = "completed"
    failure_reasons: List[str] = []
    run_info = workflow.info()

    wf_logger = get_workflow_logger()

    def _emit(event: str, *, level: str = "info", **payload: Any) -> None:
        msg = f"{workflow_name} | event={event} | {payload}"
        if level == "error":
            wf_logger.error(msg)
        elif level == "warn" or level == "warning":
            wf_logger.warning(msg)
        else:
            wf_logger.info(msg)

    async def _log(
        event: str,
        *,
        message: str | None = None,
        data: Dict[str, Any] | None = None,
        site_url: str | None = None,
        level: str = "info",
    ) -> None:
        _emit(event, level=level, siteUrl=site_url, message=message, data=data)

    await _log("workflow.start", message="Scrape workflow started")

    try:
        # Keep leasing jobs until none available (or max_leases reached).
        while True:
            if max_leases is not None and leased_count >= max_leases:
                break
            lease_args = ["scraper-worker", 300, None, scrape_provider]
            await workflow_checkpoint(
                "scraper.before_lease",
                location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:before_lease",
                data={
                    "leasedCount": leased_count,
                    "maxLeases": max_leases,
                    "provider": scrape_provider,
                    "leaseArgs": lease_args,
                },
            )
            site = await workflow.execute_activity(
                lease_site,
                args=lease_args,
                schedule_to_close_timeout=timedelta(seconds=30),
            )

            if not site:
                break

            leased_count += 1
            site_urls.append(site["url"])
            await workflow_checkpoint(
                "scraper.after_lease",
                location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:after_lease",
                data={
                    "leasedCount": leased_count,
                    "siteUrl": site.get("url"),
                    "siteId": site.get("_id"),
                    "pattern": site.get("pattern"),
                },
            )
            await _log(
                "site.leased",
                site_url=site["url"],
                data={"siteId": site.get("_id"), "pattern": site.get("pattern")},
            )

            try:
                workflow_context = {
                    "workflowName": workflow_name,
                    "workflowId": run_info.workflow_id,
                    "runId": run_info.run_id,
                }
                activity_args = [site, workflow_context, persist_scrapes]
                if scrape_activity is scrape_site_firecrawl:
                    activity_args = [site, None, workflow_context, persist_scrapes]
                await workflow_checkpoint(
                    "scraper.before_activity",
                    location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:before_activity",
                    data={
                        "activity": getattr(scrape_activity, "__name__", "scrape_activity"),
                        "siteUrl": site.get("url"),
                        "siteId": site.get("_id"),
                        "persistScrapes": persist_scrapes,
                    },
                )
                res = await workflow.execute_activity(
                    scrape_activity,
                    args=activity_args,
                    start_to_close_timeout=activity_timeout,
                )
                # Tag scrape payload with workflow name for downstream storage
                scrape_id = None
                summary = None
                recovery_payload = None
                if isinstance(res, dict):
                    scrape_id = res.get("scrapeId") if persist_scrapes else None
                    summary = res.get("summary")
                    recovery_payload = res.get("recoveryPayload")
                await workflow_checkpoint(
                    "scraper.after_activity",
                    location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:after_activity",
                    data={
                        "siteUrl": site.get("url"),
                        "siteId": site.get("_id"),
                        "persistScrapes": persist_scrapes,
                        "hasScrapeId": bool(scrape_id),
                        "hasSummary": isinstance(summary, dict),
                        "hasRecoveryPayload": bool(recovery_payload),
                    },
                )

                if persist_scrapes and scrape_id:
                    scrape_ids.append(scrape_id)
                else:
                    if isinstance(res, dict):
                        res_dict: Dict[str, Any] = res
                        res_dict.setdefault("workflowName", workflow_name)
                        res_dict.setdefault("siteId", site.get("_id"))
                        res_dict.setdefault("workflowId", run_info.workflow_id)
                        res_dict.setdefault("runId", run_info.run_id)
                        items_raw = res_dict.get("items")
                        items: Dict[str, Any] = items_raw if isinstance(items_raw, dict) else {}
                        job_id = res_dict.get("jobId") or items.get("jobId")
                        if items.get("queued") and job_id:
                            recovery_payload = {
                                "jobId": str(job_id),
                                "webhookId": res_dict.get("webhookId") or items.get("webhookId"),
                                "metadata": res_dict.get("metadata"),
                                "siteId": site.get("_id"),
                                "siteUrl": site.get("url"),
                                "statusUrl": res_dict.get("statusUrl") or items.get("statusUrl"),
                                "receivedAt": res_dict.get("receivedAt") or items.get("receivedAt"),
                            }

                    await workflow_checkpoint(
                        "scraper.before_store_scrape",
                        location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:before_store_scrape",
                        data={
                            "siteUrl": site.get("url"),
                            "siteId": site.get("_id"),
                            "persistScrapes": persist_scrapes,
                        },
                    )
                    scrape_id = await workflow.execute_activity(
                        store_scrape,
                        args=[res],
                        schedule_to_close_timeout=timedelta(minutes=3),
                        start_to_close_timeout=timedelta(minutes=3),
                    )
                    scrape_ids.append(scrape_id)
                    summary = summarize_scrape_result(res) if isinstance(res, dict) else {"provider": "unknown"}

                if recovery_payload and recovery_payload.get("jobId"):
                    await workflow_checkpoint(
                        "scraper.before_start_recovery",
                        location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:before_start_recovery",
                        data={
                            "siteUrl": site.get("url"),
                            "siteId": site.get("_id"),
                            "jobId": recovery_payload.get("jobId"),
                        },
                    )
                    try:
                        await workflow.start_child_workflow(
                            "RecoverMissingFirecrawlWebhook",
                            recovery_payload,
                            id=f"wf-firecrawl-recovery-{recovery_payload['jobId']}",
                            task_queue=workflow.info().task_queue,
                        )
                    except Exception as start_err:  # noqa: BLE001
                        await _log(
                            "recovery.start_failed",
                            site_url=site.get("url"),
                            message=str(start_err),
                            level="warn",
                        )

                await _log(
                    "scrape.result",
                    site_url=site["url"],
                    data=summary if isinstance(summary, dict) else summarize_scrape_result(res),
                )

                # Mark site completed so next lease skips it
                await workflow_checkpoint(
                    "scraper.before_complete_site",
                    location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:before_complete_site",
                    data={"siteUrl": site.get("url"), "siteId": site.get("_id")},
                )
                await workflow.execute_activity(
                    complete_site,
                    args=[site["_id"]],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception as e:  # noqa: BLE001
                reason = _format_failure_reason(e)
                # On failure, record and release the lock for retry after TTL or immediately
                await workflow_checkpoint(
                    "scraper.before_fail_site",
                    location=f"scrape_workflow:_run_scrape_workflow:{workflow_name}:before_fail_site",
                    data={"siteUrl": site.get("url"), "siteId": site.get("_id")},
                )
                await workflow.execute_activity(
                    fail_site,
                    args=[{"id": site["_id"], "error": reason}],
                    start_to_close_timeout=timedelta(seconds=30),
                )
                status = "failed"
                if isinstance(e, ActivityError) and e.cause:
                    failure_reasons.append(f"{site['url']}: {reason}")
                elif isinstance(e, ApplicationError):
                    failure_reasons.append(f"{site['url']}: {reason}")
                else:
                    failure_reasons.append(f"{site['url']}: {reason}")

                await _log(
                    "site.error",
                    site_url=site["url"],
                    message=reason,
                    level="error",
                )
            await _yield_if_needed(leased_count, every=5)

        return ScrapeSummary(site_count=leased_count, scrape_ids=scrape_ids)
    except Exception as e:  # noqa: BLE001
        status = "failed"
        reason = _format_failure_reason(e)
        failure_reasons.append(reason)
        await _log("workflow.error", message=reason, level="error")
        raise
    finally:
        completed_at = _workflow_now_ms()
        if not site_urls:
            failure_reasons.append("No sites were leased (siteUrls empty).")
        try:
            await workflow.execute_activity(
                record_workflow_run,
                args=[
                    {
                        "runId": workflow.info().run_id,
                        "workflowId": workflow.info().workflow_id,
                        "workflowName": workflow_name,
                        "status": status,
                        "startedAt": started_at,
                        "completedAt": completed_at,
                        "siteUrls": site_urls,
                        "sitesProcessed": leased_count,
                        "jobsScraped": len(scrape_ids),
                        "workerId": "scraper-worker",
                        "taskQueue": "scraper-task-queue",
                        "error": "; ".join(failure_reasons) if failure_reasons else None,
                    }
                ],
                schedule_to_close_timeout=timedelta(seconds=30),
            )
        except Exception:
            # Best-effort; do not fail workflow on log write issues
            pass

        await _log(
            "workflow.complete",
            message="Scrape workflow finished",
            data={
                "status": status,
                "sitesProcessed": leased_count,
                "jobsScraped": len(scrape_ids),
            },
            level="warn" if status != "completed" else "info",
        )


@workflow.defn(name="ScrapeWorkflow")
class ScrapeWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        return await _run_scrape_workflow(
            scrape_site,
            "ScrapeWorkflow",
            scrape_provider="fetchfox",
            persist_scrapes=settings.persist_scrapes_in_activity,
        )


@workflow.defn(name="ScraperFirecrawl")
class FirecrawlScrapeWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        return await _run_scrape_workflow(
            scrape_site_firecrawl,
            "ScraperFirecrawl",
            scrape_provider="firecrawl",
            persist_scrapes=settings.persist_scrapes_in_activity,
        )


@workflow.defn(name="FetchfoxSpidercloud")
class FetchfoxSpidercloudWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        return await _run_scrape_workflow(
            crawl_site_fetchfox,
            "FetchfoxSpidercloud",
            scrape_provider="fetchfox_spidercloud",
            activity_timeout=timedelta(minutes=10),
            max_leases=1,
            persist_scrapes=settings.persist_scrapes_in_activity,
        )


@workflow.defn(name="ScraperSpidercloud")
class SpidercloudScrapeWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        return await _run_scrape_workflow(
            scrape_site,
            "ScraperSpidercloud",
            scrape_provider="spidercloud",
            activity_timeout=timedelta(minutes=25),
            max_leases=1,
            persist_scrapes=settings.persist_scrapes_in_activity,
        )


@workflow.defn(name="SpidercloudJobDetails")
class SpidercloudJobDetailsWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        scrape_ids: list[str] = []
        site_count = 0
        started_at = _workflow_now_ms()
        status = "completed"
        failure_reasons: list[str] = []
        run_info = workflow.info()
        wf_logger = get_workflow_logger()
        persist_scrapes = settings.persist_scrapes_in_activity

        async def _log(event: str, *, level: str = "info", data: dict | None = None):
            msg = f"SpidercloudJobDetails | event={event} | data={data}"
            if level == "error":
                wf_logger.error(msg)
            elif level in {"warn", "warning"}:
                wf_logger.warning(msg)
            else:
                wf_logger.info(msg)

        async def _complete_urls(entries: list[dict] | list[str], status: str, error: str | None = None) -> None:
            if not entries:
                return
            chunk_size = 100
            await workflow_checkpoint(
                "job_details.complete_urls.start",
                location="scrape_workflow:SpidercloudJobDetails.run:complete_urls:start",
                data={"status": status, "total": len(entries), "chunkSize": chunk_size, "error": error},
            )
            for idx, start in enumerate(range(0, len(entries), chunk_size)):
                chunk = entries[start : start + chunk_size]
                await workflow_checkpoint(
                    "job_details.complete_urls.chunk",
                    location="scrape_workflow:SpidercloudJobDetails.run:complete_urls:chunk",
                    data={"status": status, "chunkIndex": idx, "chunkSize": len(chunk)},
                )
                items = []
                for entry in chunk:
                    item = _build_completion_item(entry)
                    if item:
                        item["isListingUrl"] = True
                        items.append(item)
                payload: dict[str, Any] = {"items": items, "status": status}
                if error:
                    payload["error"] = error
                await workflow.execute_activity(
                    complete_scrape_urls,
                    args=[payload],
                    schedule_to_close_timeout=timedelta(seconds=20),
                )
                await _yield_if_needed(idx, every=5)
            await workflow_checkpoint(
                "job_details.complete_urls.done",
                location="scrape_workflow:SpidercloudJobDetails.run:complete_urls:done",
                data={"status": status, "total": len(entries)},
            )

        await _log("workflow.start")

        try:
            await workflow_checkpoint(
                "job_details.before_lease",
                location="scrape_workflow:SpidercloudJobDetails.run:before_lease",
                data={
                    "provider": "spidercloud",
                    "urlType": "detail",
                    "limit": SPIDERCLOUD_BATCH_SIZE,
                },
            )
            batch = await workflow.execute_activity(
                lease_scrape_url_batch,
                args=["spidercloud", SPIDERCLOUD_BATCH_SIZE, "detail"],
                schedule_to_close_timeout=timedelta(seconds=20),
            )
            await _safe_workflow_sleep(0)

            skipped_urls = []
            skipped_count = 0
            if isinstance(batch, dict):
                raw_skipped = batch.get("skippedUrls")
                if isinstance(raw_skipped, list):
                    skipped_count = len(raw_skipped)
                    for idx, entry in enumerate(raw_skipped):
                        if isinstance(entry, str):
                            skipped_urls.append(entry)
                        if idx and idx % 200 == 0:
                            await _safe_workflow_sleep(0)
                await workflow_checkpoint(
                    "job_details.process_skipped_urls",
                    location="scrape_workflow:SpidercloudJobDetails.run:process_skipped_urls",
                    data={"skippedCount": skipped_count},
                )
                await _safe_workflow_sleep(0)
            if skipped_urls:
                await _log(
                    "batch.skipped_urls",
                    data={"count": len(skipped_urls), "sample": skipped_urls[:25]},
                )

            urls = batch.get("urls") if isinstance(batch, dict) else None
            if not urls:
                return ScrapeSummary(site_count=site_count, scrape_ids=scrape_ids)

            await workflow_checkpoint(
                "job_details.before_log_batch_leased",
                location="scrape_workflow:SpidercloudJobDetails.run:before_log_batch_leased",
                data={"batchUrls": len(urls), "skippedCount": skipped_count},
            )
            await _log("batch.leased", data={"count": len(urls), "skippedCount": skipped_count})
            await _safe_workflow_sleep(0)

            try:
                await workflow_checkpoint(
                    "job_details.before_process_batch",
                    location="scrape_workflow:SpidercloudJobDetails.run:before_process_batch",
                    data={"batchUrls": len(urls), "skippedCount": skipped_count},
                )
                res = await workflow.execute_activity(
                    process_spidercloud_job_batch,
                    args=[batch, persist_scrapes],
                    start_to_close_timeout=timedelta(
                        minutes=runtime_config.spidercloud_job_details_timeout_minutes
                    ),
                )
                await _safe_workflow_sleep(0)
                scrapes = res.get("scrapes") if isinstance(res, dict) else []
                scrape_ids_payload = res.get("scrapeIds") if isinstance(res, dict) else None
                stored_count = res.get("stored") if isinstance(res, dict) else None
                invalid_count = res.get("invalid") if isinstance(res, dict) else None
                failed_count = res.get("failed") if isinstance(res, dict) else None

                if persist_scrapes:
                    if isinstance(scrape_ids_payload, list):
                        scrape_ids.extend([sid for sid in scrape_ids_payload if isinstance(sid, str)])
                        await _safe_workflow_sleep(0)

                    if stored_count is None and isinstance(scrape_ids_payload, list):
                        stored_count = len(scrape_ids_payload)

                    if isinstance(stored_count, int) or isinstance(invalid_count, int) or isinstance(failed_count, int):
                        await workflow_checkpoint(
                            "job_details.before_log_batch_store",
                            location="scrape_workflow:SpidercloudJobDetails.run:before_log_batch_store",
                            data={
                                "stored": stored_count,
                                "invalid": invalid_count,
                                "failed": failed_count,
                            },
                        )
                        await _log(
                            "batch.store",
                            data={
                                "scrapes": stored_count,
                                "completed": stored_count,
                                "invalid": invalid_count,
                                "failed": failed_count,
                            },
                        )
                        await _safe_workflow_sleep(0)

                    if not isinstance(scrapes, list):
                        scrapes = []

                    if scrapes:
                        await workflow_checkpoint(
                            "job_details.before_log_batch_processed",
                            location="scrape_workflow:SpidercloudJobDetails.run:before_log_batch_processed",
                            data={"scrapes": len(scrapes)},
                        )
                        await _log("batch.processed", data={"count": len(scrapes)})
                    else:
                        await workflow_checkpoint(
                            "job_details.before_log_batch_processed_empty",
                            location="scrape_workflow:SpidercloudJobDetails.run:before_log_batch_processed_empty",
                            data={"urls": len(urls)},
                        )
                        await _log("batch.processed", data={"count": len(urls)})
                    await _safe_workflow_sleep(0)
                else:
                    if not isinstance(scrapes, list):
                        scrapes = []

                    completed_count = 0
                    invalid_count = 0
                    failed_count = 0
                    completed_urls: list[str] = []
                    invalid_urls: list[str] = []
                    failed_urls: list[str] = []

                    def _scrape_url(scrape: Dict[str, Any]) -> str | None:
                        sub_urls = scrape.get("subUrls")
                        if isinstance(sub_urls, list):
                            for entry in sub_urls:
                                if isinstance(entry, str) and entry.strip():
                                    return entry
                        source_url = scrape.get("sourceUrl")
                        if isinstance(source_url, str) and source_url.strip():
                            return source_url
                        return None

                    if scrapes:
                        await workflow_checkpoint(
                            "job_details.store_scrapes_loop",
                            location="scrape_workflow:SpidercloudJobDetails.run:store_scrapes_loop",
                            data={"scrapes": len(scrapes), "urls": len(urls)},
                        )
                        for idx, scrape in enumerate(scrapes):
                            if not isinstance(scrape, dict):
                                continue
                            url_val = _scrape_url(scrape)
                            try:
                                res_id = await workflow.execute_activity(
                                    store_scrape,
                                    args=[scrape],
                                    schedule_to_close_timeout=timedelta(minutes=3),
                                    start_to_close_timeout=timedelta(minutes=3),
                                )
                                if isinstance(res_id, str):
                                    scrape_ids.append(res_id)
                                completed_count += 1
                                if isinstance(url_val, str):
                                    completed_urls.append(url_val)
                            except ActivityError as exc:
                                cause = exc.cause
                                if isinstance(cause, ApplicationError) and cause.type == "invalid_scrape":
                                    invalid_count += 1
                                    if isinstance(url_val, str):
                                        invalid_urls.append(url_val)
                                else:
                                    failed_count += 1
                                    if isinstance(url_val, str):
                                        failed_urls.append(url_val)
                            except ApplicationError as exc:
                                if exc.type == "invalid_scrape":
                                    invalid_count += 1
                                    if isinstance(url_val, str):
                                        invalid_urls.append(url_val)
                                else:
                                    failed_count += 1
                                    if isinstance(url_val, str):
                                        failed_urls.append(url_val)
                            except Exception:
                                failed_count += 1
                                if isinstance(url_val, str):
                                    failed_urls.append(url_val)
                            await _yield_if_needed(idx)
                        await _safe_workflow_sleep(0)

                        if completed_urls:
                            await workflow_checkpoint(
                                "job_details.complete_urls",
                                location="scrape_workflow:SpidercloudJobDetails.run:complete_urls",
                                data={"completed": len(completed_urls)},
                            )
                            await _complete_urls(completed_urls, "completed")
                        if invalid_urls:
                            await workflow_checkpoint(
                                "job_details.complete_invalid",
                                location="scrape_workflow:SpidercloudJobDetails.run:complete_invalid",
                                data={"invalid": len(invalid_urls)},
                            )
                            await _complete_urls(invalid_urls, "invalid", error="invalid_job_data")
                        if failed_urls:
                            await workflow_checkpoint(
                                "job_details.complete_failed",
                                location="scrape_workflow:SpidercloudJobDetails.run:complete_failed",
                                data={"failed": len(failed_urls)},
                            )
                            await _complete_urls(failed_urls, "failed", error="store_scrape_failed")

                        await _log(
                            "batch.store",
                            data={
                                "scrapes": len(scrapes),
                                "completed": completed_count,
                                "invalid": invalid_count,
                                "failed": failed_count,
                            },
                        )

                    await _log("batch.processed", data={"count": len(scrapes) or len(urls)})
                    await _safe_workflow_sleep(0)
                site_count += 1
            except Exception as exc:  # noqa: BLE001
                reason = _format_failure_reason(exc)
                sample = _format_batch_url_sample(urls if isinstance(urls, list) else None)
                reason_with_urls = f"{reason} urls={sample}" if sample else reason
                await _log("batch.error", level="error", data={"error": reason_with_urls})
                failure_reasons.append(reason_with_urls)
                status = "failed"
                # Release leased URLs so they can be retried
                try:
                    await workflow_checkpoint(
                        "job_details.fail_cleanup",
                        location="scrape_workflow:SpidercloudJobDetails.run:fail_cleanup",
                    )
                    leased_entries: list[dict] = []
                    if isinstance(batch, dict):
                        raw_urls = batch.get("urls")
                        if isinstance(raw_urls, list):
                            for idx, entry in enumerate(raw_urls):
                                if isinstance(entry, dict):
                                    leased_entries.append(entry)
                                await _yield_if_needed(idx)
                        if leased_entries:
                            await workflow_checkpoint(
                                "job_details.fail_cleanup.complete_urls",
                                location="scrape_workflow:SpidercloudJobDetails.run:fail_cleanup_complete_urls",
                                data={"leased": len(leased_entries)},
                            )
                            await _complete_urls(leased_entries, "failed", error=reason_with_urls)
                except Exception:
                    pass

            return ScrapeSummary(site_count=site_count, scrape_ids=scrape_ids)
        except Exception as exc:  # noqa: BLE001
            failure_reasons.append(str(exc))
            status = "failed"
            raise
        finally:
            completed_at = _workflow_now_ms()
            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    args=[
                        {
                            "runId": run_info.run_id,
                            "workflowId": run_info.workflow_id,
                            "workflowName": "SpidercloudJobDetails",
                            "status": status,
                            "startedAt": started_at,
                            "completedAt": completed_at,
                            "siteUrls": [],
                            "sitesProcessed": site_count,
                            "jobsScraped": len(scrape_ids),
                            "workerId": "scraper-worker",
                            "taskQueue": run_info.task_queue,
                            "error": "; ".join(failure_reasons) if failure_reasons else None,
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass


@workflow.defn(name="SpidercloudListing")
class SpidercloudListingWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        scrape_ids: list[str] = []
        site_count = 0
        started_at = _workflow_now_ms()
        status = "completed"
        failure_reasons: list[str] = []
        run_info = workflow.info()
        wf_logger = get_workflow_logger()
        async def _log(event: str, *, level: str = "info", data: dict | None = None):
            msg = f"SpidercloudListing | event={event} | data={data}"
            if level == "error":
                wf_logger.error(msg)
            elif level in {"warn", "warning"}:
                wf_logger.warning(msg)
            else:
                wf_logger.info(msg)

        await _log("workflow.start")

        try:
            await workflow_checkpoint(
                "listing.before_lease",
                location="scrape_workflow:SpidercloudListing.run:before_lease",
                data={
                    "provider": "spidercloud",
                    "urlType": "listing",
                    "limit": runtime_config.spidercloud_listing_batch_size,
                },
            )
            batch = await workflow.execute_activity(
                lease_scrape_url_batch,
                args=["spidercloud", runtime_config.spidercloud_listing_batch_size, "listing"],
                schedule_to_close_timeout=timedelta(seconds=20),
            )
            await _safe_workflow_sleep(0)

            skipped_urls = []
            skipped_count = 0
            if isinstance(batch, dict):
                raw_skipped = batch.get("skippedUrls")
                if isinstance(raw_skipped, list):
                    skipped_count = len(raw_skipped)
                    for idx, entry in enumerate(raw_skipped):
                        if isinstance(entry, str):
                            skipped_urls.append(entry)
                        if idx and idx % 200 == 0:
                            await _safe_workflow_sleep(0)
                await workflow_checkpoint(
                    "listing.process_skipped_urls",
                    location="scrape_workflow:SpidercloudListing.run:process_skipped_urls",
                    data={"skippedCount": skipped_count},
                )
                await _safe_workflow_sleep(0)
            if skipped_urls:
                await _log(
                    "batch.skipped_urls",
                    data={"count": len(skipped_urls), "sample": skipped_urls[:25]},
                )

            urls = batch.get("urls") if isinstance(batch, dict) else None
            if not urls:
                return ScrapeSummary(site_count=site_count, scrape_ids=scrape_ids)

            await workflow_checkpoint(
                "listing.before_log_batch_leased",
                location="scrape_workflow:SpidercloudListing.run:before_log_batch_leased",
                data={"batchUrls": len(urls), "skippedCount": skipped_count},
            )
            await _log("batch.leased", data={"count": len(urls), "skippedCount": skipped_count})
            await _safe_workflow_sleep(0)

            try:
                await workflow_checkpoint(
                    "listing.before_process_batch",
                    location="scrape_workflow:SpidercloudListing.run:before_process_batch",
                    data={"batchUrls": len(urls), "skippedCount": skipped_count},
                )
                res = await workflow.execute_activity(
                    process_spidercloud_listing_batch,
                    args=[batch],
                    start_to_close_timeout=timedelta(
                        minutes=runtime_config.spidercloud_job_details_timeout_minutes
                    ),
                )
                await _safe_workflow_sleep(0)
                queued = res.get("queued") if isinstance(res, dict) else None
                listing_completed = res.get("listingCompleted") if isinstance(res, dict) else None
                await workflow_checkpoint(
                    "listing.before_log_batch_processed",
                    location="scrape_workflow:SpidercloudListing.run:before_log_batch_processed",
                    data={
                        "batchUrls": len(urls),
                        "queued": queued,
                        "listingCompleted": listing_completed,
                    },
                )
                await _log(
                    "batch.processed",
                    data={
                        "queued": queued,
                        "listingCompleted": listing_completed,
                        "batchCount": len(urls),
                    },
                )
                site_count += 1
            except Exception as exc:  # noqa: BLE001
                reason = _format_failure_reason(exc)
                sample = _format_batch_url_sample(urls if isinstance(urls, list) else None)
                reason_with_urls = f"{reason} urls={sample}" if sample else reason
                await _log("batch.error", level="error", data={"error": reason_with_urls})
                failure_reasons.append(reason_with_urls)
                status = "failed"
                # Release leased URLs so they can be retried
                try:
                    await workflow_checkpoint(
                        "listing.fail_cleanup",
                        location="scrape_workflow:SpidercloudListing.run:fail_cleanup",
                        data={"batchUrls": len(urls)},
                    )
                    await workflow.execute_activity(
                        fail_listing_batch_urls,
                        args=[batch, reason_with_urls],
                        schedule_to_close_timeout=timedelta(seconds=20),
                    )
                except Exception:
                    pass

            return ScrapeSummary(site_count=site_count, scrape_ids=scrape_ids)
        except Exception as exc:  # noqa: BLE001
            failure_reasons.append(str(exc))
            status = "failed"
            raise
        finally:
            completed_at = _workflow_now_ms()
            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    args=[
                        {
                            "runId": run_info.run_id,
                            "workflowId": run_info.workflow_id,
                            "workflowName": "SpidercloudListing",
                            "status": status,
                            "startedAt": started_at,
                            "completedAt": completed_at,
                            "siteUrls": [],
                            "sitesProcessed": site_count,
                            "jobsScraped": len(scrape_ids),
                            "workerId": "scraper-worker",
                            "taskQueue": run_info.task_queue,
                            "error": "; ".join(failure_reasons) if failure_reasons else None,
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass
