from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import logging

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

# Import activity call prototypes inside workflow via type hints / names
with workflow.unsafe.imports_passed_through():
    from .activities import (
        SPIDERCLOUD_BATCH_SIZE,
        complete_scrape_urls,
        complete_site,
        crawl_site_fetchfox,
        fail_site,
        lease_scrape_url_batch,
        lease_site,
        process_spidercloud_job_batch,
        record_scratchpad,
        record_workflow_run,
        scrape_site,
        scrape_site_firecrawl,
        store_scrape,
    )

from .scratchpad_utils import extract_http_exchange
from ..config import runtime_config


logger = logging.getLogger("temporal.worker.scrape")


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


def _workflow_now_ms() -> int:
    """
    Return current workflow time in ms; fall back to wall clock when outside a workflow loop.
    """

    try:
        return int(workflow.now().timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


async def _run_scrape_workflow(
    scrape_activity,
    workflow_name: str,
    *,
    scrape_provider: str | None = "fetchfox",
    activity_timeout: timedelta = timedelta(minutes=10),
) -> ScrapeSummary:
    scrape_ids: List[str] = []
    leased_count = 0
    site_urls: List[str] = []
    started_at = _workflow_now_ms()
    status = "completed"
    failure_reasons: List[str] = []
    run_info = workflow.info()

    def _emit(event: str, *, level: str = "info", **payload: Any) -> None:
        msg = f"{workflow_name} | event={event} | {payload}"
        if level == "error":
            logger.error(msg)
        elif level == "warn" or level == "warning":
            logger.warning(msg)
        else:
            logger.info(msg)

    async def _log(
        event: str,
        *,
        message: str | None = None,
        data: Dict[str, Any] | None = None,
        site_url: str | None = None,
        level: str = "info",
    ) -> None:
        _emit(event, level=level, siteUrl=site_url, message=message, data=data)
        try:
            await workflow.execute_activity(
                record_scratchpad,
                args=[
                    {
                        "runId": run_info.run_id,
                        "workflowId": run_info.workflow_id,
                        "workflowName": workflow_name,
                        "siteUrl": site_url,
                        "event": event,
                        "message": message,
                        "data": data,
                        "level": level,
                        "createdAt": _workflow_now_ms(),
                    }
                ],
                schedule_to_close_timeout=timedelta(seconds=60),
            )
        except Exception:
            # Best-effort logging only
            pass

    await _log("workflow.start", message="Scrape workflow started")

    try:
        # Keep leasing jobs until none available
        while True:
            lease_args = ["scraper-worker", 300, None, scrape_provider]
            site = await workflow.execute_activity(
                lease_site,
                args=lease_args,
                schedule_to_close_timeout=timedelta(seconds=30),
            )

            if not site:
                break

            leased_count += 1
            site_urls.append(site["url"])
            await _log(
                "site.leased",
                site_url=site["url"],
                data={"siteId": site.get("_id"), "pattern": site.get("pattern")},
            )

            try:
                res = await workflow.execute_activity(
                    scrape_activity,
                    args=[site],
                    start_to_close_timeout=activity_timeout,
                )
                # Tag scrape payload with workflow name for downstream storage
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
                        try:
                            await workflow.start_child_workflow(
                                "RecoverMissingFirecrawlWebhook",
                                recovery_payload,
                                id=f"wf-firecrawl-recovery-{job_id}",
                                task_queue=workflow.info().task_queue,
                            )
                        except Exception as start_err:  # noqa: BLE001
                            await _log(
                                "recovery.start_failed",
                                site_url=site.get("url"),
                                message=str(start_err),
                                level="warn",
                            )

                http_exchange = extract_http_exchange(res)
                if http_exchange:
                    http_exchange.setdefault("siteId", site.get("_id"))
                    await _log(
                        "scrape.http",
                        site_url=site["url"],
                        data=http_exchange,
                    )

                scrape_id = await workflow.execute_activity(
                    store_scrape,
                    args=[res],
                    schedule_to_close_timeout=timedelta(minutes=3),
                    start_to_close_timeout=timedelta(minutes=3),
                    heartbeat_timeout=timedelta(seconds=30),
                )
                scrape_ids.append(scrape_id)

                await _log(
                    "scrape.result",
                    site_url=site["url"],
                    data=summarize_scrape_result(res),
                )

                # Mark site completed so next lease skips it
                await workflow.execute_activity(
                    complete_site,
                    args=[site["_id"]],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception as e:  # noqa: BLE001
                # On failure, record and release the lock for retry after TTL or immediately
                await workflow.execute_activity(
                    fail_site,
                    args=[{"id": site["_id"], "error": str(e)}],
                    start_to_close_timeout=timedelta(seconds=30),
                )
                status = "failed"
                if isinstance(e, ActivityError) and e.cause:
                    failure_reasons.append(f"{site['url']}: {e.cause}")
                elif isinstance(e, ApplicationError):
                    failure_reasons.append(f"{site['url']}: {e}")
                else:
                    failure_reasons.append(f"{site['url']}: {e}")

                await _log(
                    "site.error",
                    site_url=site["url"],
                    message=str(e),
                    level="error",
                )

        return ScrapeSummary(site_count=leased_count, scrape_ids=scrape_ids)
    except Exception as e:  # noqa: BLE001
        status = "failed"
        failure_reasons.append(str(e))
        await _log("workflow.error", message=str(e), level="error")
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
        )


@workflow.defn(name="ScraperFirecrawl")
class FirecrawlScrapeWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        return await _run_scrape_workflow(
            scrape_site_firecrawl,
            "ScraperFirecrawl",
            scrape_provider="firecrawl",
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

        async def _log(event: str, *, level: str = "info", data: dict | None = None):
            try:
                await workflow.execute_activity(
                    record_scratchpad,
                    args=[
                        {
                            "runId": run_info.run_id,
                            "workflowId": run_info.workflow_id,
                            "workflowName": "SpidercloudJobDetails",
                            "event": event,
                            "data": data,
                            "level": level,
                            "createdAt": _workflow_now_ms(),
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=60),
                )
            except Exception:
                pass


        await _log("workflow.start")

        try:
            while True:
                batch = await workflow.execute_activity(
                    lease_scrape_url_batch,
                    args=["spidercloud", SPIDERCLOUD_BATCH_SIZE],
                    schedule_to_close_timeout=timedelta(seconds=20),
                )

                skipped_urls = []
                if isinstance(batch, dict):
                    raw_skipped = batch.get("skippedUrls")
                    if isinstance(raw_skipped, list):
                        skipped_urls = [u for u in raw_skipped if isinstance(u, str)]
                if skipped_urls:
                    await _log(
                        "batch.skipped_urls",
                        data={"count": len(skipped_urls), "sample": skipped_urls[:25]},
                    )

                urls = batch.get("urls") if isinstance(batch, dict) else None
                if not urls:
                    break

                await _log("batch.leased", data={"count": len(urls)})

                try:
                    res = await workflow.execute_activity(
                        process_spidercloud_job_batch,
                        args=[batch],
                        start_to_close_timeout=timedelta(
                            minutes=runtime_config.spidercloud_job_details_timeout_minutes
                        ),
                    )
                    scrapes = res.get("scrapes") if isinstance(res, dict) else []
                    if not isinstance(scrapes, list):
                        scrapes = []

                    # Store each scrape independently so individual URL results are persisted even if others fail.
                    store_futures: list[tuple[str | None, workflow.ActivityFuture[str]]] = []
                    for scrape in scrapes:
                        if not isinstance(scrape, dict):
                            continue
                        scrape.setdefault("workflowId", run_info.workflow_id)
                        scrape.setdefault("runId", run_info.run_id)
                        urls_for_scrape = []
                        sub_urls = scrape.get("subUrls")
                        if isinstance(sub_urls, list) and sub_urls:
                            urls_for_scrape = [u for u in sub_urls if isinstance(u, str)]
                        url_for_scrape = urls_for_scrape[0] if urls_for_scrape else scrape.get("sourceUrl")
                        store_futures.append(
                            (
                                url_for_scrape if isinstance(url_for_scrape, str) else None,
                                workflow.start_activity(
                                    store_scrape,
                                    args=[scrape],
                                    start_to_close_timeout=timedelta(minutes=3),
                                    heartbeat_timeout=timedelta(seconds=30),
                                ),
                            )
                        )

                    succeeded: list[str] = []
                    failed: list[str] = []
                    if store_futures:
                        for url_val, fut in store_futures:
                            try:
                                res_id = await fut
                                if isinstance(res_id, str):
                                    scrape_ids.append(res_id)
                                if url_val:
                                    succeeded.append(url_val)
                            except Exception as activity_exc:  # noqa: BLE001
                                if url_val:
                                    failed.append(url_val)
                                await _log(
                                    "batch.error",
                                    level="error",
                                    data={"error": str(activity_exc), "url": url_val},
                                )

                    # Mark queue statuses per-URL regardless of other outcomes.
                    if succeeded:
                        try:
                            await workflow.execute_activity(
                                complete_scrape_urls,
                                args=[{"urls": succeeded, "status": "completed"}],
                                schedule_to_close_timeout=timedelta(seconds=20),
                            )
                        except Exception:
                            pass
                    if failed:
                        try:
                            await workflow.execute_activity(
                                complete_scrape_urls,
                                args=[{"urls": failed, "status": "failed", "error": "store_scrape_failed"}],
                                schedule_to_close_timeout=timedelta(seconds=20),
                            )
                        except Exception:
                            pass

                    await _log("batch.processed", data={"count": len(scrapes) or len(urls)})
                    site_count += 1
                except Exception as exc:  # noqa: BLE001
                    await _log("batch.error", level="error", data={"error": str(exc)})
                    failure_reasons.append(str(exc))
                    status = "failed"
                    # Release leased URLs so they can be retried
                    try:
                        leased_urls = []
                        if isinstance(batch, dict):
                            raw_urls = batch.get("urls")
                            if isinstance(raw_urls, list):
                                for entry in raw_urls:
                                    if isinstance(entry, dict) and isinstance(entry.get("url"), str):
                                        leased_urls.append(entry["url"])
                        if leased_urls:
                            await workflow.execute_activity(
                                complete_scrape_urls,
                                args=[{"urls": leased_urls, "status": "failed", "error": "batch_failed"}],
                                schedule_to_close_timeout=timedelta(seconds=20),
                            )
                    except Exception:
                        pass
                    continue

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
                            "taskQueue": "scraper-task-queue",
                            "error": "; ".join(failure_reasons) if failure_reasons else None,
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass
