from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

# Import activity call prototypes inside workflow via type hints / names
with workflow.unsafe.imports_passed_through():
    from .activities import (
        lease_site,
        scrape_site,
        scrape_site_firecrawl,
        store_scrape,
        complete_site,
        fail_site,
        record_workflow_run,
        record_scratchpad,
    )

from .scratchpad_utils import extract_http_exchange


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
    summary: Dict[str, Any] = {
        "provider": res.get("provider") or items.get("provider") if isinstance(items, dict) else None,
        "queued": items.get("queued") if isinstance(items, dict) else None,
        "jobId": items.get("jobId") if isinstance(items, dict) else None,
        "statusUrl": items.get("statusUrl") if isinstance(items, dict) else None,
        "jobs": jobs,
    }
    if res.get("workflowName"):
        summary["workflowName"] = res.get("workflowName")
    if res.get("costMilliCents"):
        summary["costMilliCents"] = res.get("costMilliCents")
    return {k: v for k, v in summary.items() if v is not None}


async def _run_scrape_workflow(
    scrape_activity,
    workflow_name: str,
    *,
    scrape_provider: str | None = "fetchfox",
) -> ScrapeSummary:
    scrape_ids: List[str] = []
    leased_count = 0
    site_urls: List[str] = []
    started_at = int(workflow.now().timestamp() * 1000)
    status = "completed"
    failure_reasons: List[str] = []
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
                        "workflowName": workflow_name,
                        "siteUrl": site_url,
                        "event": event,
                        "message": message,
                        "data": data,
                        "level": level,
                        "createdAt": int(workflow.now().timestamp() * 1000),
                    }
                ],
                schedule_to_close_timeout=timedelta(seconds=20),
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
                    start_to_close_timeout=timedelta(minutes=10),
                )
                # Tag scrape payload with workflow name for downstream storage
                if isinstance(res, dict):
                    res_dict: Dict[str, Any] = res
                    res_dict.setdefault("workflowName", workflow_name)
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
                    schedule_to_close_timeout=timedelta(seconds=30),
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
        completed_at = int(workflow.now().timestamp() * 1000)
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
