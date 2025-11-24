from __future__ import annotations

from dataclasses import dataclass
from typing import List

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

# Import activity call prototypes inside workflow via type hints / names
with workflow.unsafe.imports_passed_through():
    from .activities import (
        fetch_sites,
        lease_site,
        scrape_site,
        store_scrape,
        complete_site,
        fail_site,
        Site,
        record_workflow_run,
    )


@dataclass
class ScrapeSummary:
    site_count: int
    scrape_ids: List[str]


@workflow.defn(name="ScrapeWorkflow")
class ScrapeWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        scrape_ids: List[str] = []
        leased_count = 0
        site_urls: List[str] = []
        started_at = int(workflow.now().timestamp() * 1000)
        status = "completed"
        failure_reasons: List[str] = []

        try:
            # Keep leasing jobs until none available
            while True:
                site = await workflow.execute_activity(
                    lease_site,
                    "scraper-worker",  # logical worker id; replace if needed
                    schedule_to_close_timeout=workflow.timedelta(seconds=30),
                )

                if not site:
                    break

                leased_count += 1
                site_urls.append(site["url"])

                try:
                    res = await workflow.execute_activity(
                        scrape_site,
                        site,
                        start_to_close_timeout=workflow.timedelta(minutes=10),
                    )
                    scrape_id = await workflow.execute_activity(
                        store_scrape,
                        res,
                        schedule_to_close_timeout=workflow.timedelta(seconds=30),
                    )
                    scrape_ids.append(scrape_id)

                    # Mark site completed so next lease skips it
                    await workflow.execute_activity(
                        complete_site,
                        site["_id"],
                        schedule_to_close_timeout=workflow.timedelta(seconds=30),
                    )
                except Exception as e:  # noqa: BLE001
                    # On failure, record and release the lock for retry after TTL or immediately
                    await workflow.execute_activity(
                        fail_site,
                        {"id": site["_id"], "error": str(e)},
                        start_to_close_timeout=workflow.timedelta(seconds=30),
                    )
                    status = "failed"
                    if isinstance(e, ActivityError) and e.cause:
                        failure_reasons.append(f"{site['url']}: {e.cause}")
                    elif isinstance(e, ApplicationError):
                        failure_reasons.append(f"{site['url']}: {e}")
                    else:
                        failure_reasons.append(f"{site['url']}: {e}")

            return ScrapeSummary(site_count=leased_count, scrape_ids=scrape_ids)
        except Exception as e:  # noqa: BLE001
            status = "failed"
            failure_reasons.append(str(e))
            raise
        finally:
            completed_at = int(workflow.now().timestamp() * 1000)
            if not site_urls:
                failure_reasons.append("No sites were leased (siteUrls empty).")
            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    {
                        "runId": workflow.info().run_id,
                        "workflowId": workflow.info().workflow_id,
                        "workflowName": "ScrapeWorkflow",
                        "status": status,
                        "startedAt": started_at,
                        "completedAt": completed_at,
                        "siteUrls": site_urls,
                        "sitesProcessed": leased_count,
                        "jobsScraped": len(scrape_ids),
                        "workerId": "scraper-worker",
                        "taskQueue": "scraper-task-queue",
                        "error": "; ".join(failure_reasons) if failure_reasons else None,
                    },
                    schedule_to_close_timeout=workflow.timedelta(seconds=30),
                )
            except Exception:
                # Best-effort; do not fail workflow on log write issues
                pass
