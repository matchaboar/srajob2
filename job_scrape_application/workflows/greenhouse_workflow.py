from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import List

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError


with workflow.unsafe.imports_passed_through():
    from .activities import (
        complete_site,
        fail_site,
        fetch_greenhouse_listing,
        filter_existing_job_urls,
        lease_site,
        record_workflow_run,
        scrape_greenhouse_jobs,
        store_scrape,
    )


@dataclass
class GreenhouseScrapeSummary:
    site_count: int
    scrape_ids: List[str]
    jobs_scraped: int


@workflow.defn(name="GreenhouseScraperWorkflow")
class GreenhouseScraperWorkflow:
    @workflow.run
    async def run(self) -> GreenhouseScrapeSummary:  # type: ignore[override]
        scrape_ids: List[str] = []
        site_urls: List[str] = []
        site_count = 0
        jobs_scraped = 0
        failure_reasons: List[str] = []
        status = "completed"
        started_at = int(workflow.now().timestamp() * 1000)

        try:
            while True:
                site = await workflow.execute_activity(
                    lease_site,
                    "scraper-worker",
                    300,
                    "greenhouse",
                    schedule_to_close_timeout=timedelta(seconds=30),
                )

                if not site:
                    break

                site_count += 1
                site_urls.append(site["url"])

                try:
                    listing = await workflow.execute_activity(
                        fetch_greenhouse_listing,
                        site,
                        start_to_close_timeout=timedelta(minutes=2),
                    )

                    job_urls: List[str] = listing.get("job_urls", []) if isinstance(listing, dict) else []
                    existing = await workflow.execute_activity(
                        filter_existing_job_urls,
                        job_urls,
                        schedule_to_close_timeout=timedelta(seconds=30),
                    )
                    existing_set = set(existing)
                    urls_to_scrape = [u for u in job_urls if u not in existing_set]

                    if urls_to_scrape:
                        scrape_res = await workflow.execute_activity(
                            scrape_greenhouse_jobs,
                            {"urls": urls_to_scrape, "source_url": site["url"]},
                            start_to_close_timeout=timedelta(minutes=10),
                        )
                        scrape_payload = scrape_res.get("scrape") if isinstance(scrape_res, dict) else None
                        jobs_scraped += int(scrape_res.get("jobsScraped") or 0) if isinstance(scrape_res, dict) else 0

                        if scrape_payload:
                            scrape_id = await workflow.execute_activity(
                                store_scrape,
                                scrape_payload,
                                schedule_to_close_timeout=timedelta(seconds=30),
                            )
                            scrape_ids.append(scrape_id)

                    await workflow.execute_activity(
                        complete_site,
                        site["_id"],
                        schedule_to_close_timeout=timedelta(seconds=30),
                    )
                except Exception as e:  # noqa: BLE001
                    await workflow.execute_activity(
                        fail_site,
                        {"id": site["_id"], "error": str(e)},
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                    status = "failed"
                    if isinstance(e, ActivityError) and e.cause:
                        failure_reasons.append(f"{site['url']}: {e.cause}")
                    elif isinstance(e, ApplicationError):
                        failure_reasons.append(f"{site['url']}: {e}")
                    else:
                        failure_reasons.append(f"{site['url']}: {e}")

            return GreenhouseScrapeSummary(site_count=site_count, scrape_ids=scrape_ids, jobs_scraped=jobs_scraped)
        except Exception as e:  # noqa: BLE001
            status = "failed"
            failure_reasons.append(str(e))
            raise
        finally:
            completed_at = int(workflow.now().timestamp() * 1000)
            if not site_urls:
                failure_reasons.append("No Greenhouse sites were leased (siteUrls empty).")

            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    {
                        "runId": workflow.info().run_id,
                        "workflowId": workflow.info().workflow_id,
                        "workflowName": "GreenhouseScraperWorkflow",
                        "status": status,
                        "startedAt": started_at,
                        "completedAt": completed_at,
                        "siteUrls": site_urls,
                        "sitesProcessed": site_count,
                        "jobsScraped": jobs_scraped,
                        "workerId": "scraper-worker",
                        "taskQueue": "scraper-task-queue",
                        "error": "; ".join(failure_reasons) if failure_reasons else None,
                    },
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                # Best effort; avoid failing workflow on logging
                pass
