from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

from .scratchpad_utils import extract_http_exchange


with workflow.unsafe.imports_passed_through():
    from .activities import (
        complete_site,
        fail_site,
        fetch_greenhouse_listing,
        filter_existing_job_urls,
        compute_urls_to_scrape,
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
        run_info = workflow.info()
        wf_logger = workflow.logger  # type: ignore[attr-defined]

        async def _log(
            event: str,
            *,
            message: str | None = None,
            data: Dict[str, Any] | None = None,
            site_url: str | None = None,
            level: str = "info",
        ) -> None:
            msg = (
                "GreenhouseScraperWorkflow"
                f" | event={event} | siteUrl={site_url} | message={message} | data={data}"
            )
            if level == "error":
                wf_logger.error(msg)
            elif level in {"warn", "warning"}:
                wf_logger.warning(msg)
            else:
                wf_logger.info(msg)

        await _log("workflow.start", message="Greenhouse workflow started")

        try:
            while True:
                site = await workflow.execute_activity(
                    lease_site,
                    args=["scraper-worker", 300, "greenhouse"],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )

                if not site:
                    break

                site_count += 1
                site_urls.append(site["url"])
                await _log(
                    "site.leased",
                    site_url=site["url"],
                    data={"siteId": site.get("_id")},
                )

                try:
                    listing = await workflow.execute_activity(
                        fetch_greenhouse_listing,
                        args=[site],
                        start_to_close_timeout=timedelta(minutes=2),
                    )

                    job_urls: List[str] = listing.get("job_urls", []) if isinstance(listing, dict) else []
                    posted_at_by_url = (
                        listing.get("posted_at_by_url")
                        if isinstance(listing, dict) and isinstance(listing.get("posted_at_by_url"), dict)
                        else None
                    )
                    existing = await workflow.execute_activity(
                        filter_existing_job_urls,
                        args=[job_urls],
                        schedule_to_close_timeout=timedelta(seconds=30),
                    )
                    diff = await workflow.execute_activity(
                        compute_urls_to_scrape,
                        args=[job_urls, existing],
                        schedule_to_close_timeout=timedelta(seconds=30),
                    )
                    urls_to_scrape = diff.get("urlsToScrape") if isinstance(diff, dict) else None
                    if not isinstance(urls_to_scrape, list):
                        urls_to_scrape = [u for u in job_urls if isinstance(u, str)]
                    existing_count = diff.get("existingCount") if isinstance(diff, dict) else None
                    if not isinstance(existing_count, int):
                        existing_count = len({u for u in existing if isinstance(u, str)})

                    await _log(
                        "greenhouse.listing",
                        site_url=site["url"],
                        data={
                            "jobUrls": len(job_urls),
                            "existing": existing_count,
                            "toScrape": len(urls_to_scrape),
                        },
                    )

                    if urls_to_scrape:
                        scrape_payload: Dict[str, Any] = {"urls": urls_to_scrape, "source_url": site["url"]}
                        if posted_at_by_url:
                            scrape_payload["posted_at_by_url"] = posted_at_by_url
                        scrape_res = await workflow.execute_activity(
                            scrape_greenhouse_jobs,
                            args=[scrape_payload],
                            start_to_close_timeout=timedelta(minutes=30),
                        )
                        scrape_payload = scrape_res.get("scrape") if isinstance(scrape_res, dict) else None
                        jobs_scraped += int(scrape_res.get("jobsScraped") or 0) if isinstance(scrape_res, dict) else 0

                        http_exchange = extract_http_exchange(scrape_payload) if scrape_payload else None
                        if http_exchange:
                            http_exchange.setdefault("siteId", site.get("_id"))
                            await _log(
                                "scrape.http",
                                site_url=site["url"],
                                data=http_exchange,
                            )

                        if scrape_payload:
                            scrape_payload.setdefault("workflowId", run_info.workflow_id)
                            scrape_payload.setdefault("runId", run_info.run_id)
                            scrape_id = await workflow.execute_activity(
                                store_scrape,
                                args=[scrape_payload],
                                schedule_to_close_timeout=timedelta(minutes=3),
                                start_to_close_timeout=timedelta(minutes=3),
                            )
                            scrape_ids.append(scrape_id)

                        await _log(
                            "greenhouse.scrape",
                            site_url=site["url"],
                            data={
                                "jobsScraped": int(scrape_res.get("jobsScraped") or 0)
                                if isinstance(scrape_res, dict)
                                else 0,
                                "urls": len(urls_to_scrape),
                            },
                        )

                    await workflow.execute_activity(
                        complete_site,
                        args=[site["_id"]],
                        schedule_to_close_timeout=timedelta(seconds=30),
                    )
                except Exception as e:  # noqa: BLE001
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

            return GreenhouseScrapeSummary(site_count=site_count, scrape_ids=scrape_ids, jobs_scraped=jobs_scraped)
        except Exception as e:  # noqa: BLE001
            status = "failed"
            failure_reasons.append(str(e))
            await _log("workflow.error", message=str(e), level="error")
            raise
        finally:
            completed_at = int(workflow.now().timestamp() * 1000)
            if not site_urls:
                failure_reasons.append("No Greenhouse sites were leased (siteUrls empty).")

            try:
                await workflow.execute_activity(
                    record_workflow_run,
                    args=[
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
                        }
                    ],
                    schedule_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                # Best effort; avoid failing workflow on logging
                pass

            await _log(
                "workflow.complete",
                message="Greenhouse workflow finished",
                data={
                    "status": status,
                    "sitesProcessed": site_count,
                    "jobsScraped": jobs_scraped,
                },
                level="warn" if status != "completed" else "info",
            )
