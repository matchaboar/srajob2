import asyncio
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import httpx
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Interceptor, Worker, WorkflowInboundInterceptor, WorkflowInterceptorClassInput

from ..config import settings
from ..services.convex_client import convex_query
from . import activities
from .scrape_workflow import (
    FirecrawlScrapeWorkflow,
    FetchfoxSpidercloudWorkflow,
    ScrapeWorkflow,
    SpidercloudJobDetailsWorkflow,
    SpidercloudScrapeWorkflow,
)
from .greenhouse_workflow import GreenhouseScraperWorkflow
from .webhook_workflow import (
    ProcessWebhookIngestWorkflow,
    RecoverMissingFirecrawlWebhookWorkflow,
    SiteLeaseWorkflow,
)

WORKFLOW_CLASSES = [
    ScrapeWorkflow,
    FirecrawlScrapeWorkflow,
    FetchfoxSpidercloudWorkflow,
    SpidercloudScrapeWorkflow,
    SpidercloudJobDetailsWorkflow,
    GreenhouseScraperWorkflow,
    SiteLeaseWorkflow,
    ProcessWebhookIngestWorkflow,
    RecoverMissingFirecrawlWebhookWorkflow,
]

ACTIVITY_FUNCTIONS = [
    activities.fetch_sites,
    activities.lease_site,
    activities.scrape_site,
    activities.scrape_site_firecrawl,
    activities.scrape_site_fetchfox,
    activities.crawl_site_fetchfox,
    activities.fetch_greenhouse_listing,
    activities.filter_existing_job_urls,
    activities.scrape_greenhouse_jobs,
    activities.start_firecrawl_webhook_scrape,
    activities.fetch_pending_firecrawl_webhooks,
    activities.get_firecrawl_webhook_status,
    activities.mark_firecrawl_webhook_processed,
    activities.collect_firecrawl_job_result,
    activities.store_scrape,
    activities.complete_site,
    activities.fail_site,
    activities.record_workflow_run,
    activities.record_scratchpad,
    activities.lease_scrape_url_batch,
    activities.process_spidercloud_job_batch,
    activities.complete_scrape_urls,
]


class WorkflowStartLoggingInterceptor(WorkflowInboundInterceptor):
    """Log workflow starts for quick visibility in the worker console."""

    def __init__(self, next: WorkflowInboundInterceptor) -> None:
        super().__init__(next)
        self._logger = logging.getLogger("temporal.worker.workflow")

    async def execute_workflow(self, input: object) -> object:  # noqa: A002
        try:
            info = workflow.info()
            self._logger.info(
                "Workflow run started: type=%s workflow_id=%s run_id=%s task_queue=%s",
                info.workflow_type,
                info.workflow_id,
                info.run_id,
                info.task_queue,
            )
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Workflow start logging failed: %s", exc)
        return await super().execute_workflow(input)


class WorkflowLoggingInterceptor(Interceptor):
    """Provide workflow-level logging hooks."""

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput  # noqa: ARG002
    ) -> Optional[type[WorkflowInboundInterceptor]]:
        return WorkflowStartLoggingInterceptor


def _setup_logging() -> logging.Logger:
    """Configure structured logging to both stdout and a rotating file."""

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "temporal_worker.log"
    scheduling_log_file = log_dir / "scheduling.log"

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handlers = [
        RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler(sys.stdout),
    ]
    scheduling_handler = RotatingFileHandler(scheduling_log_file, maxBytes=2_000_000, backupCount=2)
    scheduling_handler.setLevel(logging.INFO)

    class _SchedulingOnly(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
            """Route only scheduling logs to scheduling.log."""

            return record.name.startswith("temporal.scheduler")

    scheduling_handler.addFilter(_SchedulingOnly())
    handlers.append(scheduling_handler)

    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers, force=True)
    return logging.getLogger("temporal.worker")


async def monitor_loop(client: Client) -> None:
    """Periodically pushes Temporal workflow status to Convex."""
    logger = logging.getLogger("temporal.worker.monitor")

    if not settings.convex_http_url:
        logger.warning("CONVEX_HTTP_URL not set. Monitor disabled.")
        return

    # Generate unique worker ID (hostname + PID)
    import socket
    import os
    hostname = socket.gethostname()
    worker_id = f"{hostname}-{os.getpid()}"

    logger.info("Monitor loop started (worker_id=%s host=%s)", worker_id, hostname)

    while True:
        try:
            workflows = []
            # List running workflows
            async for wf in client.list_workflows('ExecutionStatus="Running"'):
                start_time = getattr(wf, "start_time", None)
                workflows.append({
                    "id": wf.id,
                    "type": getattr(wf, "type", getattr(wf, "workflow_type", "unknown")),
                    "status": "Running",
                    "startTime": start_time.isoformat() if start_time else "",
                })
            
            # Determine reason if no workflows
            no_workflows_reason = None
            if len(workflows) == 0:
                no_workflows_reason = "No workflows scheduled - waiting for work"
            
            # Build payload with worker identification
            payload = {
                "workerId": worker_id,
                "hostname": hostname,
                "temporalAddress": settings.temporal_address,
                "temporalNamespace": settings.temporal_namespace,
                "taskQueue": settings.task_queue,
                "workflows": workflows,
            }
            
            if no_workflows_reason:
                payload["noWorkflowsReason"] = no_workflows_reason
            
            # Push to Convex
            url = settings.convex_http_url.rstrip("/") + "/api/temporal/status"
            async with httpx.AsyncClient() as http:
                resp = await http.post(url, json=payload)
                if resp.status_code != 200:
                    logger.error("Monitor post failed status=%s body=%s", resp.status_code, resp.text)
                else:
                    logger.info("Monitor heartbeat sent (workflows=%d)", len(workflows))
        except Exception as e:
            logger.exception("Monitor loop error: %s", e)

        await asyncio.sleep(30)  # Update every 30 seconds


async def webhook_wait_logger() -> None:
    """Emit a heartbeat of pending Firecrawl webhook jobs for visibility."""

    logger = logging.getLogger("temporal.worker.webhooks")
    if not (settings.convex_http_url or settings.convex_url):
        logger.info("Convex URL not set. Webhook wait logger disabled.")
        return

    try:
        while True:
            try:
                pending = await convex_query("router:listPendingFirecrawlWebhooks", {"limit": 15})
                summary_parts = []
                if isinstance(pending, list):
                    for event in pending:
                        metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
                        job_id = str(event.get("jobId") or metadata.get("jobId") or event.get("id") or "")
                        site_url = event.get("siteUrl") or metadata.get("siteUrl") or "unknown"
                        status_url = (
                            event.get("statusUrl")
                            or event.get("status_url")
                            or metadata.get("statusUrl")
                            or metadata.get("status_url")
                        )
                        summary_parts.append(
                            f"{job_id or 'unknown'} waiting for webhook url={site_url} status={status_url or 'unknown'}"
                        )

                if summary_parts:
                    logger.info(
                        "Waiting for Firecrawl webhooks (%d): %s",
                        len(summary_parts),
                        "; ".join(summary_parts),
                    )
                else:
                    logger.info("Waiting for Firecrawl webhooks: none pending")
            except Exception as exc:  # noqa: BLE001
                logger.exception("Pending webhook check failed: %s", exc)

            await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.info("Webhook wait logger stopped.")


async def main() -> None:
    logger = _setup_logging()
    logger.info("Worker main() started.")
    logger.info("Settings: Temporal=%s, Convex=%s", settings.temporal_address, settings.convex_http_url)
    logger.info("Connecting to Temporal at %s...", settings.temporal_address)
    try:
        client = await asyncio.wait_for(
            Client.connect(
                settings.temporal_address,
                namespace=settings.temporal_namespace,
            ),
            timeout=10.0
        )
    except asyncio.TimeoutError:
        logger.error("Timed out connecting to Temporal at %s after 10 seconds.", settings.temporal_address)
        logger.error("Ensure the Temporal server is running and accessible.")
        return
    except Exception as e:
        logger.exception("Error connecting to Temporal: %s", e)
        return

    logger.info("Connected to Temporal!")

    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=WORKFLOW_CLASSES,
        activities=ACTIVITY_FUNCTIONS,
        interceptors=[WorkflowLoggingInterceptor()],
    )

    # Start the monitor loop in the background
    monitor_task = asyncio.create_task(monitor_loop(client))
    webhook_log_task = asyncio.create_task(webhook_wait_logger())

    logger.info(
        "Worker started. Namespace=%s Address=%s TaskQueue=%s",
        settings.temporal_namespace,
        settings.temporal_address,
        settings.task_queue,
    )
    try:
        await worker.run()
    except asyncio.CancelledError:
        logger.info("Worker cancelled; shutting down...")
        return
    except KeyboardInterrupt:
        logger.info("Worker interrupted; shutting down...")
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        webhook_log_task.cancel()
        try:
            await webhook_log_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("temporal.worker").info("Exiting on CTRL+C")
    except asyncio.CancelledError:
        logging.getLogger("temporal.worker").info("Cancelled on shutdown")
