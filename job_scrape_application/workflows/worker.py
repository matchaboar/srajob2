import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional
import os

import httpx
from temporalio import workflow
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import (
    Interceptor,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    Worker,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)

from ..config import settings
from ..services import telemetry
from ..services.convex_client import convex_query
from . import activities
from .deadlock_logging import install_deadlock_posthog_handler, record_run_metadata, update_run_metadata
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
from .schedule_audit import schedule_audit_logger

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

_WORKER_CONTEXT: dict[str, object] = {}


def _set_worker_context(context: dict[str, object]) -> None:
    _WORKER_CONTEXT.clear()
    _WORKER_CONTEXT.update(context)

ACTIVITY_FUNCTIONS = [
    activities.fetch_sites,
    activities.lease_site,
    activities.scrape_site,
    activities.scrape_site_firecrawl,
    activities.scrape_site_fetchfox,
    activities.crawl_site_fetchfox,
    activities.fetch_greenhouse_listing,
    activities.filter_existing_job_urls,
    activities.compute_urls_to_scrape,
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
    activities.lease_scrape_url_batch,
    activities.process_spidercloud_job_batch,
    activities.complete_scrape_urls,
]

FIRECRAWL_WORKFLOWS = {
    FirecrawlScrapeWorkflow,
    SiteLeaseWorkflow,
    ProcessWebhookIngestWorkflow,
    RecoverMissingFirecrawlWebhookWorkflow,
}
FETCHFOX_WORKFLOWS = {ScrapeWorkflow, FetchfoxSpidercloudWorkflow}
FIRECRAWL_ACTIVITIES = {
    activities.scrape_site_firecrawl,
    activities.start_firecrawl_webhook_scrape,
    activities.fetch_pending_firecrawl_webhooks,
    activities.get_firecrawl_webhook_status,
    activities.mark_firecrawl_webhook_processed,
    activities.collect_firecrawl_job_result,
}
FETCHFOX_ACTIVITIES = {
    activities.scrape_site_fetchfox,
    activities.crawl_site_fetchfox,
}

JOB_DETAILS_WORKFLOWS = [SpidercloudJobDetailsWorkflow]
JOB_DETAILS_ACTIVITIES = [
    activities.record_workflow_run,
    activities.lease_scrape_url_batch,
    activities.process_spidercloud_job_batch,
    activities.store_scrape,
    activities.complete_scrape_urls,
]

@dataclass(frozen=True)
class WorkerConfig:
    task_queue: str
    workflows: list[type]
    activities: list
    role: str


@dataclass
class WorkflowTaskDebugState:
    last_activity: str | None = None
    last_activity_id: str | None = None
    last_activity_queue: str | None = None
    last_activity_at: str | None = None
    last_child_workflow: str | None = None
    last_child_workflow_id: str | None = None
    last_child_workflow_queue: str | None = None
    last_child_workflow_at: str | None = None
    task_activity_count: int = 0
    task_child_count: int = 0
    task_last_activity: str | None = None
    task_last_activity_id: str | None = None
    task_last_activity_queue: str | None = None
    task_last_activity_at: str | None = None
    task_last_child_workflow: str | None = None
    task_last_child_workflow_id: str | None = None
    task_last_child_workflow_queue: str | None = None
    task_last_child_workflow_at: str | None = None

    def reset_for_task(self) -> None:
        self.task_activity_count = 0
        self.task_child_count = 0
        self.task_last_activity = None
        self.task_last_activity_id = None
        self.task_last_activity_queue = None
        self.task_last_activity_at = None
        self.task_last_child_workflow = None
        self.task_last_child_workflow_id = None
        self.task_last_child_workflow_queue = None
        self.task_last_child_workflow_at = None


class WorkflowTaskDebugOutboundInterceptor(WorkflowOutboundInterceptor):
    def __init__(self, next: WorkflowOutboundInterceptor, state: WorkflowTaskDebugState) -> None:
        super().__init__(next)
        self._state = state

    def start_activity(self, input: StartActivityInput):  # type: ignore[override]
        now = workflow.now().isoformat()
        self._state.task_activity_count += 1
        self._state.task_last_activity = input.activity
        self._state.task_last_activity_id = input.activity_id
        self._state.task_last_activity_queue = input.task_queue
        self._state.task_last_activity_at = now
        self._state.last_activity = input.activity
        self._state.last_activity_id = input.activity_id
        self._state.last_activity_queue = input.task_queue
        self._state.last_activity_at = now
        try:
            update_run_metadata(
                workflow.info().run_id,
                lastActivity=input.activity,
                lastActivityId=input.activity_id,
                lastActivityQueue=input.task_queue,
                lastActivityAt=now,
            )
        except Exception:
            pass
        return self.next.start_activity(input)

    def start_local_activity(self, input: StartLocalActivityInput):  # type: ignore[override]
        now = workflow.now().isoformat()
        self._state.task_activity_count += 1
        self._state.task_last_activity = input.activity
        self._state.task_last_activity_id = input.activity_id
        self._state.task_last_activity_queue = "local"
        self._state.task_last_activity_at = now
        self._state.last_activity = input.activity
        self._state.last_activity_id = input.activity_id
        self._state.last_activity_queue = "local"
        self._state.last_activity_at = now
        try:
            update_run_metadata(
                workflow.info().run_id,
                lastActivity=input.activity,
                lastActivityId=input.activity_id,
                lastActivityQueue="local",
                lastActivityAt=now,
            )
        except Exception:
            pass
        return self.next.start_local_activity(input)

    async def start_child_workflow(self, input: StartChildWorkflowInput):  # type: ignore[override]
        now = workflow.now().isoformat()
        self._state.task_child_count += 1
        self._state.task_last_child_workflow = input.workflow
        self._state.task_last_child_workflow_id = input.id
        self._state.task_last_child_workflow_queue = input.task_queue
        self._state.task_last_child_workflow_at = now
        self._state.last_child_workflow = input.workflow
        self._state.last_child_workflow_id = input.id
        self._state.last_child_workflow_queue = input.task_queue
        self._state.last_child_workflow_at = now
        try:
            update_run_metadata(
                workflow.info().run_id,
                lastChildWorkflow=input.workflow,
                lastChildWorkflowId=input.id,
                lastChildWorkflowQueue=input.task_queue,
                lastChildWorkflowAt=now,
            )
        except Exception:
            pass
        return await self.next.start_child_workflow(input)


def _select_worker_config() -> tuple[str, list[type], list]:
    role = (settings.worker_role or "all").strip().lower()
    if role in {"job-details", "spidercloud-job-details"}:
        queue = settings.job_details_task_queue or settings.task_queue
        return queue, JOB_DETAILS_WORKFLOWS, JOB_DETAILS_ACTIVITIES
    workflows = list(WORKFLOW_CLASSES)
    activities_list = list(ACTIVITY_FUNCTIONS)
    if not settings.enable_firecrawl:
        workflows = [wf for wf in workflows if wf not in FIRECRAWL_WORKFLOWS]
        activities_list = [act for act in activities_list if act not in FIRECRAWL_ACTIVITIES]
    if not settings.enable_fetchfox:
        workflows = [wf for wf in workflows if wf not in FETCHFOX_WORKFLOWS]
        activities_list = [act for act in activities_list if act not in FETCHFOX_ACTIVITIES]
    return settings.task_queue, workflows, activities_list


def _select_worker_configs() -> list[WorkerConfig]:
    role = (settings.worker_role or "all").strip().lower()
    if role in {"job-details", "spidercloud-job-details"}:
        queue = settings.job_details_task_queue or settings.task_queue
        return [WorkerConfig(queue, JOB_DETAILS_WORKFLOWS, JOB_DETAILS_ACTIVITIES, role)]

    task_queue, workflows, activities_list = _select_worker_config()
    configs = [WorkerConfig(task_queue, workflows, activities_list, role)]
    job_details_queue = settings.job_details_task_queue
    if role == "all" and job_details_queue and job_details_queue != task_queue:
        configs.append(
            WorkerConfig(job_details_queue, JOB_DETAILS_WORKFLOWS, JOB_DETAILS_ACTIVITIES, "job-details")
        )
    return configs


class WorkflowStartLoggingInterceptor(WorkflowInboundInterceptor):
    """Log workflow starts for quick visibility in the worker console."""

    def __init__(self, next: WorkflowInboundInterceptor) -> None:
        super().__init__(next)
        self._logger = logging.getLogger("temporal.worker.workflow")
        self._debug_state = WorkflowTaskDebugState()

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        wrapped = WorkflowTaskDebugOutboundInterceptor(outbound, self._debug_state)
        self.next.init(wrapped)

    async def execute_workflow(self, input: object) -> object:  # noqa: A002
        self._debug_state.reset_for_task()
        error: BaseException | None = None
        info = None
        start_time: float | None = None
        try:
            info = workflow.info()
            self._logger.info(
                "Workflow run started: type=%s workflow_id=%s run_id=%s task_queue=%s",
                info.workflow_type,
                info.workflow_id,
                info.run_id,
                info.task_queue,
            )
            record_run_metadata(
                info.run_id,
                info.workflow_id,
                info.workflow_type,
                info.task_queue,
            )
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Workflow start logging failed: %s", exc)

        with workflow.unsafe.sandbox_unrestricted():
            start_time = time.perf_counter()

        try:
            return await super().execute_workflow(input)
        except BaseException as exc:
            error = exc
            raise
        finally:
            if start_time is not None:
                with workflow.unsafe.sandbox_unrestricted():
                    elapsed_ms = int((time.perf_counter() - start_time) * 1000)

                threshold_ms = max(0, settings.workflow_task_debug_log_ms)
                should_log = (
                    error is not None
                    or settings.workflow_task_debug_log_all
                    or (threshold_ms > 0 and elapsed_ms >= threshold_ms)
                )
                if should_log:
                    if info is None:
                        try:
                            info = workflow.info()
                        except Exception:
                            info = None
                    payload = {
                    "event": "workflow.run.error" if error else "workflow.run.slow",
                        "runId": getattr(info, "run_id", None),
                        "workflowId": getattr(info, "workflow_id", None),
                        "workflowType": getattr(info, "workflow_type", None),
                        "taskQueue": getattr(info, "task_queue", None),
                        "attempt": getattr(info, "attempt", None),
                        "elapsedMs": elapsed_ms,
                    "runActivityCount": self._debug_state.task_activity_count,
                    "runChildWorkflowCount": self._debug_state.task_child_count,
                    "runLastActivity": self._debug_state.task_last_activity,
                    "runLastActivityId": self._debug_state.task_last_activity_id,
                    "runLastActivityQueue": self._debug_state.task_last_activity_queue,
                    "runLastActivityAt": self._debug_state.task_last_activity_at,
                    "runLastChildWorkflow": self._debug_state.task_last_child_workflow,
                    "runLastChildWorkflowId": self._debug_state.task_last_child_workflow_id,
                    "runLastChildWorkflowQueue": self._debug_state.task_last_child_workflow_queue,
                    "runLastChildWorkflowAt": self._debug_state.task_last_child_workflow_at,
                    "lastActivity": self._debug_state.last_activity,
                    "lastActivityId": self._debug_state.last_activity_id,
                    "lastActivityQueue": self._debug_state.last_activity_queue,
                    "lastActivityAt": self._debug_state.last_activity_at,
                    "lastChildWorkflow": self._debug_state.last_child_workflow,
                    "lastChildWorkflowId": self._debug_state.last_child_workflow_id,
                    "lastChildWorkflowQueue": self._debug_state.last_child_workflow_queue,
                    "lastChildWorkflowAt": self._debug_state.last_child_workflow_at,
                        "isReplay": workflow.unsafe.is_replaying(),
                        "workerId": _WORKER_CONTEXT.get("workerId"),
                        "hostname": _WORKER_CONTEXT.get("hostname"),
                        "workerRole": _WORKER_CONTEXT.get("workerRole"),
                        "taskQueues": _WORKER_CONTEXT.get("taskQueues"),
                    }
                    if error is not None:
                        payload["errorType"] = type(error).__name__
                        payload["errorMessage"] = str(error)
                    self._logger.warning(
                        payload["event"],
                        extra={k: v for k, v in payload.items() if v is not None},
                    )


class WorkflowLoggingInterceptor(Interceptor):
    """Provide workflow-level logging hooks."""

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput  # noqa: ARG002
    ) -> Optional[type[WorkflowInboundInterceptor]]:
        return WorkflowStartLoggingInterceptor


def _setup_logging() -> logging.Logger:
    """Configure structured logging to PostHog and rotating files."""

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "temporal_worker.log"
    scheduling_log_file = log_dir / "scheduling.log"

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handlers = [RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)]
    scheduling_handler = RotatingFileHandler(scheduling_log_file, maxBytes=2_000_000, backupCount=2)
    scheduling_handler.setLevel(logging.INFO)

    class _SchedulingOnly(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
            """Route only scheduling logs to scheduling.log."""

            return record.name.startswith("temporal.scheduler")

    scheduling_handler.addFilter(_SchedulingOnly())
    handlers.append(scheduling_handler)
    posthog_handler = telemetry.build_posthog_log_handler(level=logging.INFO)
    if posthog_handler is not None:
        handlers.append(posthog_handler)

    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers, force=True)
    # HTTPX logs every heartbeat request at INFO; keep them quiet unless debugging.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("temporal.worker")


async def monitor_loop(client: Client, worker_id: str) -> None:
    """Periodically pushes Temporal workflow status to Convex."""
    logger = logging.getLogger("temporal.worker.monitor")

    if not settings.convex_http_url:
        logger.warning("CONVEX_HTTP_URL not set. Monitor disabled.")
        return

    import socket
    hostname = socket.gethostname()

    logger.info("Monitor loop started (worker_id=%s host=%s)", worker_id, hostname)

    last_log_time = 0.0
    last_workflow_count: int | None = None
    last_no_workflows_reason: str | None = None
    monitor_timeout: timedelta | None = None
    if settings.monitor_rpc_timeout_seconds > 0:
        monitor_timeout = timedelta(seconds=settings.monitor_rpc_timeout_seconds)

    while True:
        try:
            workflows = []
            list_failed = False
            # List running workflows
            try:
                async for wf in client.list_workflows(
                    'ExecutionStatus="Running"',
                    rpc_timeout=monitor_timeout,
                ):
                    start_time = getattr(wf, "start_time", None)
                    workflows.append({
                        "id": wf.id,
                        "type": getattr(wf, "type", getattr(wf, "workflow_type", "unknown")),
                        "status": "Running",
                        "startTime": start_time.isoformat() if start_time else "",
                    })
            except RPCError as exc:
                if exc.status in {
                    RPCStatusCode.CANCELLED,
                    RPCStatusCode.DEADLINE_EXCEEDED,
                    RPCStatusCode.UNAVAILABLE,
                }:
                    logger.warning("Monitor workflow listing timed out: %s", exc.message)
                    list_failed = True
                else:
                    raise

            if not list_failed:
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
                        now_monotonic = time.monotonic()
                        should_log = False
                        if last_workflow_count is None or last_workflow_count != len(workflows):
                            should_log = True
                        if no_workflows_reason != last_no_workflows_reason:
                            should_log = True
                        if now_monotonic - last_log_time >= 300:
                            should_log = True
                        if should_log:
                            logger.info("Monitor heartbeat sent (workflows=%d)", len(workflows))
                            last_log_time = now_monotonic
                            last_workflow_count = len(workflows)
                            last_no_workflows_reason = no_workflows_reason
        except Exception as e:
            logger.exception("Monitor loop error: %s", e)

        await asyncio.sleep(30)  # Update every 30 seconds


async def webhook_wait_logger() -> None:
    """Emit a heartbeat of pending Firecrawl webhook jobs for visibility."""

    logger = logging.getLogger("temporal.worker.webhooks")
    if not (settings.convex_http_url or settings.convex_url):
        logger.info("Convex URL not set. Webhook wait logger disabled.")
        return
    interval = int(getattr(settings, "webhook_wait_logger_interval_seconds", 60))
    if interval <= 0:
        logger.info("Webhook wait logger disabled (interval=%s).", interval)
        return

    last_log_time = 0.0
    last_count: int | None = None
    try:
        while True:
            try:
                pending = await convex_query("router:listPendingFirecrawlWebhooks", {"limit": 10})
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

                count = len(summary_parts)
                now_monotonic = time.monotonic()
                should_log = False
                if last_count is None or last_count != count:
                    should_log = True
                if now_monotonic - last_log_time >= 300:
                    should_log = True

                if should_log:
                    if summary_parts:
                        logger.info(
                            "Waiting for Firecrawl webhooks (%d): %s",
                            count,
                            "; ".join(summary_parts),
                        )
                    else:
                        logger.info("Waiting for Firecrawl webhooks: none pending")
                    last_log_time = now_monotonic
                    last_count = count
            except Exception as exc:  # noqa: BLE001
                logger.exception("Pending webhook check failed: %s", exc)

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("Webhook wait logger stopped.")


async def main() -> None:
    logger = _setup_logging()
    logger.info("Worker main() started.")
    logger.info("Settings: Temporal=%s, Convex=%s", settings.temporal_address, settings.convex_http_url)
    if telemetry.initialize_posthog_exception_tracking():
        logger.info("PostHog exception autocapture enabled.")
    elif settings.posthog_project_api_key:
        logger.warning("PostHog exception autocapture disabled or failed to initialize.")
    logger.info("Connecting to Temporal at %s...", settings.temporal_address)
    os.environ.setdefault("TEMPORAL_MAX_INCOMING_GRPC_BYTES", str(10 * 1024 * 1024))
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

    # Generate unique worker ID (hostname + PID) shared across monitors
    import socket

    hostname = socket.gethostname()
    worker_id = f"{hostname}-{os.getpid()}"

    configs = _select_worker_configs()
    queue_summary = ", ".join(f"{cfg.task_queue}({cfg.role})" for cfg in configs)
    worker_context = {
        "workerId": worker_id,
        "hostname": hostname,
        "temporalAddress": settings.temporal_address,
        "temporalNamespace": settings.temporal_namespace,
        "taskQueues": queue_summary,
        "workerRole": settings.worker_role or "all",
    }
    _set_worker_context(worker_context)
    install_deadlock_posthog_handler(worker_context)
    workers = [
        Worker(
            client,
            task_queue=cfg.task_queue,
            workflows=cfg.workflows,
            activities=cfg.activities,
            interceptors=[WorkflowLoggingInterceptor()],
        )
        for cfg in configs
    ]

    # Start the monitor loop in the background
    monitor_task = asyncio.create_task(monitor_loop(client, worker_id))
    webhook_log_task = asyncio.create_task(webhook_wait_logger())
    schedule_audit_task = asyncio.create_task(schedule_audit_logger(worker_id))

    logger.info(
        "Worker started. Namespace=%s Address=%s TaskQueues=%s Role=%s",
        settings.temporal_namespace,
        settings.temporal_address,
        queue_summary,
        settings.worker_role or "all",
    )
    try:
        run_tasks = [asyncio.create_task(worker.run()) for worker in workers]
        await asyncio.gather(*run_tasks)
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
        schedule_audit_task.cancel()
        try:
            await schedule_audit_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("temporal.worker").info("Exiting on CTRL+C")
    except asyncio.CancelledError:
        logging.getLogger("temporal.worker").info("Cancelled on shutdown")
