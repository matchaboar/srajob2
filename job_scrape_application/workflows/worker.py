import asyncio
import httpx
from temporalio.client import Client
from temporalio.worker import Worker

from .config import settings
from . import activities
from .scrape_workflow import FirecrawlScrapeWorkflow, ScrapeWorkflow
from .greenhouse_workflow import GreenhouseScraperWorkflow

print("Loading worker module...")


async def monitor_loop(client: Client) -> None:
    """Periodically pushes Temporal workflow status to Convex."""
    if not settings.convex_http_url:
        print("Warning: CONVEX_HTTP_URL not set. Monitor disabled.")
        return

    # Generate unique worker ID (hostname + PID)
    import socket
    import os
    hostname = socket.gethostname()
    worker_id = f"{hostname}-{os.getpid()}"
    
    print(f"Monitor: Worker ID = {worker_id}, Hostname = {hostname}")
    print("Monitor loop started.")
    
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
                    print(f"Monitor Error: HTTP {resp.status_code} - {resp.text}")
                else:
                    print(f"✓ Monitor: {len(workflows)} workflows")
        except Exception as e:
            print(f"✗ Monitor Error: {e}")
        
        await asyncio.sleep(30)  # Update every 30 seconds


async def main() -> None:
    print("Worker main() started.")
    print(f"Settings: Temporal={settings.temporal_address}, Convex={settings.convex_http_url}")
    print(f"Connecting to Temporal at {settings.temporal_address}...")
    try:
        client = await asyncio.wait_for(
            Client.connect(
                settings.temporal_address,
                namespace=settings.temporal_namespace,
            ),
            timeout=10.0
        )
    except asyncio.TimeoutError:
        print(f"Error: Timed out connecting to Temporal at {settings.temporal_address} after 10 seconds.")
        print("Ensure the Temporal server is running and accessible.")
        return
    except Exception as e:
        print(f"Error connecting to Temporal: {e}")
        return

    print("Connected to Temporal!")

    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[ScrapeWorkflow, FirecrawlScrapeWorkflow, GreenhouseScraperWorkflow],
        activities=[
            activities.fetch_sites,
            activities.lease_site,
            activities.scrape_site,
            activities.scrape_site_firecrawl,
            activities.scrape_site_fetchfox,
            activities.fetch_greenhouse_listing,
            activities.filter_existing_job_urls,
            activities.scrape_greenhouse_jobs,
            activities.store_scrape,
            activities.complete_site,
            activities.fail_site,
            activities.record_workflow_run,
        ],
    )

    # Start the monitor loop in the background
    monitor_task = asyncio.create_task(monitor_loop(client))

    print(
        f"Worker started. Namespace={settings.temporal_namespace} "
        f"Address={settings.temporal_address} TaskQueue={settings.task_queue}"
    )
    try:
        await worker.run()
    except asyncio.CancelledError:
        print("Worker cancelled; shutting down...")
        return
    except KeyboardInterrupt:
        print("Worker interrupted; shutting down...")
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting on CTRL+C")
    except asyncio.CancelledError:
        print("Cancelled on shutdown")
