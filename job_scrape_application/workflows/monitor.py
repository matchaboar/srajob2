import asyncio
import httpx
import socket
import os
from temporalio.client import Client
from .config import settings

async def main():
    if not settings.convex_http_url:
        print("Error: CONVEX_HTTP_URL not set")
        return

    # Generate unique worker ID (hostname + PID)
    hostname = socket.gethostname()
    worker_id = f"{hostname}-{os.getpid()}"
    
    print(f"Worker ID: {worker_id}")
    print(f"Hostname: {hostname}")
    print(f"Connecting to Temporal at {settings.temporal_address}...")
    
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )
    print("Connected. Starting monitor loop...")

    while True:
        try:
            # List open workflows
            workflows = []
            async for wf in client.list_workflows('ExecutionStatus="Running"'):
                workflows.append({
                    "id": wf.id,
                    "type": wf.type,
                    "status": "Running",
                    "startTime": wf.start_time.isoformat(),
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
                response = await http.post(url, json=payload)
                response.raise_for_status()
            
            print(f"✓ Updated status: {len(workflows)} running workflows")
        except Exception as e:
            print(f"✗ Error updating status: {e}")
        
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
