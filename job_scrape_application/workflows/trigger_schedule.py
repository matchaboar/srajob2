from __future__ import annotations

import asyncio
import time
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from .config import settings


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )
    async def trigger_or_start(schedule_id: str, workflow_name: str) -> None:
        handle = client.get_schedule_handle(schedule_id)
        try:
            await handle.trigger()
            print(f"Triggered {schedule_id} once.")
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                print(f"Schedule {schedule_id} not found; starting one-off {workflow_name} instead.")
                wf = await client.start_workflow(
                    workflow_name,
                    id=f"{workflow_name}-oneshot-{int(time.time())}",
                    task_queue=settings.task_queue,
                )
                print(f"Started one-off workflow id={wf.id} run={wf.run_id}")
            else:
                raise

    await trigger_or_start("scrape-every-15-mins", "ScraperFirecrawl")
    await trigger_or_start("greenhouse-scrape-every-15-mins", "GreenhouseScraperWorkflow")


if __name__ == "__main__":
    asyncio.run(main())
