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
    handle = client.get_schedule_handle("scrape-every-15-mins")
    try:
        await handle.trigger()
        print("Triggered scrape schedule once.")
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            print("Schedule not found; starting a one-off ScrapeWorkflow instead.")
            wf = await client.start_workflow(
                "ScrapeWorkflow",
                id=f"scrape-oneshot-{int(time.time())}",
                task_queue=settings.task_queue,
            )
            print(f"Started one-off workflow id={wf.id} run={wf.run_id}")
        else:
            raise


if __name__ == "__main__":
    asyncio.run(main())
