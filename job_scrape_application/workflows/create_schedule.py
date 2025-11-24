from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleUpdate,
    ScheduleAlreadyRunningError,
    ScheduleSpec,
)
from .config import settings


SCHEDULE_ID = "scrape-every-15-mins"


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    spec = ScheduleSpec(
        intervals=[
            ScheduleIntervalSpec(every=timedelta(minutes=15)),
        ]
    )

    action = ScheduleActionStartWorkflow(
        "ScrapeWorkflow",
        id=f"wf-{SCHEDULE_ID}",
        task_queue=settings.task_queue,
    )

    policy = SchedulePolicy(
        # If a run is missed (e.g., worker down), buffer at most 1 and run once
        catchup_window=timedelta(hours=12),
        overlap=ScheduleOverlapPolicy.SKIP,
    )
    schedule = Schedule(action=action, spec=spec, policy=policy)

    # Create or update the schedule idempotently
    handle = client.get_schedule_handle(SCHEDULE_ID)
    try:
        await handle.describe()
        await handle.update(lambda _: ScheduleUpdate(schedule=schedule))
        print(f"Updated schedule: {SCHEDULE_ID}")
    except Exception:
        handle = await client.create_schedule(
            id=SCHEDULE_ID,
            schedule=schedule,
            trigger_immediately=True,
        )
        print(f"Created schedule: {SCHEDULE_ID}")
        print("Triggered schedule immediately for first run.")
    except ScheduleAlreadyRunningError:
        # Another instance created it between describe and create; treat as success
        print(f"Schedule already running: {SCHEDULE_ID}")


if __name__ == "__main__":
    asyncio.run(main())
