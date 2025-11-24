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


SCHEDULES = (
    ("scrape-every-15-mins", "ScraperFirecrawl"),
    ("greenhouse-scrape-every-15-mins", "GreenhouseScraperWorkflow"),
)


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    for schedule_id, workflow_name in SCHEDULES:
        spec = ScheduleSpec(
            intervals=[
                ScheduleIntervalSpec(every=timedelta(minutes=15)),
            ]
        )

        action = ScheduleActionStartWorkflow(
            workflow_name,
            id=f"wf-{schedule_id}",
            task_queue=settings.task_queue,
        )

        policy = SchedulePolicy(
            # If a run is missed (e.g., worker down), buffer at most 1 and run once
            catchup_window=timedelta(hours=12),
            overlap=ScheduleOverlapPolicy.SKIP,
        )
        schedule = Schedule(action=action, spec=spec, policy=policy)

        handle = client.get_schedule_handle(schedule_id)
        try:
            await handle.describe()
            await handle.update(lambda _: ScheduleUpdate(schedule=schedule))
            print(f"Updated schedule: {schedule_id}")
        except ScheduleAlreadyRunningError:
            print(f"Schedule already running: {schedule_id}")
        except Exception:
            handle = await client.create_schedule(
                id=schedule_id,
                schedule=schedule,
                trigger_immediately=True,
            )
            print(f"Created schedule: {schedule_id}")
            print("Triggered schedule immediately for first run.")


if __name__ == "__main__":
    asyncio.run(main())
