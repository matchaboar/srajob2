from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List

import yaml

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleUpdate,
)
from temporalio.service import RPCError, RPCStatusCode

from ..config import resolve_config_path, settings


SCHEDULES_YAML = resolve_config_path("schedules.yaml")


@dataclass
class ScheduleConfig:
    id: str
    workflow: str
    interval_seconds: int
    task_queue: str | None = None
    catchup_window_hours: int = 12
    overlap: str = "skip"


def load_schedule_configs(path: Path = SCHEDULES_YAML) -> List[ScheduleConfig]:
    data = yaml.safe_load(path.read_text()) if path.exists() else {}
    items = data.get("schedules", []) if isinstance(data, dict) else []
    configs: List[ScheduleConfig] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        configs.append(
            ScheduleConfig(
                id=str(item["id"]),
                workflow=str(item["workflow"]),
                interval_seconds=int(item.get("interval_seconds", 15)),
                task_queue=item.get("task_queue"),
                catchup_window_hours=int(item.get("catchup_window_hours", 12)),
                overlap=str(item.get("overlap", "skip")).lower(),
            )
        )
    return configs


def _overlap_policy(name: str) -> ScheduleOverlapPolicy:
    name = name.lower()
    if name == "skip":
        return ScheduleOverlapPolicy.SKIP
    if name == "buffer_all":
        return ScheduleOverlapPolicy.BUFFER_ALL
    if name == "cancel_other":
        return ScheduleOverlapPolicy.CANCEL_OTHER
    return ScheduleOverlapPolicy.SKIP


def build_schedule(cfg: ScheduleConfig) -> Schedule:
    spec = ScheduleSpec(
        intervals=[ScheduleIntervalSpec(every=timedelta(seconds=cfg.interval_seconds))]
    )

    task_queue = cfg.task_queue or settings.task_queue
    if cfg.workflow == "SpidercloudJobDetails" and settings.job_details_task_queue:
        task_queue = settings.job_details_task_queue

    action = ScheduleActionStartWorkflow(
        cfg.workflow,
        id=f"wf-{cfg.id}",
        task_queue=task_queue,
    )

    policy = SchedulePolicy(
        catchup_window=timedelta(hours=cfg.catchup_window_hours),
        overlap=_overlap_policy(cfg.overlap),
    )

    return Schedule(action=action, spec=spec, policy=policy)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update Temporal schedules")
    parser.add_argument(
        "--skip-trigger",
        action="store_true",
        help="Do not trigger schedules immediately when they are first created.",
    )
    return parser.parse_args()


async def main(*, skip_trigger: bool = False) -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    configs = load_schedule_configs()
    desired_ids = {cfg.id for cfg in configs}

    # Delete any schedules not present in YAML
    schedule_iter = await client.list_schedules()
    async for entry in schedule_iter:
        existing_id = entry.id
        if existing_id not in desired_ids:
            handle = client.get_schedule_handle(existing_id)
            try:
                await handle.delete()
                print(f"Deleted schedule not in config: {existing_id}")
            except RPCError as e:
                if e.status != RPCStatusCode.NOT_FOUND:
                    raise
            except Exception:
                pass

    # Upsert desired schedules
    for cfg in configs:
        schedule_id = cfg.id
        schedule = build_schedule(cfg)
        handle = client.get_schedule_handle(schedule_id)
        try:
            await handle.describe()
            await handle.update(lambda _: ScheduleUpdate(schedule=schedule))
            print(f"Updated schedule: {schedule_id}")
        except ScheduleAlreadyRunningError:
            print(f"Schedule already running: {schedule_id}")
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                await client.create_schedule(
                    id=schedule_id,
                    schedule=schedule,
                    trigger_immediately=not skip_trigger,
                )
                print(f"Created schedule: {schedule_id}")
                if not skip_trigger:
                    print("Triggered schedule immediately for first run.")
            else:
                raise


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(skip_trigger=bool(args.skip_trigger)))
