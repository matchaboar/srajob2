from __future__ import annotations

import asyncio
import time

import yaml
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from ..config import resolve_config_path, settings

SCHEDULES_YAML = resolve_config_path("schedules.yaml")


def _load_ids_from_yaml() -> list[tuple[str, str]]:
    data = yaml.safe_load(SCHEDULES_YAML.read_text()) if SCHEDULES_YAML.exists() else {}
    items = data.get("schedules", []) if isinstance(data, dict) else []
    firecrawl_workflows = {"SiteLease", "ProcessWebhookScrape", "RecoverMissingFirecrawlWebhook", "ScraperFirecrawl"}
    fetchfox_workflows = {"FetchfoxSpidercloud", "ScrapeWorkflow"}
    out: list[tuple[str, str]] = []
    for item in items:
        if isinstance(item, dict) and "id" in item and "workflow" in item:
            workflow_name = str(item["workflow"])
            if workflow_name in firecrawl_workflows and not settings.enable_firecrawl:
                continue
            if workflow_name in fetchfox_workflows and not settings.enable_fetchfox:
                continue
            out.append((str(item["id"]), workflow_name))
    return out


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )
    triggered_ids: set[str] = set()

    async def trigger_or_start(schedule_id: str, workflow_name: str) -> None:
        handle = client.get_schedule_handle(schedule_id)
        try:
            await handle.trigger()
            print(f"Triggered {schedule_id} once.")
            triggered_ids.add(schedule_id)
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

    for schedule_id, workflow_name in _load_ids_from_yaml():
        if schedule_id in triggered_ids:
            continue
        await trigger_or_start(schedule_id, workflow_name)


if __name__ == "__main__":
    asyncio.run(main())
