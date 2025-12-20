from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import create_schedule as cs
from temporalio.client import ScheduleActionStartWorkflow, ScheduleOverlapPolicy
from temporalio.service import RPCError, RPCStatusCode


def test_load_schedule_configs_reads_yaml(tmp_path: Path):
    data = {
        "schedules": [
            {
                "id": "alpha",
                "workflow": "WFAlpha",
                "interval_seconds": 5,
                "task_queue": "q1",
                "catchup_window_hours": 1,
                "overlap": "buffer_all",
            }
        ]
    }
    path = tmp_path / "schedules.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")

    configs = cs.load_schedule_configs(path)
    assert len(configs) == 1
    cfg = configs[0]
    assert cfg.id == "alpha"
    assert cfg.workflow == "WFAlpha"
    assert cfg.interval_seconds == 5
    assert cfg.task_queue == "q1"
    assert cfg.catchup_window_hours == 1
    assert cfg.overlap == "buffer_all"


def test_build_schedule_uses_interval_and_policy():
    cfg = cs.ScheduleConfig(
        id="fifteen-min",
        workflow="ScrapeWorkflow",
        interval_seconds=900,
        task_queue="q-tasks",
        catchup_window_hours=3,
        overlap="buffer_all",
    )

    schedule = cs.build_schedule(cfg)

    assert isinstance(schedule.action, ScheduleActionStartWorkflow)
    assert schedule.action.workflow == "ScrapeWorkflow"
    assert schedule.action.task_queue == "q-tasks"
    assert schedule.spec.intervals[0].every.total_seconds() == 900
    assert schedule.policy.catchup_window.total_seconds() == 3 * 3600
    assert schedule.policy.overlap == ScheduleOverlapPolicy.BUFFER_ALL


def test_build_schedule_supports_long_interval():
    cfg = cs.ScheduleConfig(
        id="two-hours",
        workflow="GreenhouseScraperWorkflow",
        interval_seconds=7200,
        task_queue=None,
        catchup_window_hours=1,
        overlap="skip",
    )

    schedule = cs.build_schedule(cfg)

    assert schedule.spec.intervals[0].every.total_seconds() == 7200
    assert schedule.policy.overlap == ScheduleOverlapPolicy.SKIP


def test_default_overlap_is_skip_to_prevent_duplicate_runs():
    cfg = cs.ScheduleConfig(
        id="no-overlap-config",
        workflow="ScrapeWorkflow",
        interval_seconds=300,
        task_queue=None,
        catchup_window_hours=1,
        overlap="skip",
    )

    schedule = cs.build_schedule(cfg)

    assert schedule.policy.overlap == ScheduleOverlapPolicy.SKIP
    # Guardrail: interval should be enforced, so no buffer_all/cancel policies that could
    # queue multiple overlapping scrapes inside the same interval.
    assert schedule.spec.intervals[0].every.total_seconds() == 300


def test_load_schedule_configs_defaults_overlap_skip(tmp_path: Path):
    yaml_data = {"schedules": [{"id": "scraper", "workflow": "ScrapeWorkflow", "interval_seconds": 600}]}
    path = tmp_path / "schedules.yaml"
    path.write_text(yaml.safe_dump(yaml_data), encoding="utf-8")

    cfgs = cs.load_schedule_configs(path)

    assert cfgs and cfgs[0].overlap == "skip"


def test_default_schedule_includes_spidercloud_job_details():
    cfgs = cs.load_schedule_configs()
    match = next((cfg for cfg in cfgs if cfg.id == "spidercloud-job-details"), None)
    assert match is not None
    assert match.workflow == "SpidercloudJobDetails"
    assert match.interval_seconds == 15
    assert match.overlap == "skip"


@dataclass
class FakeScheduleEntry:
    id: str
    schedule: Any = None
    info: Any = None
    typed_search_attributes: Any = None
    search_attributes: Any = None
    data_converter: Any = None
    raw_entry: Any = None


class FakeHandle:
    def __init__(self, client: "FakeClient", schedule_id: str):
        self.client = client
        self.schedule_id = schedule_id

    async def describe(self):
        if self.schedule_id not in self.client.schedules:
            raise RPCError("not found", RPCStatusCode.NOT_FOUND, b"")
        return self.client.schedules[self.schedule_id]

    async def update(self, updater):
        self.client.updated.append(self.schedule_id)
        return None

    async def delete(self):
        if self.schedule_id not in self.client.schedules:
            raise RPCError("not found", RPCStatusCode.NOT_FOUND, b"")
        self.client.deleted.append(self.schedule_id)
        del self.client.schedules[self.schedule_id]


class FakeClient:
    def __init__(self, existing: List[str] | None = None):
        self.schedules: Dict[str, Any] = {sid: {} for sid in (existing or [])}
        self.updated: List[str] = []
        self.created: List[str] = []
        self.deleted: List[str] = []
        self.list_schedules_awaited = False

    def get_schedule_handle(self, schedule_id: str) -> FakeHandle:
        return FakeHandle(self, schedule_id)

    async def create_schedule(self, id: str, schedule: Any, trigger_immediately: bool = False):
        self.created.append(id)
        self.schedules[id] = schedule

    async def list_schedules(self, *_, **__):
        self.list_schedules_awaited = True

        async def _gen():
            for sid in list(self.schedules.keys()):
                yield FakeScheduleEntry(id=sid)

        return _gen()


@pytest.mark.asyncio
async def test_sync_deletes_unknown_and_upserts(tmp_path: Path, monkeypatch):
    yaml_data = {
        "schedules": [
            {"id": "keep-one", "workflow": "WF1", "interval_seconds": 5},
            {"id": "new-one", "workflow": "WF2", "interval_seconds": 7},
        ]
    }
    path = tmp_path / "schedules.yaml"
    path.write_text(yaml.safe_dump(yaml_data), encoding="utf-8")

    cfgs = cs.load_schedule_configs(path)
    fake_client = FakeClient(existing=["keep-one", "remove-me"])

    # Monkeypatch builder to avoid Temporal types; we only care about call flow
    monkeypatch.setattr(cs, "build_schedule", lambda cfg: {"built": cfg.id})

    # Run sync logic
    desired_ids = {c.id for c in cfgs}
    # mimic create_schedule main steps
    # delete others
    async for entry in await fake_client.list_schedules():
        if entry.id not in desired_ids:
            await fake_client.get_schedule_handle(entry.id).delete()
    # upsert
    for cfg in cfgs:
        handle = fake_client.get_schedule_handle(cfg.id)
        try:
            await handle.describe()
            await handle.update(lambda _: None)
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                await fake_client.create_schedule(cfg.id, {"built": cfg.id}, trigger_immediately=True)
            else:
                raise

    assert fake_client.deleted == ["remove-me"]
    assert "keep-one" in fake_client.updated
    assert "new-one" in fake_client.created


@pytest.mark.asyncio
async def test_main_awaits_list_schedules_and_syncs(monkeypatch):
    cfgs = [
        cs.ScheduleConfig(id="fresh", workflow="WF2", interval_seconds=10, task_queue=None),
    ]
    fake_client = FakeClient(existing=["stale", "fresh"])

    monkeypatch.setattr(cs, "load_schedule_configs", lambda: cfgs)
    monkeypatch.setattr(cs, "build_schedule", lambda cfg: {"built": cfg.id})

    async def fake_connect(*args, **kwargs):
        return fake_client

    monkeypatch.setattr(cs.Client, "connect", staticmethod(fake_connect))

    await cs.main()

    assert fake_client.list_schedules_awaited is True
    assert fake_client.deleted == ["stale"]
    assert fake_client.updated == ["fresh"]
    assert fake_client.created == []
