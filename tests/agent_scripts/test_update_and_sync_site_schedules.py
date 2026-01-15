from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_module():
    module_path = (
        Path(__file__).resolve().parents[2]
        / "agent_scripts"
        / "config"
        / "update_and_sync_site_schedules.py"
    )
    spec = importlib.util.spec_from_file_location("update_and_sync_site_schedules", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load update_and_sync_site_schedules module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_update_entries_sets_default_start_time_and_name():
    mod = _load_module()
    entries = [{"schedule": {"days": ["mon"], "intervalMinutes": 120}}]

    updated = mod._update_entries(
        entries,
        days=["mon", "tue"],
        interval_minutes=120.0,
        name_template="Weekdays every 2 hours @ {startTime}",
    )

    schedule = updated[0]["schedule"]
    assert schedule["startTime"] == "09:30"
    assert schedule["name"] == "Weekdays every 2 hours @ 09:30"
    assert schedule["days"] == ["mon", "tue"]
    assert schedule["intervalMinutes"] == 120.0
    assert schedule["timezone"] == mod.DEFAULT_TIMEZONE


@pytest.mark.asyncio
async def test_push_to_convex_uses_default_start_time(monkeypatch):
    mod = _load_module()
    entries = [
        {
            "name": "Example",
            "url": "https://example.com",
            "type": "general",
            "scrapeProvider": "fetchfox",
            "enabled": True,
            "schedule": {"days": ["mon"], "intervalMinutes": 120},
        }
    ]

    updated = mod._update_entries(
        entries,
        days=["mon"],
        interval_minutes=120.0,
        name_template="Weekdays every 2 hours @ {startTime}",
    )

    calls: list[tuple[str, dict]] = []

    async def fake_query(name: str, args: dict | None = None):
        if name == "router:listSchedules":
            return []
        raise AssertionError(f"Unexpected query {name}")

    async def fake_mutation(name: str, args: dict | None = None):
        calls.append((name, args or {}))
        if name == "router:upsertSchedule":
            return "sched-1"
        if name == "router:upsertSite":
            return "site-1"
        raise AssertionError(f"Unexpected mutation {name}")

    monkeypatch.setattr(mod, "convex_query", fake_query)
    monkeypatch.setattr(mod, "convex_mutation", fake_mutation)

    await mod._push_to_convex(updated)

    assert calls[0][0] == "router:upsertSchedule"
    assert calls[0][1]["startTime"] == "09:30"
    assert calls[1][0] == "router:upsertSite"
    assert calls[1][1]["scheduleId"] == "sched-1"


@pytest.mark.asyncio
async def test_push_to_convex_includes_pagination_limit(monkeypatch):
    mod = _load_module()
    entries = [
        {
            "name": "LimitTest",
            "url": "https://example.com",
            "type": "general",
            "scrapeProvider": "spidercloud",
            "enabled": True,
            "paginationLimit": 5,
            "schedule": {"days": ["mon"], "intervalMinutes": 15},
        }
    ]

    updated = mod._update_entries(
        entries,
        days=["mon"],
        interval_minutes=15.0,
        name_template="Weekdays every 15 minutes @ {startTime}",
    )

    calls: list[tuple[str, dict]] = []

    async def fake_query(name: str, args: dict | None = None):
        if name == "router:listSchedules":
            return []
        raise AssertionError(f"Unexpected query {name}")

    async def fake_mutation(name: str, args: dict | None = None):
        calls.append((name, args or {}))
        if name == "router:upsertSchedule":
            return "sched-2"
        if name == "router:upsertSite":
            return "site-2"
        raise AssertionError(f"Unexpected mutation {name}")

    monkeypatch.setattr(mod, "convex_query", fake_query)
    monkeypatch.setattr(mod, "convex_mutation", fake_mutation)

    await mod._push_to_convex(updated)

    assert calls[1][0] == "router:upsertSite"
    assert calls[1][1]["paginationLimit"] == 5


@pytest.mark.asyncio
async def test_push_to_convex_skip_sync(monkeypatch):
    mod = _load_module()
    entries = [
        {
            "name": "SkipSync",
            "url": "https://example.com",
            "type": "general",
            "scrapeProvider": "fetchfox",
            "enabled": True,
            "schedule": {"days": ["mon"], "intervalMinutes": 20},
        }
    ]

    updated = mod._update_entries(
        entries,
        days=["mon"],
        interval_minutes=20.0,
        name_template="Weekdays every 20 minutes @ {startTime}",
    )

    calls: list[tuple[str, dict]] = []

    async def fake_query(name: str, args: dict | None = None):
        raise AssertionError("Convex query should not run when skip sync is enabled")

    async def fake_mutation(name: str, args: dict | None = None):
        calls.append((name, args or {}))
        if name == "router:upsertSchedule":
            return "sched-3"
        if name == "router:upsertSite":
            return "site-3"
        raise AssertionError(f"Unexpected mutation {name}")

    monkeypatch.setattr(mod, "convex_query", fake_query)
    monkeypatch.setattr(mod, "convex_mutation", fake_mutation)

    await mod._push_to_convex(updated, skip_sync=True)

    assert calls[0][0] == "router:upsertSchedule"
    assert calls[1][0] == "router:upsertSite"
