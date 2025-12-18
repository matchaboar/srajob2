from __future__ import annotations

from datetime import datetime, timezone
import asyncio
from zoneinfo import ZoneInfo

from job_scrape_application.workflows import schedule_audit


def _ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def test_latest_eligible_time_matches_ts_logic():
    sched = {
        "days": ["mon", "tue", "wed", "thu", "fri"],
        "startTime": "09:30",
        "intervalMinutes": 60,
        "timezone": "America/Denver",
    }
    now = datetime(2025, 1, 6, 10, 15, tzinfo=ZoneInfo("America/Denver"))  # Monday
    eligible = schedule_audit._latest_eligible_time(sched, _ts(now))
    expected = _ts(datetime(2025, 1, 6, 9, 30, tzinfo=ZoneInfo("America/Denver")))
    assert eligible == expected


def test_schedule_decision_due_with_schedule():
    sched = {
        "days": ["mon"],
        "startTime": "09:00",
        "intervalMinutes": 120,
        "timezone": "America/Denver",
    }
    schedule_map = {"s1": sched}
    now = _ts(datetime(2025, 1, 6, 11, 5, tzinfo=ZoneInfo("America/Denver")))
    site = {"_id": "site-1", "url": "https://a", "scheduleId": "s1", "lastRunAt": 0, "type": "general"}

    decision = schedule_audit._schedule_decision_for_site(site, schedule_map, now)

    assert decision["due"] is True
    assert "scheduled slot" in decision["reason"]
    assert decision["eligibleAt"] is not None


def test_schedule_decision_skips_if_already_ran():
    sched = {
        "days": ["mon"],
        "startTime": "09:00",
        "intervalMinutes": 60,
        "timezone": "America/Denver",
    }
    schedule_map = {"s1": sched}
    now = _ts(datetime(2025, 1, 6, 10, 30, tzinfo=ZoneInfo("America/Denver")))
    eligible = schedule_audit._latest_eligible_time(sched, now)
    site = {"_id": "site-1", "url": "https://a", "scheduleId": "s1", "lastRunAt": eligible, "type": "general"}

    decision = schedule_audit._schedule_decision_for_site(site, schedule_map, now)

    assert decision["due"] is False
    assert "already ran" in decision["reason"]


def test_schedule_decision_outside_window():
    sched = {
        "days": ["mon"],
        "startTime": "12:00",
        "intervalMinutes": 60,
        "timezone": "America/Denver",
    }
    schedule_map = {"s1": sched}
    now = _ts(datetime(2025, 1, 6, 9, 0, tzinfo=ZoneInfo("America/Denver")))
    site = {"_id": "site-1", "url": "https://a", "scheduleId": "s1", "lastRunAt": 0, "type": "general"}

    decision = schedule_audit._schedule_decision_for_site(site, schedule_map, now)

    assert decision["due"] is False
    assert "outside schedule window" in decision["reason"]


def test_schedule_decision_manual_trigger_overrides_last_run():
    now = _ts(datetime(2025, 1, 6, 9, 0, tzinfo=timezone.utc))
    site = {
        "_id": "site-manual",
        "url": "https://manual",
        "scheduleId": None,
        "lastRunAt": now - 10 * 60 * 1000,  # 10 minutes ago
        "manualTriggerAt": now - 5 * 60 * 1000,  # 5 minutes ago
    }

    decision = schedule_audit._schedule_decision_for_site(site, {}, now)

    assert decision["due"] is True
    assert "manual trigger" in decision["reason"]


def test_gather_schedule_audit_builds_summary(monkeypatch):
    now = _ts(datetime(2025, 1, 6, 10, 0, tzinfo=ZoneInfo("America/Denver")))

    sites = [
        {
            "_id": "site-1",
            "url": "https://a",
            "scheduleId": "sched-1",
            "lastRunAt": 0,
            "type": "general",
        },
        {
            "_id": "site-2",
            "url": "https://b",
            "scheduleId": "sched-1",
            "lastRunAt": now,
            "type": "general",
        },
    ]
    schedules = [
        {
            "_id": "sched-1",
            "days": ["mon"],
            "startTime": "09:00",
            "intervalMinutes": 60,
            "timezone": "America/Denver",
        }
    ]

    async def fake_convex_query(name: str, args: dict | None = None):
        if name.endswith("listSites"):
            return sites
        if name.endswith("listSchedules"):
            return schedules
        raise AssertionError(f"unexpected query {name}")

    monkeypatch.setattr(schedule_audit, "convex_query", fake_convex_query)

    async def _run():
        return await schedule_audit._gather_schedule_audit(worker_id="worker-x", now_ms=now)

    entries = asyncio.run(_run())

    summary = entries[0]
    assert summary["event"] == "schedule.audit.summary"
    assert summary["data"]["total"] == 2
    site_entries = [e for e in entries if e["event"] == "schedule.audit.site"]
    due_entries = [e for e in site_entries if e["data"]["due"]]
    assert len(due_entries) == 1
    assert due_entries[0]["siteId"] == "site-1"
