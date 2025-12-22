from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest


def _load_module():
    module_path = Path(__file__).resolve().parents[2] / "agent_scripts" / "diagnose_spidercloud_stalls.py"
    spec = importlib.util.spec_from_file_location("diagnose_spidercloud_stalls", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load diagnose_spidercloud_stalls module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _to_ms(year: int, month: int, day: int, hour: int, minute: int, tz: str) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(tz)).timestamp() * 1000)


def test_summarize_queue_marks_stale_and_sources():
    ds = _load_module()
    now_ms = 1_700_000_000_000
    rows = [
        {
            "url": "https://example.com/1",
            "status": "pending",
            "attempts": 0,
            "createdAt": now_ms - 40 * 60 * 1000,
            "updatedAt": now_ms - 30 * 60 * 1000,
            "sourceUrl": "https://source-a.test",
        },
        {
            "url": "https://example.com/2",
            "status": "pending",
            "attempts": 2,
            "createdAt": now_ms - 10 * 60 * 1000,
            "updatedAt": now_ms - 5 * 60 * 1000,
            "sourceUrl": "https://source-a.test",
        },
        {
            "url": "https://example.com/3",
            "status": "pending",
            "attempts": 1,
            "createdAt": now_ms - 50 * 60 * 1000,
            "updatedAt": now_ms - 45 * 60 * 1000,
            "sourceUrl": "https://source-b.test",
            "lastError": "Payment Required: insufficient credits",
        },
    ]

    summary = ds._summarize_queue(rows, now_ms, expire_minutes=20)

    assert summary["count"] == 3
    assert summary["staleCount"] == 2
    assert summary["maxAttempts"] == 2
    assert summary["topSources"][0] == ("https://source-a.test", 2)
    assert summary["topSources"][1] == ("https://source-b.test", 1)
    assert summary["sample"][0]["url"] == "https://example.com/1"


def test_latest_eligible_time_respects_start_and_interval():
    ds = _load_module()
    schedule = {
        "days": ["mon"],
        "startTime": "09:30",
        "intervalMinutes": 120,
        "timezone": "America/Denver",
    }

    before_start = _to_ms(2025, 12, 22, 2, 17, "America/Denver")
    assert ds._latest_eligible_time(schedule, before_start) is None

    after_start = _to_ms(2025, 12, 22, 12, 45, "America/Denver")
    expected = _to_ms(2025, 12, 22, 11, 30, "America/Denver")
    assert ds._latest_eligible_time(schedule, after_start) == expected


@pytest.mark.asyncio
async def test_gather_site_schedule_summary_classifies_due_and_not_due(monkeypatch):
    ds = _load_module()
    now_ms = _to_ms(2025, 12, 22, 12, 45, "America/Denver")
    eligible_at = _to_ms(2025, 12, 22, 11, 30, "America/Denver")

    schedules = [
        {
            "_id": "sched-1",
            "days": ["mon"],
            "startTime": "09:30",
            "intervalMinutes": 120,
            "timezone": "America/Denver",
        }
    ]
    sites = [
        {
            "_id": "site-due",
            "url": "https://explore.jobs.netflix.net/careers?query=engineer",
            "scheduleId": "sched-1",
            "lastRunAt": eligible_at - 60 * 60 * 1000,
        },
        {
            "_id": "site-not-due",
            "url": "https://example.com/not-due",
            "scheduleId": "sched-1",
            "lastRunAt": eligible_at + 1,
        },
        {"_id": "site-failed", "url": "https://example.com/failed", "failed": True},
        {
            "_id": "site-locked",
            "url": "https://example.com/locked",
            "lockExpiresAt": now_ms + 60 * 60 * 1000,
        },
        {"_id": "site-unscheduled", "url": "https://example.com/unscheduled"},
    ]

    async def fake_query(name: str, args: dict | None = None):
        if name == "router:listSites":
            return sites
        if name == "router:listSchedules":
            return schedules
        raise AssertionError(f"Unexpected query {name}")

    monkeypatch.setattr(ds, "convex_query", fake_query)

    summary = await ds._gather_site_schedule_summary(now_ms)

    assert summary["summary"]["total"] == 5
    assert summary["summary"]["due"] == 1
    assert summary["summary"]["not_due"] == 1
    assert summary["summary"]["failed"] == 1
    assert summary["summary"]["locked"] == 1
    assert summary["summary"]["unscheduled"] == 1
    assert summary["dueSamples"][0]["url"] == "https://explore.jobs.netflix.net/careers?query=engineer"


@pytest.mark.asyncio
async def test_gather_temporal_status_counts_workers_and_runs(monkeypatch):
    ds = _load_module()

    async def fake_query(name: str, args: dict | None = None):
        if name == "temporal:getActiveWorkers":
            return [
                {"workerId": "w1", "taskQueue": "scraper-task-queue"},
                {"workerId": "w2", "taskQueue": "scraper-task-queue"},
                {"workerId": "w3", "taskQueue": "job-details-queue"},
            ]
        if name == "temporal:getStaleWorkers":
            return [{"workerId": "s1"}, {"workerId": "s2"}, {"workerId": "s3"}]
        if name == "temporal:listWorkflowRuns":
            return [
                {"workflowName": "ScraperSpidercloud", "status": "completed"},
                {"workflowName": "ScraperSpidercloud", "status": "completed"},
                {"workflowName": "SpidercloudJobDetails", "status": "completed"},
                {"workflowName": "SiteLease", "status": "completed"},
            ]
        raise AssertionError(f"Unexpected query {name}")

    monkeypatch.setattr(ds, "convex_query", fake_query)

    status = await ds._gather_temporal_status()

    assert status["activeWorkers"] == 3
    assert status["staleWorkers"] == 3
    assert status["taskQueues"] == ["job-details-queue", "scraper-task-queue"]
    assert status["recentWorkflowCounts"]["ScraperSpidercloud"] == 2
    assert status["recentWorkflowCounts"]["SpidercloudJobDetails"] == 1


@pytest.mark.asyncio
async def test_gather_scrape_errors_includes_payment_required(monkeypatch):
    ds = _load_module()

    async def fake_query(name: str, args: dict | None = None):
        if name == "router:listScrapeErrors":
            return [
                {"event": "batch_scrape", "status": "error", "error": "Payment Required: insufficient credits"},
                {"event": "batch_scrape", "status": "error", "error": "Payment Required: insufficient credits"},
                {"event": "job_details", "status": "error", "error": "Timeout"},
            ]
        raise AssertionError(f"Unexpected query {name}")

    monkeypatch.setattr(ds, "convex_query", fake_query)

    errors = await ds._gather_scrape_errors()

    assert errors["count"] == 3
    assert errors["byEvent"]["batch_scrape"] == 2
    assert errors["byEvent"]["job_details"] == 1
    assert "Payment Required" in errors["sample"][0]["error"]
