from __future__ import annotations

import json
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SITES_YAML = REPO_ROOT / "job_scrape_application" / "config" / "prod" / "site_schedules.yml"

SCHEDULE_AUDIT_INTERVALS_PER_DAY = 48  # every 30 minutes
LOCK_CLEAR_INTERVALS_PER_DAY = 720  # every 2 minutes
MAX_ESTIMATED_GB_PER_DAY = 0.5  # guardrail for read amplification
MAX_LOCK_CLEAR_GB_PER_DAY = 0.25
MAX_QUEUE_LIST_GB_PER_DAY = 0.5


def _json_size(obj: object) -> int:
    return len(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _load_site_schedule_entries() -> list[dict]:
    if not SITES_YAML.exists():
        return []
    parsed = yaml.safe_load(SITES_YAML.read_text()) or {}
    entries = parsed.get("site_schedules", []) if isinstance(parsed, dict) else []
    return [entry for entry in entries if isinstance(entry, dict)]


def test_schedule_audit_read_budget_estimate() -> None:
    entries = _load_site_schedule_entries()
    if not entries:
        assert False, f"No site schedules found at {SITES_YAML}"

    # Approximate the data pulled by schedule_audit: listSites + listSchedules.
    site_docs = []
    schedule_docs: dict[str, dict] = {}
    for idx, entry in enumerate(entries):
        url = entry.get("url") or ""
        schedule = entry.get("schedule") or {}
        schedule_name = schedule.get("name")
        site_docs.append(
            {
                "_id": f"site-{idx}",
                "url": url,
                "type": entry.get("type") or "general",
                "scrapeProvider": entry.get("scrapeProvider") or "spidercloud",
                "scheduleId": schedule_name,
                "lastRunAt": 0,
                "manualTriggerAt": 0,
                "lockExpiresAt": 0,
                "completed": False,
                "failed": False,
            }
        )
        if schedule_name and schedule_name not in schedule_docs:
            schedule_docs[schedule_name] = {
                "_id": schedule_name,
                "name": schedule_name,
                "days": schedule.get("days", []),
                "startTime": schedule.get("startTime", "00:00"),
                "intervalMinutes": schedule.get("intervalMinutes", 1440),
                "timezone": schedule.get("timezone", "America/Denver"),
                "createdAt": 0,
                "updatedAt": 0,
            }

    per_run_bytes = sum(_json_size(site) for site in site_docs) + sum(
        _json_size(sched) for sched in schedule_docs.values()
    )
    daily_bytes = per_run_bytes * SCHEDULE_AUDIT_INTERVALS_PER_DAY
    daily_gb = daily_bytes / 1_000_000_000

    assert daily_gb < MAX_ESTIMATED_GB_PER_DAY, (
        f"Estimated schedule audit reads {daily_gb:.3f} GB/day, "
        f"over {MAX_ESTIMATED_GB_PER_DAY} GB/day guardrail."
    )


def test_clear_expired_site_locks_read_budget_estimate() -> None:
    entries = _load_site_schedule_entries()
    if not entries:
        assert False, f"No site schedules found at {SITES_YAML}"

    site_docs = []
    for idx, entry in enumerate(entries):
        url = entry.get("url") or ""
        site_docs.append(
            {
                "_id": f"site-{idx}",
                "url": url,
                "enabled": entry.get("enabled", True),
                "scheduleId": (entry.get("schedule") or {}).get("name"),
                "lockedBy": "worker-x",
                "lockExpiresAt": 1,
                "lastRunAt": 0,
                "failed": False,
            }
        )

    per_run_bytes = sum(_json_size(site) for site in site_docs)
    daily_bytes = per_run_bytes * LOCK_CLEAR_INTERVALS_PER_DAY
    daily_gb = daily_bytes / 1_000_000_000

    assert daily_gb < MAX_LOCK_CLEAR_GB_PER_DAY, (
        f"Estimated clearExpiredSiteLocks reads {daily_gb:.3f} GB/day, "
        f"over {MAX_LOCK_CLEAR_GB_PER_DAY} GB/day guardrail."
    )


def test_list_queued_jobs_read_budget_estimate() -> None:
    fixture = (
        REPO_ROOT
        / "tests"
        / "job_scrape_application"
        / "workflows"
        / "fixtures"
        / "scrape_queue_fixture.json"
    )
    if not fixture.exists():
        assert False, f"Missing scrape queue fixture at {fixture}"

    raw = json.loads(fixture.read_text())
    rows = raw.get("rows", [])
    if not rows:
        assert False, "scrape queue fixture is empty"

    avg_row_bytes = sum(_json_size(row) for row in rows) / len(rows)
    page_size = 20
    polls_per_day = 2880  # every 30s
    daily_bytes = avg_row_bytes * page_size * polls_per_day
    daily_gb = daily_bytes / 1_000_000_000

    assert daily_gb < MAX_QUEUE_LIST_GB_PER_DAY, (
        f"Estimated listQueuedJobs reads {daily_gb:.3f} GB/day (page_size={page_size}, polls/day={polls_per_day}), "
        f"over {MAX_QUEUE_LIST_GB_PER_DAY} GB/day guardrail."
    )
