from __future__ import annotations

import orjson
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SITES_YAML = REPO_ROOT / "job_scrape_application" / "config" / "prod" / "site_schedules.yml"

SCHEDULE_CONFIG_REFRESH_SECONDS = 600
SITES_REFRESH_SECONDS = 300
MAX_SCHEDULE_READ_GB_PER_DAY = 0.25
MAX_SITE_READ_GB_PER_DAY = 0.5


def _json_size(obj: object) -> int:
    return len(orjson.dumps(obj))


def _load_site_schedule_entries() -> list[dict]:
    if not SITES_YAML.exists():
        return []
    parsed = yaml.safe_load(SITES_YAML.read_text()) or {}
    entries = parsed.get("site_schedules", []) if isinstance(parsed, dict) else []
    return [entry for entry in entries if isinstance(entry, dict)]


def _build_site_docs(entries: list[dict]) -> list[dict]:
    site_docs: list[dict] = []
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
                "enabled": entry.get("enabled", True),
                "lastRunAt": 0,
                "manualTriggerAt": 0,
                "lockExpiresAt": 0,
                "completed": False,
                "failed": False,
            }
        )
    return site_docs


def test_dbos_schedule_config_read_budget_estimate() -> None:
    entries = _load_site_schedule_entries()
    if not entries:
        assert False, f"No site schedules found at {SITES_YAML}"

    schedule_doc = {
        "mode": "interval",
        "time": "08:00",
        "timezone": "America/Denver",
        "intervalMinutes": 15,
        "name": "scrape-every-15",
    }

    refreshes_per_day = int(86_400 / SCHEDULE_CONFIG_REFRESH_SECONDS)
    daily_bytes = _json_size(schedule_doc) * refreshes_per_day
    daily_gb = daily_bytes / 1_000_000_000

    assert daily_gb < MAX_SCHEDULE_READ_GB_PER_DAY, (
        f"Estimated DBOS schedule config reads {daily_gb:.3f} GB/day, "
        f"over {MAX_SCHEDULE_READ_GB_PER_DAY} GB/day guardrail."
    )


def test_dbos_site_list_read_budget_estimate() -> None:
    entries = _load_site_schedule_entries()
    if not entries:
        assert False, f"No site schedules found at {SITES_YAML}"

    site_docs = _build_site_docs(entries)
    refreshes_per_day = int(86_400 / SITES_REFRESH_SECONDS)
    per_run_bytes = sum(_json_size(site) for site in site_docs)
    daily_bytes = per_run_bytes * refreshes_per_day
    daily_gb = daily_bytes / 1_000_000_000

    assert daily_gb < MAX_SITE_READ_GB_PER_DAY, (
        f"Estimated DBOS listSites reads {daily_gb:.3f} GB/day, "
        f"over {MAX_SITE_READ_GB_PER_DAY} GB/day guardrail."
    )
