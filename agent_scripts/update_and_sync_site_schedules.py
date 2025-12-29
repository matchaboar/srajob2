#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict, Iterable, List
import sys

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.config import get_env_dir  # noqa: E402
from job_scrape_application.services import convex_mutation, convex_query  # noqa: E402

DEFAULT_TIMEZONE = "America/Denver"


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text()) or {}
    return raw if isinstance(raw, dict) else {}


def _write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False))


def _parse_days(days_csv: str) -> List[str]:
    return [d.strip().lower() for d in days_csv.split(",") if d.strip()]


def _update_entries(
    entries: Iterable[Dict[str, Any]],
    *,
    days: List[str],
    interval_minutes: float,
    name_template: str,
) -> List[Dict[str, Any]]:
    updated: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        schedule = entry.get("schedule")
        if isinstance(schedule, dict):
            start_time = schedule.get("startTime") or "09:30"
            schedule["startTime"] = start_time
            schedule["days"] = days
            schedule["intervalMinutes"] = float(interval_minutes)
            schedule["timezone"] = schedule.get("timezone") or DEFAULT_TIMEZONE
            schedule["name"] = name_template.format(startTime=start_time)
        updated.append(entry)
    return updated


def _schedule_key(name: str) -> str:
    return name.strip().lower()


def _strip_none(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


async def _push_to_convex(entries: List[Dict[str, Any]]) -> None:
    schedules = await convex_query("router:listSchedules", {}) or []
    schedule_map = {
        _schedule_key(row.get("name", "")): row for row in schedules if isinstance(row, dict)
    }
    schedule_ids: Dict[str, str] = {}

    for entry in entries:
        schedule = entry.get("schedule")
        if not isinstance(schedule, dict):
            continue
        name = schedule.get("name", "")
        if not isinstance(name, str) or not name.strip():
            continue
        key = _schedule_key(name)
        if key in schedule_ids:
            continue
        existing = schedule_map.get(key)
        args: Dict[str, Any] = {
            "name": name,
            "days": schedule.get("days", []),
            "startTime": schedule.get("startTime"),
            "intervalMinutes": schedule.get("intervalMinutes"),
            "timezone": schedule.get("timezone"),
        }
        if existing and existing.get("_id"):
            args["id"] = existing["_id"]
        schedule_id = await convex_mutation("router:upsertSchedule", _strip_none(args))
        schedule_ids[key] = schedule_id

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        schedule_id = None
        schedule = entry.get("schedule")
        if isinstance(schedule, dict):
            name = schedule.get("name", "")
            key = _schedule_key(str(name))
            schedule_id = schedule_ids.get(key)
        args = {
            "name": entry.get("name"),
            "url": entry.get("url"),
            "type": entry.get("type"),
            "scrapeProvider": entry.get("scrapeProvider"),
            "pattern": entry.get("pattern"),
            "scheduleId": schedule_id,
            "enabled": bool(entry.get("enabled", True)),
        }
        await convex_mutation("router:upsertSite", _strip_none(args))


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update site schedules to weekdays every 2 hours and sync to Convex."
    )
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument(
        "--days",
        default="mon,tue,wed,thu,fri",
        help="Comma-separated day list to apply to every schedule.",
    )
    parser.add_argument(
        "--interval-minutes",
        type=float,
        default=120.0,
        help="Interval minutes to apply to every schedule.",
    )
    parser.add_argument(
        "--name-template",
        default="Weekdays every 2 hours @ {startTime}",
        help="Schedule name template. Use {startTime} to include start time.",
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="Only update YAML; skip pushing to Convex.",
    )
    args = parser.parse_args()

    yaml_path = get_env_dir(args.env) / "site_schedules.yml"
    payload = _load_yaml(yaml_path)
    entries = payload.get("site_schedules", [])
    if not isinstance(entries, list):
        entries = []

    days = _parse_days(args.days)
    updated_entries = _update_entries(
        entries,
        days=days,
        interval_minutes=args.interval_minutes,
        name_template=args.name_template,
    )
    payload["site_schedules"] = updated_entries
    _write_yaml(yaml_path, payload)
    print(f"Updated schedules in {yaml_path}")

    if not args.no_push:
        await _push_to_convex(updated_entries)
        print("Pushed schedules to Convex.")


if __name__ == "__main__":
    asyncio.run(main())
