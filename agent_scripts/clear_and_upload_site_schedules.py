#!/usr/bin/env python3
"""Clear all sites and schedules from Convex, then upload from YAML files."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.services import convex_mutation, convex_query  # noqa: E402


async def clear_convex_sites_and_schedules() -> None:
    """Delete all sites and schedules from Convex."""
    print("Fetching all sites...")
    sites = await convex_query("router:listSites", {"enabledOnly": False}) or []
    print(f"Found {len(sites)} sites")

    # Delete all sites first (they reference schedules)
    for site in sites:
        site_id = site.get("_id")
        site_name = site.get("name", "unknown")
        if site_id:
            print(f"  Deleting site: {site_name} ({site_id})")
            await convex_mutation("router:deleteSite", {"id": site_id})

    print("Fetching all schedules...")
    schedules = await convex_query("router:listSchedules", {}) or []
    print(f"Found {len(schedules)} schedules")

    # Delete all schedules
    for schedule in schedules:
        schedule_id = schedule.get("_id")
        schedule_name = schedule.get("name", "unknown")
        if schedule_id:
            print(f"  Deleting schedule: {schedule_name} ({schedule_id})")
            await convex_mutation("router:deleteSchedule", {"id": schedule_id})

    print("All sites and schedules cleared from Convex")


async def upload_from_yaml(env: str) -> None:
    """Upload sites and schedules from YAML using the sync script."""
    print(f"\nUploading {env} schedules from YAML...")

    # Import and run the upload script
    from update_and_sync_site_schedules import _load_yaml
    from job_scrape_application.config import get_env_dir

    yaml_path = get_env_dir(env) / "site_schedules.yml"
    payload = _load_yaml(yaml_path)
    entries = payload.get("site_schedules", [])

    if not isinstance(entries, list):
        print(f"  Warning: No site_schedules found in {yaml_path}")
        return

    print(f"  Found {len(entries)} entries to upload")
    # Upload with paginationLimit included
    await _push_schedules_and_sites(entries)
    print(f"  Successfully uploaded {env} schedules")


async def _push_schedules_and_sites(entries: list) -> None:
    """Push schedules and sites to Convex including paginationLimit."""
    from update_and_sync_site_schedules import _schedule_key, _strip_none, _coerce_pagination_limit

    # First, create all schedules
    schedule_map = {}
    schedule_ids = {}

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

        args = {
            "name": name,
            "days": schedule.get("days", []),
            "startTime": schedule.get("startTime"),
            "intervalMinutes": schedule.get("intervalMinutes"),
            "timezone": schedule.get("timezone"),
        }
        schedule_id = await convex_mutation("router:upsertSchedule", _strip_none(args))
        schedule_ids[key] = schedule_id

    # Then create all sites
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
            "paginationLimit": _coerce_pagination_limit(entry.get("paginationLimit")),
            "scheduleId": schedule_id,
            "enabled": bool(entry.get("enabled", True)),
        }
        await convex_mutation("router:upsertSite", _strip_none(args))


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clear Convex DB and upload from YAML files."
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod", "both"],
        default="both",
        help="Which environment to process (default: both)",
    )
    args = parser.parse_args()

    envs_to_process = ["dev", "prod"] if args.env == "both" else [args.env]

    for env in envs_to_process:
        print(f"\n{'='*60}")
        print(f"Processing {env.upper()} environment")
        print(f"{'='*60}")

        # Set the correct Convex URL for this environment
        if env == "dev":
            env_file = REPO_ROOT / ".env"
        else:
            env_file = REPO_ROOT / "job_board_application" / ".env.production"

        if env_file.exists():
            print(f"Loading environment from {env_file}")
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        if key == "CONVEX_HTTP_URL":
                            os.environ[key] = value
                            print(f"  Using CONVEX_HTTP_URL: {value}")

        await clear_convex_sites_and_schedules()
        await upload_from_yaml(env)

    print(f"\n{'='*60}")
    print("All done!")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
