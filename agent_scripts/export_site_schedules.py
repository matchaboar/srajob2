#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict
import sys

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.config import get_config_env, get_env_dir  # noqa: E402
from job_scrape_application.services import convex_query  # noqa: E402


def _strip_none(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in payload.items() if v is not None}


def _normalize_site_entry(site: Dict[str, Any], schedule: Dict[str, Any] | None) -> Dict[str, Any]:
    schedule_block = None
    if schedule:
        schedule_block = _strip_none(
            {
                "name": schedule.get("name"),
                "days": schedule.get("days"),
                "startTime": schedule.get("startTime"),
                "intervalMinutes": schedule.get("intervalMinutes"),
                "timezone": schedule.get("timezone"),
            }
        )

    return _strip_none(
        {
            "url": site.get("url"),
            "name": site.get("name"),
            "enabled": site.get("enabled"),
            "type": site.get("type"),
            "scrapeProvider": site.get("scrapeProvider"),
            "pattern": site.get("pattern"),
            "schedule": schedule_block,
        }
    )


async def _fetch_site_schedules() -> list[Dict[str, Any]]:
    sites = await convex_query("router:listSites", {"enabledOnly": False}) or []
    schedules = await convex_query("router:listSchedules", {}) or []
    schedule_map = {str(row.get("_id")): row for row in schedules if isinstance(row, dict)}

    rows: list[Dict[str, Any]] = []
    for site in sites:
        if not isinstance(site, dict):
            continue
        schedule_id = site.get("scheduleId")
        if not schedule_id:
            continue
        schedule = schedule_map.get(str(schedule_id))
        rows.append(_normalize_site_entry(site, schedule))

    rows.sort(key=lambda row: str(row.get("url") or ""))
    return rows


def _write_yaml(entries: list[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"site_schedules": entries}
    output_path.write_text(yaml.safe_dump(payload, sort_keys=False))


async def main() -> None:
    parser = argparse.ArgumentParser(description="Export Convex site schedules to YAML.")
    parser.add_argument(
        "--env",
        default=get_config_env(),
        choices=["dev", "prod"],
        help="Config environment to write (dev or prod).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override output YAML path (defaults to config/<env>/site_schedules.yml).",
    )
    args = parser.parse_args()

    entries = await _fetch_site_schedules()
    output_path = Path(args.output) if args.output else get_env_dir(args.env) / "site_schedules.yml"
    _write_yaml(entries, output_path)
    print(f"Wrote {len(entries)} site schedule entries to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
