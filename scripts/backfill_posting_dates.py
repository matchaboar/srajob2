#!/usr/bin/env python3
"""
Backfill posting dates for Greenhouse jobs.

This script:
1. Reads site schedules to find all Greenhouse sites
2. For each site, fetches the listing page from the Greenhouse API
3. Extracts first_published and updated_at from each job
4. Sends batch updates to Convex (Convex handles finding existing jobs)

Usage:
    uv run python scripts/backfill_posting_dates.py --env prod --dry-run
    uv run python scripts/backfill_posting_dates.py --env prod
    uv run python scripts/backfill_posting_dates.py --env prod --site airbnb
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
SITE_SCHEDULES_PATH = REPO_ROOT / "job_scrape_application" / "config" / "prod" / "site_schedules.yml"
BATCH_SIZE = 50  # Number of updates per Convex mutation call


def load_env(target_env: str) -> None:
    """Load environment variables for the target environment."""
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def run_convex(args: List[str], *, env: Dict[str, str]) -> Any:
    """Run a Convex CLI command and return parsed JSON output."""
    try:
        result = subprocess.run(
            args,
            cwd=str(CONVEX_DIR),
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        stdout = result.stdout.strip()
        if not stdout:
            return None
        return json.loads(stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running convex command: {' '.join(args[:5])}...")
        if e.stderr:
            print(f"Error: {e.stderr.strip()[:200]}")
        return None
    except json.JSONDecodeError:
        return None


def build_convex_run_args(env: str, function_name: str, payload: Dict[str, Any]) -> List[str]:
    """Build command line args for 'npx convex run'."""
    cmd = ["npx", "convex", "run"]
    if env == "prod":
        cmd.append("--prod")
    cmd.append(function_name)
    cmd.append(json.dumps(payload))
    return cmd


def parse_iso_to_ms(value: Any) -> Optional[int]:
    """Parse an ISO date string or timestamp to milliseconds."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value < 1e12:
            return int(value * 1000)
        return int(value)
    if isinstance(value, str):
        try:
            from datetime import datetime
            cleaned = value.strip()
            if cleaned.endswith("Z"):
                cleaned = cleaned[:-1] + "+00:00"
            dt = datetime.fromisoformat(cleaned)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
    return None


def load_site_schedules() -> List[Dict[str, Any]]:
    """Load site schedules from YAML file."""
    if not SITE_SCHEDULES_PATH.exists():
        print(f"Site schedules not found: {SITE_SCHEDULES_PATH}")
        return []
    with open(SITE_SCHEDULES_PATH, "r") as f:
        data = yaml.safe_load(f)
    return data.get("site_schedules", [])


def get_greenhouse_sites(schedules: List[Dict[str, Any]], site_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """Filter to only Greenhouse sites."""
    greenhouse_sites = []
    for site in schedules:
        if site.get("type", "") != "greenhouse":
            continue
        if not site.get("enabled", False):
            continue
        if site_filter and site.get("name", "") != site_filter:
            continue
        greenhouse_sites.append(site)
    return greenhouse_sites


def extract_board_slug(url: str) -> Optional[str]:
    """Extract the board slug from a Greenhouse API URL."""
    parsed = urlparse(url)
    path_parts = [p for p in parsed.path.split("/") if p]
    if len(path_parts) >= 3 and path_parts[0] == "v1" and path_parts[1] == "boards":
        return path_parts[2]
    return None


async def fetch_greenhouse_listing(url: str) -> List[Dict[str, Any]]:
    """Fetch the job listing from Greenhouse API."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url, headers={"Accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        return data.get("jobs", [])


def build_updates_from_jobs(jobs: List[Dict[str, Any]], board_slug: str) -> List[Dict[str, Any]]:
    """Build update payloads from Greenhouse job listing."""
    updates = []
    for job in jobs:
        job_id = job.get("id")
        if not job_id:
            continue

        updated_at_ms = parse_iso_to_ms(job.get("updated_at"))
        first_published_ms = parse_iso_to_ms(job.get("first_published"))

        if not updated_at_ms and not first_published_ms:
            continue

        # Build all possible URL formats for this job
        urls = [
            f"https://boards-api.greenhouse.io/v1/boards/{board_slug}/jobs/{job_id}",
            f"https://boards.greenhouse.io/{board_slug}/jobs/{job_id}",
            f"https://job-boards.greenhouse.io/{board_slug}/jobs/{job_id}",
        ]

        update: Dict[str, Any] = {"urls": urls}
        if updated_at_ms:
            update["postedAt"] = updated_at_ms
        if first_published_ms:
            update["postingFirstPublishedAt"] = first_published_ms

        updates.append(update)

    return updates


async def process_site(
    site: Dict[str, Any],
    target_env: str,
    env_vars: Dict[str, str],
    dry_run: bool,
) -> Dict[str, Any]:
    """Process a single Greenhouse site."""
    site_name = site.get("name", "unknown")
    site_url = site.get("url", "")
    board_slug = extract_board_slug(site_url)

    if not board_slug:
        return {"site": site_name, "error": "Could not extract board slug", "updated": 0}

    print(f"  {site_name}: ", end="", flush=True)

    # Fetch listing from Greenhouse API
    try:
        jobs = await fetch_greenhouse_listing(site_url)
    except Exception as e:
        print(f"ERROR - {e}")
        return {"site": site_name, "error": str(e), "updated": 0}

    # Build updates
    updates = build_updates_from_jobs(jobs, board_slug)

    if not updates:
        print(f"{len(jobs)} jobs, 0 with dates")
        return {"site": site_name, "jobs_fetched": len(jobs), "updated": 0}

    if dry_run:
        print(f"{len(jobs)} jobs, {len(updates)} with dates (dry-run)")
        return {"site": site_name, "jobs_fetched": len(jobs), "updates": len(updates), "updated": 0}

    # Send batch updates to Convex
    total_updated = 0
    total_not_found = 0

    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        cmd = build_convex_run_args(target_env, "admin:batchPatchPostingDates", {"updates": batch})
        result = run_convex(cmd, env=env_vars)

        if result:
            total_updated += result.get("updated", 0)
            total_not_found += result.get("notFound", 0)

    print(f"{len(jobs)} jobs, {total_updated} updated, {total_not_found} not in DB")

    return {
        "site": site_name,
        "jobs_fetched": len(jobs),
        "updates_sent": len(updates),
        "updated": total_updated,
        "not_found": total_not_found,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill posting dates for Greenhouse jobs")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--site", help="Process only a specific site (by name)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated")
    parser.add_argument("--limit", type=int, help="Limit the number of sites to process")
    args = parser.parse_args()

    load_env(args.env)
    env_vars = os.environ.copy()

    print(f"Environment: {args.env}" + (" (DRY RUN)" if args.dry_run else ""))

    schedules = load_site_schedules()
    greenhouse_sites = get_greenhouse_sites(schedules, args.site)

    if args.limit:
        greenhouse_sites = greenhouse_sites[:args.limit]

    print(f"Processing {len(greenhouse_sites)} Greenhouse sites\n")

    if not greenhouse_sites:
        return

    results = []
    for site in greenhouse_sites:
        try:
            result = await process_site(site, args.env, env_vars, args.dry_run)
            results.append(result)
        except Exception as e:
            print(f"  {site.get('name')}: ERROR - {e}")
            results.append({"site": site.get("name"), "error": str(e), "updated": 0})

    # Summary
    total_updated = sum(r.get("updated", 0) for r in results)
    total_fetched = sum(r.get("jobs_fetched", 0) for r in results)
    errors = [r for r in results if r.get("error")]

    print(f"\nTotal: {total_updated} updated across {len(results)} sites ({total_fetched} jobs fetched)")
    if errors:
        print(f"Errors: {len(errors)} sites had errors")


if __name__ == "__main__":
    asyncio.run(main())
