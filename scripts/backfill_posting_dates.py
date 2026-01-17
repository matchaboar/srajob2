#!/usr/bin/env python3
"""
Backfill posting dates for Greenhouse and DocuSign jobs.

This script:
1. Reads site schedules to find all Greenhouse and DocuSign sites
2. For each site, fetches the listing page from the API
3. Extracts posting dates from each job
4. Sends batch updates to Convex (Convex handles finding existing jobs)

Usage:
    uv run python scripts/backfill_posting_dates.py --env prod --dry-run
    uv run python scripts/backfill_posting_dates.py --env prod
    uv run python scripts/backfill_posting_dates.py --env prod --site Docusign
"""
from __future__ import annotations

import argparse
import asyncio
import orjson
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
        return orjson.loads(stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running convex command: {' '.join(args[:5])}...")
        if e.stderr:
            print(f"Error: {e.stderr.strip()[:200]}")
        return None
    except orjson.JSONDecodeError:
        return None


def build_convex_run_args(env: str, function_name: str, payload: Dict[str, Any]) -> List[str]:
    """Build command line args for 'npx convex run'."""
    cmd = ["npx", "convex", "run"]
    if env == "prod":
        cmd.append("--prod")
    cmd.append(function_name)
    cmd.append(orjson.dumps(payload).decode("utf-8"))
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


def get_supported_sites(schedules: List[Dict[str, Any]], site_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """Filter to only supported sites (Greenhouse and DocuSign)."""
    supported_sites = []
    for site in schedules:
        site_type = site.get("type", "")
        if site_type not in ("greenhouse", "docusign"):
            continue
        if not site.get("enabled", False):
            continue
        if site_filter and site.get("name", "") != site_filter:
            continue
        supported_sites.append(site)
    return supported_sites


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
        data = orjson.loads(response.text)
        return data.get("jobs", [])


async def fetch_docusign_listing(url: str) -> Dict[str, Any]:
    """Fetch the job listing from DocuSign API."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url, headers={"Accept": "application/json"})
        response.raise_for_status()
        return orjson.loads(response.text)


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


def build_updates_from_docusign(posted_at_by_url: Dict[str, int]) -> List[Dict[str, Any]]:
    """Build update payloads from DocuSign job listing."""
    updates = []
    for url, posted_at_ms in posted_at_by_url.items():
        if not url or not posted_at_ms:
            continue
        update: Dict[str, Any] = {
            "urls": [url],
            "postedAt": posted_at_ms,
        }
        updates.append(update)
    return updates


async def process_site(
    site: Dict[str, Any],
    target_env: str,
    env_vars: Dict[str, str],
    dry_run: bool,
) -> Dict[str, Any]:
    """Process a single site (Greenhouse or DocuSign)."""
    site_name = site.get("name", "unknown")
    site_type = site.get("type", "")
    site_url = site.get("url", "")

    print(f"  {site_name} ({site_type}): ", end="", flush=True)

    # Route to appropriate handler
    if site_type == "greenhouse":
        return await process_greenhouse_site(site_name, site_url, target_env, env_vars, dry_run)
    elif site_type == "docusign":
        return await process_docusign_site(site_name, site_url, target_env, env_vars, dry_run)
    else:
        return {"site": site_name, "error": f"Unsupported site type: {site_type}", "updated": 0}


async def process_greenhouse_site(
    site_name: str,
    site_url: str,
    target_env: str,
    env_vars: Dict[str, str],
    dry_run: bool,
) -> Dict[str, Any]:
    """Process a Greenhouse site."""
    board_slug = extract_board_slug(site_url)

    if not board_slug:
        print("ERROR - Could not extract board slug")
        return {"site": site_name, "error": "Could not extract board slug", "updated": 0}

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
    return await send_batch_updates(site_name, len(jobs), updates, target_env, env_vars)


async def process_docusign_site(
    site_name: str,
    site_url: str,
    target_env: str,
    env_vars: Dict[str, str],
    dry_run: bool,
) -> Dict[str, Any]:
    """Process a DocuSign site."""
    # Import DocuSign handler
    from job_scrape_application.workflows.site_handlers.docusign import DocusignHandler

    # For backfilling, use unfiltered API to get ALL jobs (not just filtered ones from schedule)
    # The site_url from schedule may have category/location filters that exclude some jobs
    parsed = urlparse(site_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    # Add pagination parameters
    unfiltered_url = f"{base_url}?page=1&limit=100"

    # Fetch all pages from DocuSign API
    handler = DocusignHandler()
    all_posted_at_by_url: Dict[str, int] = {}
    total_jobs = 0

    try:
        # Fetch first page
        payload = await fetch_docusign_listing(unfiltered_url)
        page_posted_at = handler.get_posted_at_by_url(payload)
        all_posted_at_by_url.update(page_posted_at)
        total_jobs += len(payload.get("jobs", []))

        # Get all pagination URLs
        pagination_urls = handler.get_pagination_urls_from_json(payload, unfiltered_url)

        # Fetch all remaining pages
        for page_url in pagination_urls:
            page_payload = await fetch_docusign_listing(page_url)
            page_posted_at = handler.get_posted_at_by_url(page_payload)
            all_posted_at_by_url.update(page_posted_at)
            total_jobs += len(page_payload.get("jobs", []))

    except Exception as e:
        print(f"ERROR - {e}")
        return {"site": site_name, "error": str(e), "updated": 0}

    updates = build_updates_from_docusign(all_posted_at_by_url)

    if not updates:
        print(f"{total_jobs} jobs, 0 with dates")
        return {"site": site_name, "jobs_fetched": total_jobs, "updated": 0}

    if dry_run:
        print(f"{total_jobs} jobs, {len(updates)} with dates (dry-run)")
        return {"site": site_name, "jobs_fetched": total_jobs, "updates": len(updates), "updated": 0}

    # Send batch updates to Convex
    return await send_batch_updates(site_name, total_jobs, updates, target_env, env_vars)


async def send_batch_updates(
    site_name: str,
    jobs_count: int,
    updates: List[Dict[str, Any]],
    target_env: str,
    env_vars: Dict[str, str],
) -> Dict[str, Any]:
    """Send batch updates to Convex."""
    total_updated = 0
    total_not_found = 0

    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        cmd = build_convex_run_args(target_env, "admin:batchPatchPostingDates", {"updates": batch})
        result = run_convex(cmd, env=env_vars)

        if result:
            total_updated += result.get("updated", 0)
            total_not_found += result.get("notFound", 0)

    print(f"{jobs_count} jobs, {total_updated} updated, {total_not_found} not in DB")

    return {
        "site": site_name,
        "jobs_fetched": jobs_count,
        "updates_sent": len(updates),
        "updated": total_updated,
        "not_found": total_not_found,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill posting dates for Greenhouse and DocuSign jobs")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--site", help="Process only a specific site (by name, e.g., 'Docusign')")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated")
    parser.add_argument("--limit", type=int, help="Limit the number of sites to process")
    args = parser.parse_args()

    load_env(args.env)
    env_vars = os.environ.copy()

    print(f"Environment: {args.env}" + (" (DRY RUN)" if args.dry_run else ""))

    schedules = load_site_schedules()
    supported_sites = get_supported_sites(schedules, args.site)

    if args.limit:
        supported_sites = supported_sites[:args.limit]

    print(f"Processing {len(supported_sites)} sites (Greenhouse + DocuSign)\n")

    if not supported_sites:
        return

    results = []
    for site in supported_sites:
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
