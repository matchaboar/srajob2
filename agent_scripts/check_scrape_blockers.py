#!/usr/bin/env python3
"""Check for recent scrape errors and blockers in Convex.

This script identifies potential reasons why scrapes might not be persisting:
- Scrape errors recorded in scrape_errors table
- Failed sites (sites with failed=true)
- Queue items stuck in failed/invalid state
- Description word limit violations
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _format_ts(ts: Optional[int | float]) -> str:
    if ts is None:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


def _time_ago(ts: Optional[int | float]) -> str:
    if ts is None:
        return "N/A"
    try:
        now = datetime.now(timezone.utc).timestamp() * 1000
        diff_seconds = (now - ts) / 1000
        if diff_seconds < 60:
            return f"{int(diff_seconds)}s ago"
        elif diff_seconds < 3600:
            return f"{int(diff_seconds / 60)}m ago"
        elif diff_seconds < 86400:
            return f"{int(diff_seconds / 3600)}h ago"
        else:
            return f"{int(diff_seconds / 86400)}d ago"
    except Exception:
        return str(ts)


async def _fetch_scrape_errors(
    convex_query, limit: int, site_filter: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Fetch recent scrape errors."""
    errors = await convex_query("router:listScrapeErrors", {"limit": limit})
    if not isinstance(errors, list):
        return []
    if site_filter:
        site_lower = site_filter.lower()
        errors = [
            e
            for e in errors
            if site_lower in (e.get("sourceUrl") or "").lower()
            or site_lower in (e.get("error") or "").lower()
        ]
    return errors


async def _fetch_failed_sites(convex_query) -> List[Dict[str, Any]]:
    """Fetch sites marked as failed."""
    sites = await convex_query("sites:listFailedSites", {})
    return sites if isinstance(sites, list) else []


async def _fetch_failed_queue_items(
    convex_query, limit: int
) -> List[Dict[str, Any]]:
    """Fetch queue items in failed/invalid state."""
    try:
        result = await convex_query(
            "jobs:listQueuedJobs",
            {
                "paginationOpts": {"numItems": limit, "cursor": None},
                "status": "failed",
            },
        )
        if isinstance(result, dict) and "page" in result:
            return result["page"]
    except Exception:
        pass
    return []


async def _fetch_scrape_activity_summary(convex_query) -> Dict[str, Any]:
    """Get scrape activity summary."""
    rows = await convex_query("sites:listScrapeActivity", {})
    if not isinstance(rows, list):
        return {"count": 0, "rows": []}
    # Count by status/recent activity
    now = datetime.now(timezone.utc).timestamp() * 1000
    recent_24h = [r for r in rows if (now - (r.get("completedAt") or 0)) < 86400000]
    return {
        "total_sites": len(rows),
        "active_24h": len(recent_24h),
        "rows": rows[:20],
    }


def _categorize_errors(errors: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Categorize errors by type."""
    categories: Dict[str, List[Dict[str, Any]]] = {
        "timeout": [],
        "description_limit": [],
        "validation": [],
        "network": [],
        "unknown": [],
    }
    for err in errors:
        error_text = (err.get("error") or "").lower()
        if "timeout" in error_text or "timed out" in error_text:
            categories["timeout"].append(err)
        elif "description" in error_text and "word" in error_text:
            categories["description_limit"].append(err)
        elif (
            "validation" in error_text
            or "invalid" in error_text
            or "required" in error_text
        ):
            categories["validation"].append(err)
        elif (
            "network" in error_text
            or "connection" in error_text
            or "fetch" in error_text
        ):
            categories["network"].append(err)
        else:
            categories["unknown"].append(err)
    return categories


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check for recent scrape errors and blockers in Convex."
    )
    parser.add_argument("--env", choices=("dev", "prod"), default="prod")
    parser.add_argument(
        "--site-filter",
        dest="site_filter",
        help="Optional site URL/name filter for errors.",
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--out", help="Optional output JSON path")
    args = parser.parse_args()

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    print(f"Checking Convex {args.env} for scrape blockers...")
    if args.site_filter:
        print(f"Site filter: {args.site_filter}")
    print("-" * 60)

    # Fetch all data in parallel
    errors, failed_sites, failed_queue, activity = await asyncio.gather(
        _fetch_scrape_errors(convex_query, args.limit, args.site_filter),
        _fetch_failed_sites(convex_query),
        _fetch_failed_queue_items(convex_query, args.limit),
        _fetch_scrape_activity_summary(convex_query),
    )

    # Categorize errors
    error_categories = _categorize_errors(errors)

    result = {
        "env": args.env,
        "site_filter": args.site_filter,
        "summary": {
            "total_errors": len(errors),
            "failed_sites": len(failed_sites),
            "failed_queue_items": len(failed_queue),
            "error_categories": {k: len(v) for k, v in error_categories.items()},
            "scrape_activity": {
                "total_sites": activity.get("total_sites", 0),
                "active_24h": activity.get("active_24h", 0),
            },
        },
        "errors": [],
        "failed_sites": [],
        "failed_queue": [],
        "error_sources": {},
    }

    # Count error sources
    source_counts: Counter[str] = Counter()
    for err in errors:
        source = err.get("sourceUrl") or "unknown"
        source_counts[source] += 1
    result["error_sources"] = dict(source_counts.most_common(20))

    # Process errors (most recent first)
    for err in errors[:50]:
        result["errors"].append(
            {
                "id": str(err.get("_id", "")),
                "sourceUrl": err.get("sourceUrl"),
                "error": err.get("error", "")[:200],
                "event": err.get("event"),
                "status": err.get("status"),
                "createdAt": _format_ts(err.get("createdAt")),
                "timeAgo": _time_ago(err.get("createdAt")),
            }
        )

    # Process failed sites
    for site in failed_sites:
        result["failed_sites"].append(
            {
                "id": str(site.get("_id", "")),
                "name": site.get("name"),
                "url": site.get("url"),
                "failCount": site.get("failCount"),
                "lastError": site.get("lastError", "")[:200] if site.get("lastError") else None,
                "lastFailureAt": _format_ts(site.get("lastFailureAt")),
            }
        )

    # Process failed queue items
    for item in failed_queue[:20]:
        result["failed_queue"].append(
            {
                "id": str(item.get("_id", "")),
                "url": item.get("url"),
                "sourceUrl": item.get("sourceUrl"),
                "status": item.get("status"),
                "lastError": item.get("lastError", "")[:200] if item.get("lastError") else None,
                "attempts": item.get("attempts"),
            }
        )

    # Print summary
    print(f"\n{'='*60}")
    print("BLOCKER SUMMARY")
    print(f"{'='*60}")
    print(f"Total scrape errors:     {len(errors)}")
    print(f"Failed sites:            {len(failed_sites)}")
    print(f"Failed queue items:      {len(failed_queue)}")
    print(f"Sites active (24h):      {activity.get('active_24h', 0)}/{activity.get('total_sites', 0)}")

    print(f"\n{'='*60}")
    print("ERROR CATEGORIES")
    print(f"{'='*60}")
    for cat, errs in error_categories.items():
        if errs:
            print(f"  {cat}: {len(errs)}")

    if errors:
        print(f"\n{'='*60}")
        print("RECENT ERRORS")
        print(f"{'='*60}")
        for err in result["errors"][:10]:
            print(f"\n  [{err['timeAgo']}] {err['sourceUrl']}")
            print(f"    Event: {err['event']} | Status: {err['status']}")
            print(f"    Error: {err['error'][:100]}...")

    if result["error_sources"]:
        print(f"\n{'='*60}")
        print("TOP ERROR SOURCES")
        print(f"{'='*60}")
        for source, count in list(result["error_sources"].items())[:10]:
            print(f"  {count:3d} errors: {source[:60]}")

    if failed_sites:
        print(f"\n{'='*60}")
        print("FAILED SITES")
        print(f"{'='*60}")
        for site in result["failed_sites"][:10]:
            print(f"\n  {site['name']} ({site['url'][:50]})")
            print(f"    Fail count: {site['failCount']} | Last: {site['lastFailureAt']}")
            if site.get("lastError"):
                print(f"    Error: {site['lastError'][:80]}...")

    # Save full output if requested
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nFull output saved to: {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
