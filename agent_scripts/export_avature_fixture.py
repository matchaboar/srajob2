#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
FIXTURE_DIR = REPO_ROOT / "tests" / "job_scrape_application" / "workflows" / "fixtures"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


async def _fetch_scrape_logs_by_handler(convex_query, handler_name: str, limit: int) -> List[Dict[str, Any]]:
    """Fetch scrape logs for a specific site handler."""
    logs = await convex_query(
        "router:listUrlScrapeLogs",
        {"limit": limit, "includeJobLookup": True},
    )
    if not isinstance(logs, list):
        return []
    
    # Filter for avature handler
    filtered = []
    for log in logs:
        if not isinstance(log, dict):
            continue
        site_handler = log.get("siteHandler")
        if site_handler and handler_name.lower() in str(site_handler).lower():
            filtered.append(log)
    
    return filtered


async def _fetch_queued_urls_by_handler(convex_query, handler_name: str, limit: int) -> List[Dict[str, Any]]:
    """Fetch queued scrape URLs for a specific site handler."""
    rows = await convex_query(
        "router:listQueuedScrapeUrls",
        {"limit": limit, "provider": "spidercloud"},
    )
    if not isinstance(rows, list):
        return []
    
    # Filter for avature handler
    filtered = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        site_handler = row.get("siteHandler")
        url = row.get("url")
        if (site_handler and handler_name.lower() in str(site_handler).lower()) or \
           (url and "avature" in str(url).lower()):
            filtered.append(row)
    
    return filtered


def _normalize_log_entry(log: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a log entry to include only relevant fields."""
    return {
        k: v for k, v in {
            "url": log.get("url"),
            "siteHandler": log.get("siteHandler"),
            "status": log.get("status"),
            "provider": log.get("provider"),
            "jobId": log.get("jobId"),
            "jobTitle": log.get("jobTitle"),
            "jobCompany": log.get("jobCompany"),
            "scrapedAt": log.get("scrapedAt"),
            "error": log.get("error"),
        }.items() if v is not None
    }


def _write_fixture(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Avature scrape logs and queued URLs into a JSON fixture.",
    )
    parser.add_argument(
        "--type",
        choices=["logs", "queue", "both"],
        default="both",
        help="Type of data to export (default: both).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of entries to export per type (default: 50).",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="prod",
        help="Convex env to target (default: prod).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override output path (defaults to tests/job_scrape_application/workflows/fixtures/avature_fixture.json).",
    )
    args = parser.parse_args()

    limit = max(1, args.limit)
    _load_env(args.env)

    from job_scrape_application.services import convex_query  # noqa: E402

    logs: List[Dict[str, Any]] = []
    queue: List[Dict[str, Any]] = []

    if args.type in ("logs", "both"):
        print(f"Fetching Avature scrape logs (limit: {limit})...")
        logs = await _fetch_scrape_logs_by_handler(convex_query, "avature", limit)
        print(f"  Found {len(logs)} log entries")

    if args.type in ("queue", "both"):
        print(f"Fetching Avature queued URLs (limit: {limit})...")
        queue = await _fetch_queued_urls_by_handler(convex_query, "avature", limit)
        print(f"  Found {len(queue)} queued URLs")

    if not logs and not queue:
        raise SystemExit("No Avature data found in database.")

    # Normalize log entries
    normalized_logs = [_normalize_log_entry(log) for log in logs]

    output_path = Path(args.output) if args.output else (FIXTURE_DIR / "avature_fixture.json")

    now_ms = int(time.time() * 1000)
    payload = {
        "meta": {
            "handler": "avature",
            "type": args.type,
            "limit": limit,
            "logCount": len(normalized_logs),
            "queueCount": len(queue),
            "env": args.env,
            "generatedAtMs": now_ms,
            "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ms / 1000)),
        },
        "logs": normalized_logs,
        "queue": queue,
    }

    _write_fixture(output_path, payload)
    print(json.dumps({
        "output": str(output_path),
        "logCount": len(normalized_logs),
        "queueCount": len(queue),
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
