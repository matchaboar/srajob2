#!/usr/bin/env python3
"""Check recent scrapes and jobs for a specific site URL or company in Convex."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
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


def _matches_url(row: Dict[str, Any], site_url: str) -> bool:
    source = row.get("sourceUrl", "")
    if not source:
        return False
    return site_url.lower() in source.lower()


async def _fetch_recent_scrapes(
    convex_query, site_url: str, limit: int
) -> List[Dict[str, Any]]:
    """Fetch recent scrapes matching the site URL."""
    all_scrapes = await convex_query("router:listScrapes", {"limit": 200})
    if not isinstance(all_scrapes, list):
        return []
    matched = [s for s in all_scrapes if _matches_url(s, site_url)]
    return matched[:limit]


async def _fetch_recent_jobs_for_company(
    convex_query, company: str, limit: int
) -> List[Dict[str, Any]]:
    """Fetch recent jobs for a specific company using listJobsByScrapedAt."""
    # Get jobs from the last 7 days
    seven_days_ago_ms = int((datetime.now(timezone.utc).timestamp() - 7 * 86400) * 1000)
    jobs = await convex_query(
        "jobs:listJobsByScrapedAt",
        {"scrapedAfter": seven_days_ago_ms, "limit": 500},
    )
    if not isinstance(jobs, list):
        return []
    # Filter by company name (case-insensitive partial match)
    matched = []
    company_lower = company.lower()
    for job in jobs:
        job_company = job.get("company", "")
        if isinstance(job_company, str) and company_lower in job_company.lower():
            matched.append(job)
    return matched[:limit]


async def _fetch_url_scrape_logs(
    convex_query, site_url: str, limit: int
) -> List[Dict[str, Any]]:
    """Fetch URL scrape logs with job lookup enabled."""
    logs = await convex_query(
        "router:listUrlScrapeLogs",
        {"limit": 200, "includeJobLookup": True},
    )
    if not isinstance(logs, list):
        return []
    matched = [
        log
        for log in logs
        if site_url.lower() in (log.get("sourceUrl", "") or "").lower()
        or site_url.lower() in (log.get("url", "") or "").lower()
    ]
    return matched[:limit]


async def _fetch_scrape_activity(
    convex_query, site_url: str
) -> List[Dict[str, Any]]:
    """Fetch scrape activity for site."""
    rows = await convex_query("sites:listScrapeActivity", {})
    if not isinstance(rows, list):
        return []
    return [
        r
        for r in rows
        if site_url.lower() in (r.get("url", "") or "").lower()
        or site_url.lower() in (r.get("name", "") or "").lower()
    ]


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check recent scrapes and jobs for a site/company in Convex."
    )
    parser.add_argument("--env", choices=("dev", "prod"), default="prod")
    parser.add_argument(
        "--site-url",
        dest="site_url",
        required=True,
        help="Site URL pattern to search for (partial match).",
    )
    parser.add_argument(
        "--company",
        help="Company name to search for jobs (partial match). Defaults to site_url.",
    )
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--out", help="Optional output JSON path")
    args = parser.parse_args()

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    site_url = args.site_url
    company = args.company or site_url

    print(f"Checking Convex {args.env} for site: {site_url}")
    print(f"Company search: {company}")
    print("-" * 60)

    # Fetch all data in parallel
    scrapes, jobs, logs, activity = await asyncio.gather(
        _fetch_recent_scrapes(convex_query, site_url, args.limit),
        _fetch_recent_jobs_for_company(convex_query, company, args.limit),
        _fetch_url_scrape_logs(convex_query, site_url, args.limit),
        _fetch_scrape_activity(convex_query, site_url),
    )

    result = {
        "site_url": site_url,
        "company": company,
        "env": args.env,
        "summary": {
            "scrapes_found": len(scrapes),
            "jobs_found": len(jobs),
            "url_logs_found": len(logs),
            "activity_entries": len(activity),
        },
        "scrapes": [],
        "jobs": [],
        "url_logs": [],
        "activity": activity,
    }

    # Process scrapes
    for scrape in scrapes:
        result["scrapes"].append(
            {
                "id": str(scrape.get("_id", "")),
                "sourceUrl": scrape.get("sourceUrl"),
                "provider": scrape.get("provider"),
                "workflowName": scrape.get("workflowName"),
                "startedAt": _format_ts(scrape.get("startedAt")),
                "completedAt": _format_ts(scrape.get("completedAt")),
                "type": scrape.get("type"),
                "batchId": scrape.get("batchId"),
            }
        )

    # Process jobs
    for job in jobs:
        result["jobs"].append(
            {
                "id": str(job.get("_id", "")),
                "url": job.get("url"),
                "scrapedAt": _format_ts(job.get("scrapedAt")),
            }
        )

    # Process URL logs
    for log in logs:
        result["url_logs"].append(
            {
                "url": log.get("url"),
                "sourceUrl": log.get("sourceUrl"),
                "jobId": log.get("jobId"),
                "status": log.get("status"),
                "timestamp": _format_ts(log.get("timestamp")),
            }
        )

    # Print summary
    print(f"\nSCRAPES FOUND: {len(scrapes)}")
    if scrapes:
        print("Recent scrapes:")
        for s in result["scrapes"][:5]:
            print(f"  - {s['completedAt']} | {s['provider']} | {s['workflowName']}")

    print(f"\nJOBS FOUND: {len(jobs)}")
    if jobs:
        print("Recent jobs (last 7 days):")
        for j in result["jobs"][:5]:
            print(f"  - {j['scrapedAt']} | {j['url'][:80]}...")

    print(f"\nURL SCRAPE LOGS: {len(logs)}")
    if logs:
        print("Recent URL logs:")
        for log in result["url_logs"][:5]:
            status = log.get("status", "unknown")
            has_job = "YES" if log.get("jobId") else "NO"
            print(f"  - {log['timestamp']} | job={has_job} | {log['url'][:60]}...")

    print(f"\nSCRAPE ACTIVITY: {len(activity)}")
    if activity:
        for act in activity[:3]:
            print(f"  - {act}")

    # Save full output if requested
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nFull output saved to: {args.out}")
    else:
        print("\n" + "-" * 60)
        print("Full JSON output:")
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
