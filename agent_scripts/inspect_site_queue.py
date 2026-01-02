#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

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


def _normalize_domain(url: str | None) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return ""
    return (parsed.hostname or "").lower()


def _matches_site(site: Dict[str, Any], *, site_url: Optional[str], name: Optional[str]) -> bool:
    if site_url and isinstance(site.get("url"), str):
        return site.get("url") == site_url
    if name and isinstance(site.get("name"), str):
        return site.get("name", "").strip().lower() == name.strip().lower()
    return False


async def _fetch_sites(convex_query) -> List[Dict[str, Any]]:
    rows = await convex_query("router:listSites", {"enabledOnly": False})
    return rows if isinstance(rows, list) else []


async def _fetch_scrape_activity(convex_query) -> List[Dict[str, Any]]:
    rows = await convex_query("sites:listScrapeActivity", {})
    return rows if isinstance(rows, list) else []


async def _fetch_queue_rows(convex_query, site_id: str, status: str, limit: int) -> List[Dict[str, Any]]:
    args: Dict[str, Any] = {"siteId": site_id, "status": status, "limit": limit}
    rows = await convex_query("router:listQueuedScrapeUrls", args)
    return rows if isinstance(rows, list) else []


async def _fetch_seen_urls(convex_query, source_url: str, pattern: Optional[str]) -> List[str]:
    payload: Dict[str, Any] = {"sourceUrl": source_url}
    if pattern is not None:
        payload["pattern"] = pattern
    res = await convex_query("router:listSeenJobUrlsForSite", payload)
    urls = res.get("urls", []) if isinstance(res, dict) else []
    return [u for u in urls if isinstance(u, str)]


async def _fetch_ignored_jobs(convex_query, limit: int) -> List[Dict[str, Any]]:
    rows = await convex_query("router:listIgnoredJobs", {"limit": limit})
    return rows if isinstance(rows, list) else []


async def _fetch_scrape_logs(convex_query, limit: int) -> List[Dict[str, Any]]:
    rows = await convex_query("router:listUrlScrapeLogs", {"limit": limit})
    return rows if isinstance(rows, list) else []


def _summarize_queue(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    errors: Dict[str, int] = {}
    urls: List[str] = []
    for row in rows:
        err = row.get("lastError")
        if isinstance(err, str) and err.strip():
            errors[err] = errors.get(err, 0) + 1
        url = row.get("url")
        if isinstance(url, str) and len(urls) < 5:
            urls.append(url)
    return {
        "count": len(rows),
        "errors": errors,
        "sampleUrls": urls,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect Convex queue + activity for a specific site."
    )
    parser.add_argument("--env", choices=("dev", "prod"), default="prod")
    parser.add_argument("--site-url", dest="site_url", help="Exact site URL to match.")
    parser.add_argument("--name", help="Site name to match (case-insensitive).")
    parser.add_argument(
        "--queue-limit",
        type=int,
        default=200,
        help="Max rows to fetch per queue status (Convex cap is 500).",
    )
    parser.add_argument(
        "--ignored-limit",
        type=int,
        default=400,
        help="Max ignored_jobs rows to fetch for sampling.",
    )
    parser.add_argument(
        "--scrape-log-limit",
        type=int,
        default=200,
        help="Max scrape log rows to fetch for sampling.",
    )
    args = parser.parse_args()

    if not args.site_url and not args.name:
        raise SystemExit("Provide --site-url or --name to filter.")

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    sites = await _fetch_sites(convex_query)
    matched = [site for site in sites if _matches_site(site, site_url=args.site_url, name=args.name)]
    if not matched:
        raise SystemExit("No matching sites found.")

    activity_rows = await _fetch_scrape_activity(convex_query)
    activity_by_id = {row.get("siteId"): row for row in activity_rows if isinstance(row, dict)}

    ignored_rows = await _fetch_ignored_jobs(convex_query, args.ignored_limit)
    scrape_logs = await _fetch_scrape_logs(convex_query, args.scrape_log_limit)

    results: List[Dict[str, Any]] = []
    for site in matched:
        site_id = site.get("_id")
        site_url = site.get("url")
        if not isinstance(site_id, str) or not isinstance(site_url, str):
            continue
        pattern = site.get("pattern") if isinstance(site.get("pattern"), str) else None
        domain = _normalize_domain(site_url)

        queue_summary: Dict[str, Any] = {}
        for status in ("pending", "processing", "completed", "failed", "invalid"):
            rows = await _fetch_queue_rows(convex_query, site_id, status, args.queue_limit)
            summary = _summarize_queue(rows)
            summary["limit"] = args.queue_limit
            queue_summary[status] = summary

        seen_urls = await _fetch_seen_urls(convex_query, site_url, pattern)

        ignored_for_site = [
            row
            for row in ignored_rows
            if isinstance(row.get("sourceUrl"), str) and domain and domain in row.get("sourceUrl", "").lower()
        ]
        ignored_reasons: Dict[str, int] = {}
        for row in ignored_for_site:
            reason = row.get("reason") or "unknown"
            if isinstance(reason, str):
                ignored_reasons[reason] = ignored_reasons.get(reason, 0) + 1

        logs_for_site = [
            row
            for row in scrape_logs
            if isinstance(row.get("sourceUrl"), str) and domain and domain in row.get("sourceUrl", "").lower()
        ][:5]

        activity = activity_by_id.get(site_id)

        results.append(
            {
                "site": {
                    "id": site_id,
                    "name": site.get("name"),
                    "url": site_url,
                    "pattern": pattern,
                    "type": site.get("type"),
                    "provider": site.get("scrapeProvider"),
                    "enabled": site.get("enabled"),
                    "failed": site.get("failed"),
                    "lastRunAt": site.get("lastRunAt"),
                    "completed": site.get("completed"),
                },
                "activity": activity,
                "queue": queue_summary,
                "seenJobUrls": {"count": len(seen_urls), "sample": seen_urls[:5]},
                "ignoredJobs": {
                    "sampledCount": len(ignored_for_site),
                    "sampleLimit": args.ignored_limit,
                    "reasons": ignored_reasons,
                },
                "scrapeLogsSample": logs_for_site,
            }
        )

    print(json.dumps({"count": len(results), "results": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
