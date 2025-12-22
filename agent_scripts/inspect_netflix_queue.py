#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.workflows.site_handlers import get_site_handler  # noqa: E402

_convex_query = None


def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _parse_statuses(raw: str | None) -> List[str]:
    if not raw:
        return ["pending", "processing"]
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or ["pending", "processing"]


def _matches_filter(url: str | None, *, domain: str | None, needle: str | None) -> bool:
    if not isinstance(url, str) or not url:
        return False
    lowered = url.lower()
    if domain:
        try:
            host = urlparse(url).hostname or ""
        except Exception:
            host = ""
        if host and host.lower().endswith(domain.lower()):
            return True
    if needle and needle.lower() in lowered:
        return True
    return False


def _is_listing_url(url: str) -> bool:
    handler = get_site_handler(url)
    return bool(handler and handler.is_listing_url(url))


async def _list_queue(status: str, provider: str | None, limit: int) -> List[Dict[str, Any]]:
    if _convex_query is None:
        raise RuntimeError("convex_query not initialized; call _load_env before querying Convex")
    args: Dict[str, Any] = {"limit": limit, "status": status}
    if provider:
        args["provider"] = provider
    rows = await _convex_query("router:listQueuedScrapeUrls", args)
    return rows or []


async def _fetch_seen_urls(source_url: str, pattern: str | None) -> List[str]:
    if _convex_query is None:
        raise RuntimeError("convex_query not initialized; call _load_env before querying Convex")
    args: Dict[str, Any] = {"sourceUrl": source_url}
    if pattern is not None:
        args["pattern"] = pattern
    res = await _convex_query("router:listSeenJobUrlsForSite", args)
    urls = res.get("urls", []) if isinstance(res, dict) else []
    return [u for u in urls if isinstance(u, str)]


def _status_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts = Counter()
    for row in rows:
        status = str(row.get("status") or "unknown")
        counts[status] += 1
    return dict(counts)


def _fmt_dt(ms: int | None) -> str:
    if not ms:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect Netflix queue rows + seen URLs to validate listing/pagination dedupe."
    )
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--provider", default="spidercloud")
    parser.add_argument(
        "--statuses",
        default="pending,processing",
        help="comma-separated statuses to query (default: pending,processing)",
    )
    parser.add_argument("--limit", type=int, default=200, help="limit per status query")
    parser.add_argument(
        "--domain",
        default="jobs.netflix.net",
        help="domain suffix filter (default: jobs.netflix.net)",
    )
    parser.add_argument(
        "--needle",
        default="netflix",
        help="substring filter for URL/sourceUrl (default: netflix)",
    )
    parser.add_argument(
        "--include-sites",
        action="store_true",
        help="include matching sites even if they have no queue rows",
    )
    parser.add_argument(
        "--show-rows",
        action="store_true",
        help="include matching queue rows in the output",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=50,
        help="max rows to include when --show-rows is set",
    )
    args = parser.parse_args()

    _load_env(args.env)
    global _convex_query
    from job_scrape_application.services import convex_query as _cq  # noqa: E402

    _convex_query = _cq

    statuses = _parse_statuses(args.statuses)
    all_rows: List[Dict[str, Any]] = []
    for status in statuses:
        rows = await _list_queue(status, args.provider, args.limit)
        for row in rows:
            url = row.get("url")
            source_url = row.get("sourceUrl")
            if _matches_filter(url, domain=args.domain, needle=args.needle) or _matches_filter(
                source_url, domain=args.domain, needle=args.needle
            ):
                all_rows.append(row)

    now_ms = int(time.time() * 1000)
    source_keys: List[Tuple[str, str | None]] = []
    for row in all_rows:
        source_url = row.get("sourceUrl")
        if isinstance(source_url, str) and source_url.strip():
            source_keys.append((source_url, row.get("pattern")))

    sites = await _convex_query("router:listSites", {"enabledOnly": False}) or []
    schedules = await _convex_query("router:listSchedules", {}) or []
    site_map: Dict[str, Dict[str, Any]] = {
        str(site.get("_id")): site for site in sites if isinstance(site, dict) and site.get("_id")
    }
    schedule_map: Dict[str, Dict[str, Any]] = {
        str(sched.get("_id")): sched for sched in schedules if isinstance(sched, dict) and sched.get("_id")
    }

    seen_cache: Dict[Tuple[str, str | None], set[str]] = {}
    for source_url, pattern in sorted(set(source_keys)):
        seen_cache[(source_url, pattern)] = set(await _fetch_seen_urls(source_url, pattern))

    summary_rows: List[Dict[str, Any]] = []
    for source_url, pattern in sorted(set(source_keys)):
        rows = [r for r in all_rows if r.get("sourceUrl") == source_url and r.get("pattern") == pattern]
        seen_urls = seen_cache.get((source_url, pattern), set())
        listing_rows = [r for r in rows if isinstance(r.get("url"), str) and _is_listing_url(r["url"])]
        job_rows = [r for r in rows if r not in listing_rows]

        seen_listing = [r for r in listing_rows if isinstance(r.get("url"), str) and r["url"] in seen_urls]
        seen_jobs = [r for r in job_rows if isinstance(r.get("url"), str) and r["url"] in seen_urls]

        summary_rows.append(
            {
                "sourceUrl": source_url,
                "pattern": pattern,
                "queueCount": len(rows),
                "listingQueued": len(listing_rows),
                "jobQueued": len(job_rows),
                "seenCount": len(seen_urls),
                "seenListingQueued": len(seen_listing),
                "seenJobQueued": len(seen_jobs),
                "statusCounts": _status_counts(rows),
                "samples": {
                    "seenListingUrls": [r.get("url") for r in seen_listing[:5]],
                    "seenJobUrls": [r.get("url") for r in seen_jobs[:5]],
                },
            }
        )

    site_groups: List[Dict[str, Any]] = []
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        site_id = str(row.get("siteId") or "")
        if site_id:
            grouped[site_id].append(row)

    def _site_matches(site: Dict[str, Any]) -> bool:
        url = site.get("url")
        name = site.get("name")
        return _matches_filter(url, domain=args.domain, needle=args.needle) or _matches_filter(
            name, domain=None, needle=args.needle
        )

    site_candidates = [
        site for site in site_map.values() if isinstance(site, dict) and _site_matches(site)
    ]

    site_ids: List[str] = list(grouped.keys())
    if args.include_sites:
        for site in site_candidates:
            site_id = str(site.get("_id") or "")
            if site_id and site_id not in site_ids:
                site_ids.append(site_id)

    for site_id in site_ids:
        rows = grouped.get(site_id, [])
        site = site_map.get(site_id) or {}
        schedule_id = str(site.get("scheduleId") or "")
        schedule = schedule_map.get(schedule_id) if schedule_id else None
        listing_rows = [r for r in rows if isinstance(r.get("url"), str) and _is_listing_url(r["url"])]
        job_rows = [r for r in rows if r not in listing_rows]
        site_groups.append(
            {
                "siteId": site_id,
                "name": site.get("name"),
                "url": site.get("url"),
                "type": site.get("type"),
                "provider": site.get("scrapeProvider") or site.get("provider"),
                "pattern": site.get("pattern"),
                "enabled": site.get("enabled"),
                "completed": site.get("completed"),
                "failed": site.get("failed"),
                "lastRunAt": _fmt_dt(site.get("lastRunAt")),
                "manualTriggerAt": _fmt_dt(site.get("manualTriggerAt")),
                "lockExpiresAt": _fmt_dt(site.get("lockExpiresAt")),
                "schedule": schedule,
                "queueCount": len(rows),
                "listingQueued": len(listing_rows),
                "jobQueued": len(job_rows),
                "statusCounts": _status_counts(rows),
            }
        )

    report: Dict[str, Any] = {
        "meta": {
            "env": args.env,
            "provider": args.provider,
            "statuses": statuses,
            "limitPerStatus": args.limit,
            "domainFilter": args.domain,
            "needleFilter": args.needle,
            "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ms / 1000)),
        },
        "summary": {
            "queueRows": len(all_rows),
            "sources": len(summary_rows),
            "siteGroups": len(site_groups),
            "matchingSites": len(site_candidates),
        },
        "sources": summary_rows,
        "siteGroups": site_groups,
    }

    if args.show_rows:
        trimmed = [
            {
                "url": r.get("url"),
                "sourceUrl": r.get("sourceUrl"),
                "status": r.get("status"),
                "attempts": r.get("attempts"),
                "updatedAt": r.get("updatedAt"),
                "pattern": r.get("pattern"),
                "listing": bool(isinstance(r.get("url"), str) and _is_listing_url(r["url"])),
                "seen": bool(
                    isinstance(r.get("sourceUrl"), str)
                    and (r.get("sourceUrl"), r.get("pattern")) in seen_cache
                    and isinstance(r.get("url"), str)
                    and r["url"] in seen_cache[(r.get("sourceUrl"), r.get("pattern"))]
                ),
            }
            for r in all_rows[: args.max_rows]
        ]
        report["rows"] = trimmed

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
