#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.services import convex_query  # noqa: E402


def _parse_statuses(raw: str | None) -> List[str]:
    if not raw:
        return ["pending", "processing"]
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or ["pending", "processing"]


def _unique_rows(rows: Iterable[Dict[str, Any]], *, max_rows: int) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        url = row.get("url")
        if not isinstance(url, str) or not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        unique.append(row)
        if len(unique) >= max_rows:
            break
    return unique


async def _fetch_queue_rows(provider: str | None, status: str | None, limit: int) -> List[Dict[str, Any]]:
    args: Dict[str, Any] = {"limit": limit}
    if provider:
        args["provider"] = provider
    if status:
        args["status"] = status
    rows = await convex_query("router:listQueuedScrapeUrls", args)
    return rows or []


async def _fetch_sites() -> List[Dict[str, Any]]:
    rows = await convex_query("router:listSites", {"enabledOnly": False})
    return rows or []


async def _fetch_seen_for_source(source_url: str, pattern: str | None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"sourceUrl": source_url}
    if pattern is not None:
        payload["pattern"] = pattern
    res = await convex_query("router:listSeenJobUrlsForSite", payload)
    urls = res.get("urls", []) if isinstance(res, dict) else []
    return {
        "sourceUrl": source_url,
        "pattern": pattern,
        "urls": [u for u in urls if isinstance(u, str)],
    }


async def _fetch_ignored_jobs(limit: int) -> List[Dict[str, Any]]:
    rows = await convex_query("router:listIgnoredJobs", {"limit": limit})
    return rows or []


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Convex queue-related state into a fixture JSON."
    )
    parser.add_argument("--provider", default="spidercloud", help="scrape_url_queue provider filter")
    parser.add_argument(
        "--statuses",
        default="pending,processing",
        help="comma-separated status list (default: pending,processing)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="max total queue rows to export",
    )
    parser.add_argument(
        "--per-status-limit",
        type=int,
        default=500,
        help="max rows to fetch per status (Convex cap is 500)",
    )
    parser.add_argument(
        "--ignored-limit",
        type=int,
        default=400,
        help="max ignored_jobs rows to export",
    )
    parser.add_argument(
        "--output",
        default="tests/job_scrape_application/workflows/fixtures/convex_prod_queue_state.json",
    )
    parser.add_argument("--env", default=None, help="optional env label to include in metadata")
    args = parser.parse_args()

    statuses = _parse_statuses(args.statuses)
    collected: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for status in statuses:
        rows = await _fetch_queue_rows(args.provider, status, args.per_status_limit)
        for row in rows:
            url = row.get("url")
            if not isinstance(url, str) or not url:
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            collected.append(row)
            if len(collected) >= args.limit:
                break
        if len(collected) >= args.limit:
            break

    unique_rows = _unique_rows(collected, max_rows=args.limit)

    sites = await _fetch_sites()
    site_by_id: Dict[str, Dict[str, Any]] = {}
    site_by_url: Dict[str, Dict[str, Any]] = {}
    for site in sites:
        site_id = site.get("_id")
        if isinstance(site_id, str):
            site_by_id[site_id] = site
        site_url = site.get("url")
        if isinstance(site_url, str) and site_url:
            site_by_url[site_url] = site

    sources: Dict[tuple[str, str | None], None] = {}
    for row in unique_rows:
        source_url = row.get("sourceUrl")
        if not isinstance(source_url, str) or not source_url:
            continue
        pattern = None
        site_id = row.get("siteId")
        if isinstance(site_id, str) and site_id in site_by_id:
            pattern = site_by_id[site_id].get("pattern")
        if pattern is None:
            site_match = site_by_url.get(source_url)
            if site_match is not None:
                pattern = site_match.get("pattern")
        sources[(source_url, pattern if isinstance(pattern, str) else None)] = None

    seen_by_source: List[Dict[str, Any]] = []
    for (source_url, pattern) in sources.keys():
        try:
            seen_by_source.append(await _fetch_seen_for_source(source_url, pattern))
        except Exception:
            seen_by_source.append({"sourceUrl": source_url, "pattern": pattern, "urls": []})

    ignored_jobs = await _fetch_ignored_jobs(args.ignored_limit)

    now_ms = int(time.time() * 1000)
    payload = {
        "meta": {
            "provider": args.provider,
            "statuses": statuses,
            "limit": args.limit,
            "count": len(unique_rows),
            "generatedAtMs": now_ms,
            "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ms / 1000)),
            "env": args.env,
        },
        "tables": {
            "sites": sites,
            "scrape_url_queue": unique_rows,
            "seen_job_urls": seen_by_source,
            "ignored_jobs": ignored_jobs,
        },
    }

    output_path = Path(args.output)
    _write_json(output_path, payload)

    summary = {
        "output": str(output_path),
        "queue_rows": len(unique_rows),
        "sites": len(sites),
        "seen_sources": len(seen_by_source),
        "ignored_jobs": len(ignored_jobs),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
