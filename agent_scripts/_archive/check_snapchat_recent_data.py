from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
DEFAULT_DOMAINS: Tuple[str, ...] = ("snap.com", "snapchat.com")
TABLES: Tuple[str, ...] = ("seen_job_urls", "ignored_jobs")

def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)
def _ensure_env(target_env: str) -> None:
    _load_env(target_env)
    if not os.getenv("CONVEX_URL") and not os.getenv("CONVEX_HTTP_URL"):
        raise RuntimeError("CONVEX_URL (or CONVEX_HTTP_URL) is required to access Convex")
def _normalize_patterns(domains: Iterable[str], companies: Iterable[str]) -> List[str]:
    patterns: List[str] = []
    for value in list(domains) + list(companies):
        cleaned = (value or "").strip().lower()
        if not cleaned:
            continue
        patterns.append(cleaned)
    return sorted(set(patterns))
def _matches_any(value: str, patterns: Iterable[str]) -> bool:
    lowered = (value or "").lower()
    return any(pattern in lowered for pattern in patterns)
def _find_sites(sites: List[Dict[str, Any]], patterns: Iterable[str]) -> List[Dict[str, Any]]:
    matched: List[Dict[str, Any]] = []
    for site in sites:
        url = site.get("url") or ""
        name = site.get("name") or ""
        if _matches_any(str(url), patterns) or _matches_any(str(name), patterns):
            matched.append(site)
    return matched
def _site_wipe_targets(sites: List[Dict[str, Any]], *, host_wide: bool) -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()
    for site in sites:
        url = site.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.hostname:
            continue
        host = parsed.hostname.lower()
        base_prefix = f"{parsed.scheme}://{host}"
        path = (parsed.path or "").rstrip("/")
        if host_wide or not path or path == "/":
            prefix = base_prefix
            domain = host
        else:
            prefix = f"{base_prefix}{path}"
            domain = f"{host}{path}"
        key = (domain, prefix)
        if key in seen:
            continue
        seen.add(key)
        targets.append({"domain": domain, "prefix": prefix})
    return targets
def _within_cutoff(value: Any, cutoff_ms: int) -> bool:
    return isinstance(value, (int, float)) and int(value) >= cutoff_ms
def _collect_recent_ignored(
    rows: List[Dict[str, Any]], patterns: Iterable[str], cutoff_ms: int
) -> List[Dict[str, Any]]:
    recent: List[Dict[str, Any]] = []
    for row in rows:
        if not _within_cutoff(row.get("createdAt"), cutoff_ms):
            continue
        url = str(row.get("url") or "")
        source = str(row.get("sourceUrl") or "")
        company = str(row.get("company") or "")
        if _matches_any(url, patterns) or _matches_any(source, patterns) or _matches_any(company, patterns):
            recent.append(row)
    return recent
async def _run() -> None:
    parser = argparse.ArgumentParser(
        description="Check recent Snapchat ignored/seen entries in prod and wipe if requested."
    )
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--domain", action="append", default=[])
    parser.add_argument("--company", action="append", default=[])
    parser.add_argument("--ignored-limit", type=int, default=400)
    parser.add_argument("--apply", action="store_true", help="Apply wipe (default is dry-run)")
    parser.add_argument("--host-wide", action="store_true", help="Wipe by host (default is path-scoped)")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=50)
    args = parser.parse_args()

    patterns = _normalize_patterns(args.domain or DEFAULT_DOMAINS, args.company or [])
    _ensure_env(args.env)
    from job_scrape_application.services import convex_mutation, convex_query

    sites_result = await convex_query("router:listSites", {"enabledOnly": False}) or []
    matched_sites = _find_sites(sites_result, patterns)

    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - int(args.hours * 60 * 60 * 1000)

    summary: Dict[str, Any] = {
        "env": args.env,
        "cutoffMs": cutoff_ms,
        "cutoffIso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ms / 1000)),
        "matchedSites": [],
    }

    any_recent = False
    for site in matched_sites:
        site_id = site.get("_id")
        site_url = site.get("url") or ""
        site_summary: Dict[str, Any] = {"id": site_id, "url": site_url}

        ignored_rows = await convex_query("router:listIgnoredJobs", {"limit": args.ignored_limit}) or []
        recent_ignored = _collect_recent_ignored(ignored_rows, patterns, cutoff_ms)
        site_summary["ignoredRecent"] = len(recent_ignored)

        seen_result = await convex_query(
            "router:listSeenJobUrlsForSite",
            {"sourceUrl": site_url},
        ) or {}
        seen_urls = seen_result.get("urls") if isinstance(seen_result, dict) else []
        site_summary["seenTotal"] = len(seen_urls) if isinstance(seen_urls, list) else 0

        summary["matchedSites"].append(site_summary)

        if recent_ignored:
            any_recent = True

    print(json.dumps(summary, indent=2))

    if not matched_sites:
        print("No matching Snapchat sites found; nothing to wipe.")
        return

    if not any_recent:
        print("No recent Snapchat ignored entries found within the cutoff window.")
        return

    wipe_targets = _site_wipe_targets(matched_sites, host_wide=args.host_wide)
    wipe_results: Dict[str, Any] = {}

    for target in wipe_targets:
        domain = target["domain"]
        prefix = target["prefix"]
        wipe_results.setdefault(domain, {})
        for table in TABLES:
            cursor = None
            pages = 0
            total_deleted = 0
            total_scanned = 0
            last_cursor = None
            last_has_more = False
            while pages < args.max_pages:
                payload = {
                    "domain": domain,
                    "prefix": prefix,
                    "table": table,
                    "dryRun": not args.apply,
                    "batchSize": args.batch_size,
                }
                if cursor:
                    payload["cursor"] = cursor
                wipe_result = await convex_mutation("admin:wipeSiteDataByDomainPage", payload)
                if not isinstance(wipe_result, dict):
                    break
                total_deleted += int(wipe_result.get("deleted", 0) or 0)
                total_scanned += int(wipe_result.get("scanned", 0) or 0)
                pages += 1
                cursor = wipe_result.get("cursor")
                last_cursor = cursor
                last_has_more = bool(wipe_result.get("hasMore"))
                if not last_has_more:
                    break
            wipe_results[domain][table] = {
                "prefix": prefix,
                "deleted": total_deleted,
                "scanned": total_scanned,
                "pages": pages,
                "hasMore": last_has_more,
                "cursor": last_cursor,
            }

    print(json.dumps({"wipe": wipe_results, "applied": args.apply}, indent=2))
def main() -> None:
    asyncio.run(_run())
if __name__ == "__main__":
    main()
