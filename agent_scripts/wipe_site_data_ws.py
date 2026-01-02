#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_TABLES = ["scrape_url_queue", "seen_job_urls"]


def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _normalize_url(url: str | None) -> Tuple[str, str]:
    if not url:
        return "", ""
    try:
        parsed = urlparse(url)
    except Exception:
        return "", ""
    if not parsed.scheme or not parsed.hostname:
        return "", ""
    domain = parsed.hostname.lower()
    prefix = f"{parsed.scheme}://{parsed.hostname}".lower()
    return domain, prefix


def _matches_site(
    site: Dict[str, Any],
    *,
    site_url: Optional[str],
    company: Optional[str],
    domain: Optional[str],
) -> bool:
    url = site.get("url") if isinstance(site.get("url"), str) else ""
    name = site.get("name") if isinstance(site.get("name"), str) else ""
    if site_url:
        return url == site_url
    if company:
        lowered = company.lower()
        return lowered in name.lower() or lowered in url.lower()
    if domain:
        lowered = domain.lower()
        return lowered in url.lower()
    return False


def _parse_tables(value: Optional[str]) -> List[str]:
    if not value:
        return DEFAULT_TABLES.copy()
    tables = [item.strip() for item in value.split(",")]
    return [table for table in tables if table]


async def _wipe_table(
    convex_mutation,
    *,
    domain: str,
    prefix: str,
    table: str,
    batch_size: int,
    max_pages: int,
    dry_run: bool,
    timeout_seconds: float,
) -> Dict[str, Any]:
    cursor = None
    total_deleted = 0
    total_scanned = 0
    pages = 0
    while pages < max_pages:
        page_start = time.monotonic()
        payload = {
            "domain": domain,
            "prefix": prefix,
            "table": table,
            "batchSize": batch_size,
            "dryRun": dry_run,
        }
        if cursor:
            payload["cursor"] = cursor
        print(
            f"  -> page {pages + 1}/{max_pages} batch={batch_size} cursor={'set' if cursor else 'none'}",
            flush=True,
        )
        try:
            result = await asyncio.wait_for(
                convex_mutation("admin:wipeSiteDataByDomainPage", payload),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            print(
                f"  !! timeout after {timeout_seconds:.1f}s wiping {table} (cursor={'set' if cursor else 'none'})",
                flush=True,
            )
            break
        if not isinstance(result, dict):
            break
        total_deleted += int(result.get("deleted", 0) or 0)
        total_scanned += int(result.get("scanned", 0) or 0)
        cursor = result.get("cursor")
        pages += 1
        elapsed = time.monotonic() - page_start
        print(
            "  <-",
            f"scanned={result.get('scanned', 0)} deleted={result.get('deleted', 0)}",
            f"hasMore={bool(result.get('hasMore'))} elapsed={elapsed:.2f}s",
            flush=True,
        )
        if not result.get("hasMore") or not cursor:
            break
    return {
        "table": table,
        "deleted": total_deleted,
        "scanned": total_scanned,
        "pages": pages,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wipe site data via Convex mutations (no convex CLI dependency)."
    )
    parser.add_argument("--env", choices=("dev", "prod"), default="dev")
    parser.add_argument("--site-url", dest="site_url", help="Exact site URL to match.")
    parser.add_argument("--company", help="Match sites by company name (substring).")
    parser.add_argument("--domain", help="Match sites by domain (substring).")
    parser.add_argument(
        "--tables",
        help="Comma-separated tables to wipe (default: scrape_url_queue,seen_job_urls).",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=25,
        help="Timeout per Convex mutation call.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-now", action="store_true")
    args = parser.parse_args()

    if not args.site_url and not args.company and not args.domain:
        raise SystemExit("Provide --site-url, --company, or --domain.")

    _load_env(args.env)

    from job_scrape_application.services import convex_mutation, convex_query  # noqa: E402

    sites = await convex_query("router:listSites", {"enabledOnly": False})
    if not isinstance(sites, list):
        raise SystemExit("Unexpected response from router:listSites.")

    matched = [
        site
        for site in sites
        if isinstance(site, dict)
        and _matches_site(
            site,
            site_url=args.site_url,
            company=args.company,
            domain=args.domain,
        )
    ]

    if not matched:
        raise SystemExit("No matching sites found.")

    tables = _parse_tables(args.tables)
    if not tables:
        raise SystemExit("No tables provided to wipe.")

    batch_size = max(1, min(args.batch_size, 500))
    max_pages = max(1, min(args.max_pages, 200))

    for site in matched:
        site_id = site.get("_id")
        site_url = site.get("url")
        if not isinstance(site_id, str) or not isinstance(site_url, str):
            continue
        domain, prefix = _normalize_url(site_url)
        if not domain or not prefix:
            print(f"Skipping invalid site URL: {site_url}")
            continue

        print(f"Wiping {site.get('name') or site_url} ({domain})")
        for table in tables:
            result = await _wipe_table(
                convex_mutation,
                domain=domain,
                prefix=prefix,
                table=table,
                batch_size=batch_size,
                max_pages=max_pages,
                dry_run=args.dry_run,
                timeout_seconds=args.timeout_seconds,
            )
            print(
                "  ",
                f"{result['table']}: deleted={result['deleted']} scanned={result['scanned']} pages={result['pages']}",
            )

        if args.run_now:
            await convex_mutation("router:runSiteNow", {"id": site_id})
            print("  runSiteNow triggered")


if __name__ == "__main__":
    asyncio.run(main())
