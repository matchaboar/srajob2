#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

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


def _normalize(value: str) -> str:
    return value.strip().lower()


def _matches_any(text: str, needles: Iterable[str]) -> bool:
    lowered = _normalize(text)
    return any(needle in lowered for needle in needles)


def _company_tokens(companies: Iterable[str]) -> List[str]:
    tokens: List[str] = []
    for company in companies:
        cleaned = _normalize(company)
        if cleaned:
            tokens.append(cleaned)
    return tokens


def _row_matches_company(row: Dict[str, Any], tokens: List[str]) -> bool:
    company = row.get("company") or ""
    url = row.get("url") or ""
    source_url = row.get("sourceUrl") or ""
    return any(
        _matches_any(value, tokens)
        for value in (company, url, source_url)
        if isinstance(value, str)
    )


def _chunk(items: List[Any], size: int) -> Iterable[List[Any]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _site_matches(company: str, site: Dict[str, Any]) -> bool:
    token = _normalize(company)
    for key in ("name", "url", "pattern", "type", "scrapeProvider"):
        value = site.get(key)
        if isinstance(value, str) and token in value.lower():
            return True
    return False


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clear ignored/seen job URLs for ignored rows and trigger rescrape."
    )
    parser.add_argument(
        "companies",
        nargs="+",
        help="Company names to match (case-insensitive).",
    )
    parser.add_argument(
        "--env",
        default="prod",
        choices=("dev", "prod"),
        help="Convex environment to query (default: prod).",
    )
    parser.add_argument(
        "--ignored-limit",
        type=int,
        default=400,
        help="Max ignored_jobs rows to fetch (default: 400).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Batch size for delete mutations (default: 100).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be deleted and rescraped.",
    )
    args = parser.parse_args()

    tokens = _company_tokens(args.companies)
    if not tokens:
        raise SystemExit("No company tokens provided.")

    _load_env(args.env)
    from job_scrape_application.services import convex_query, convex_mutation  # noqa: E402

    ignored_rows = await convex_query("router:listIgnoredJobs", {"limit": args.ignored_limit})
    if not isinstance(ignored_rows, list):
        ignored_rows = []

    matched_rows = [row for row in ignored_rows if _row_matches_company(row, tokens)]
    ids = [row.get("_id") for row in matched_rows if isinstance(row.get("_id"), str)]
    seen_entries = [
        {"sourceUrl": row.get("sourceUrl", ""), "url": row.get("url", "")}
        for row in matched_rows
        if isinstance(row.get("sourceUrl"), str) and isinstance(row.get("url"), str)
    ]

    summary: Dict[str, Any] = {
        "env": args.env,
        "companies": args.companies,
        "ignored": {
            "matched": len(matched_rows),
            "ids": len(ids),
            "urls": [row.get("url") for row in matched_rows],
        },
    }

    if args.dry_run:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    deleted_ignored = 0
    for chunk in _chunk(ids, args.chunk_size):
        if not chunk:
            continue
        res = await convex_mutation("router:deleteIgnoredJobsByIds", {"ids": chunk})
        if isinstance(res, dict):
            deleted_ignored += int(res.get("deleted", 0) or 0)

    deleted_seen = 0
    for chunk in _chunk(seen_entries, args.chunk_size):
        if not chunk:
            continue
        res = await convex_mutation("router:deleteSeenJobUrls", {"entries": chunk})
        if isinstance(res, dict):
            deleted_seen += int(res.get("deleted", 0) or 0)

    sites = await convex_query("router:listSites", {"enabledOnly": False})
    if not isinstance(sites, list):
        sites = []

    matched_sites: Dict[str, Dict[str, Any]] = {}
    for company in args.companies:
        for site in sites:
            if not isinstance(site, dict):
                continue
            if _site_matches(company, site):
                site_id = site.get("_id")
                if isinstance(site_id, str):
                    matched_sites[site_id] = site

    rescrape_counts = defaultdict(int)
    for site_id, site in matched_sites.items():
        await convex_mutation("router:runSiteNow", {"id": site_id})
        rescrape_counts[site.get("name") or site.get("url") or site_id] += 1

    summary.update(
        {
            "deleted": {
                "ignored": deleted_ignored,
                "seen": deleted_seen,
            },
            "rescrape": {
                "sites": len(matched_sites),
                "siteIds": list(matched_sites.keys()),
            },
        }
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
