#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"


from job_scrape_application.dbos_runtime import queue as dbos_queue  # noqa: E402

def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)
def _normalize(value: str) -> str:
    return value.strip().lower()
def _site_matches(company: str, site: Dict[str, Any]) -> bool:
    token = _normalize(company)
    for key in ("name", "url", "pattern", "type", "scrapeProvider"):
        value = site.get(key)
        if isinstance(value, str) and token in value.lower():
            return True
    return False
def _extract_url_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for row in rows:
        status = row.get("status") or "unknown"
        counts[str(status)] += 1
    return dict(counts)
async def _safe_query(convex_query, name: str, args: Dict[str, Any], timeout: int) -> Optional[Any]:
    try:
        return await asyncio.wait_for(convex_query(name, args), timeout=timeout)
    except asyncio.TimeoutError:
        return {"_error": f"timeout_after_{timeout}s"}
    except Exception as exc:  # noqa: BLE001
        return {"_error": f"{type(exc).__name__}: {exc}"}
async def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize DBOS queue rows for sites.")
    parser.add_argument("companies", nargs="+", help="Company names to match against sites.")
    parser.add_argument(
        "--env",
        default="prod",
        choices=("dev", "prod"),
        help="Convex environment to query (default: prod).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max queue rows per site (default: 500).",
    )
    parser.add_argument(
        "--timeout-secs",
        type=int,
        default=20,
        help="Timeout in seconds per Convex query (default: 20).",
    )
    args = parser.parse_args()

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    sites = await _safe_query(
        convex_query,
        "router:listSites",
        {"enabledOnly": False},
        args.timeout_secs,
    )
    if not isinstance(sites, list):
        print(
            json.dumps(
                {
                    "env": args.env,
                    "companies": args.companies,
                    "error": "failed to fetch sites",
                    "details": sites,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    matched_sites = []
    for site in sites:
        if not isinstance(site, dict):
            continue
        if any(_site_matches(company, site) for company in args.companies):
            matched_sites.append(site)

    summary: Dict[str, Any] = {
        "env": args.env,
        "companies": args.companies,
        "sites": [],
    }

    for site in matched_sites:
        site_id = site.get("_id")
        if not isinstance(site_id, str):
            continue
        rows = dbos_queue.list_scrape_urls(site_id=site_id, limit=args.limit) or []
        if not isinstance(rows, list):
            rows = []
        counts = _extract_url_counts(rows)
        summary["sites"].append(
            {
                "id": site_id,
                "name": site.get("name"),
                "url": site.get("url"),
                "type": site.get("type"),
                "queueRows": len(rows),
                "statusCounts": counts,
            }
        )

    print(json.dumps(summary, ensure_ascii=False, indent=2))
if __name__ == "__main__":
    asyncio.run(main())
