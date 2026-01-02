#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
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


def _matches(row: Dict[str, Any], site_url: Optional[str], name: Optional[str]) -> bool:
    if site_url and row.get("url") == site_url:
        return True
    if name and isinstance(row.get("name"), str) and row.get("name").lower() == name.lower():
        return True
    return False


async def _fetch_scrape_activity(convex_query) -> List[Dict[str, Any]]:
    rows = await convex_query("sites:listScrapeActivity", {})
    return rows if isinstance(rows, list) else []


async def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Convex site scrape activity.")
    parser.add_argument("--env", choices=("dev", "prod"), default="prod")
    parser.add_argument("--site-url", dest="site_url", help="Exact site URL to match.")
    parser.add_argument("--name", help="Site name to match (case-insensitive).")
    args = parser.parse_args()

    if not args.site_url and not args.name:
        raise SystemExit("Provide --site-url or --name to filter.")

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    rows = await _fetch_scrape_activity(convex_query)
    matched = [row for row in rows if _matches(row, args.site_url, args.name)]

    print(json.dumps({"count": len(matched), "rows": matched}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
