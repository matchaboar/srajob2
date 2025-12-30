#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict, List
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.config import get_config_env  # noqa: E402
from job_scrape_application.services import convex_mutation, convex_query  # noqa: E402


def _matches_url(site: Dict[str, Any], needle: str) -> bool:
    url = site.get("url")
    if not isinstance(url, str):
        return False
    return needle.lower() in url.lower()


async def _delete_sites(url_substring: str) -> List[Dict[str, Any]]:
    sites = await convex_query("router:listSites", {"enabledOnly": False}) or []
    deleted: List[Dict[str, Any]] = []
    for site in sites:
        if not isinstance(site, dict):
            continue
        if not _matches_url(site, url_substring):
            continue
        site_id = site.get("_id")
        if not site_id:
            continue
        result = await convex_mutation("router:deleteSite", {"id": site_id})
        deleted.append(
            {
                "id": site_id,
                "url": site.get("url"),
                "result": result,
            }
        )
    return deleted


async def main() -> None:
    parser = argparse.ArgumentParser(description="Delete Convex sites whose URLs match a substring.")
    parser.add_argument(
        "--env",
        default=get_config_env(),
        choices=["dev", "prod"],
        help="Convex environment to target (dev or prod).",
    )
    parser.add_argument(
        "--url-substring",
        default="example.com",
        help="Substring to match in site URLs (case-insensitive).",
    )
    args = parser.parse_args()

    deleted = await _delete_sites(args.url_substring)
    if not deleted:
        print(f"No sites matched '{args.url_substring}'.")
        return
    for row in deleted:
        print(f"Deleted site id={row['id']} url={row.get('url')}")
    print(f"Deleted {len(deleted)} site(s).")


if __name__ == "__main__":
    asyncio.run(main())
