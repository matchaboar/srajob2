from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
DEFAULT_DOMAIN = "bloomberg.avature.net"
TABLES: Tuple[str, ...] = (
    "jobs",
    "scrapes",
    "scrape_url_queue",
    "ignored_jobs",
    "scrape_errors",
    "run_requests",
    "workflow_run_sites",
    "scratchpad_entries",
)


def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _run_convex(
    args: List[str],
    *,
    env: Dict[str, str],
) -> Any:
    result = subprocess.run(
        args,
        cwd=str(CONVEX_DIR),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = result.stdout.strip()
    if not stdout:
        return None
    return json.loads(stdout)


def _build_convex_run_args(env: str, function_name: str, payload: Dict[str, Any]) -> List[str]:
    cmd = ["npx", "convex", "run"]
    if env == "prod":
        cmd.append("--prod")
    cmd.append(function_name)
    cmd.append(json.dumps(payload))
    return cmd


def _find_bloomberg_sites(sites: List[Dict[str, Any]], domain: str) -> List[Dict[str, Any]]:
    lowered = domain.lower()
    matched = []
    for site in sites:
        url = site.get("url")
        if isinstance(url, str) and lowered in url.lower():
            matched.append(site)
    return matched


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wipe Bloomberg Avature data from Convex and trigger a run-now scrape."
    )
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--domain", default=DEFAULT_DOMAIN)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=50)
    args = parser.parse_args()

    _load_env(args.env)
    env = os.environ.copy()

    wipe_results: Dict[str, Any] = {}
    for table in TABLES:
        cursor = None
        pages = 0
        total_deleted = 0
        total_scanned = 0
        last_cursor = None
        last_has_more = False
        while pages < args.max_pages:
            wipe_payload = {
                "domain": args.domain,
                "table": table,
                "dryRun": args.dry_run,
                "batchSize": args.batch_size,
            }
            if cursor:
                wipe_payload["cursor"] = cursor
            wipe_cmd = _build_convex_run_args(args.env, "admin:wipeSiteDataByDomainPage", wipe_payload)
            wipe_result = _run_convex(wipe_cmd, env=env)
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
        wipe_results[table] = {
            "deleted": total_deleted,
            "scanned": total_scanned,
            "pages": pages,
            "hasMore": last_has_more,
            "cursor": last_cursor,
        }
    print(json.dumps({"wipe": wipe_results}, indent=2))

    sites_cmd = _build_convex_run_args(args.env, "router:listSites", {"enabledOnly": False})
    sites_result = _run_convex(sites_cmd, env=env)
    matched_sites = _find_bloomberg_sites(sites_result or [], args.domain)
    if not matched_sites:
        print(f"No sites matched {args.domain}; skipping run-now trigger.")
        return

    triggered: List[Dict[str, Any]] = []
    for site in matched_sites:
        site_id = site.get("_id")
        if not site_id:
            continue
        run_cmd = _build_convex_run_args(args.env, "router:runSiteNow", {"id": site_id})
        run_result = _run_convex(run_cmd, env=env)
        triggered.append({"id": site_id, "url": site.get("url"), "result": run_result})

    print(json.dumps({"runNow": triggered}, indent=2))


if __name__ == "__main__":
    main()
