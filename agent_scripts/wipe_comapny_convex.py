from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
DEFAULT_DOMAIN = "bloomberg.avature.net"
GREENHOUSE_API_HOST = "api.greenhouse.io"
GREENHOUSE_BOARD_HOST = "boards.greenhouse.io"
GREENHOUSE_BOARD_TABLES: Tuple[str, ...] = (
    "jobs",
    "scrapes",
    "scrape_activity",
    "scrape_url_queue",
    "seen_job_urls",
    "ignored_jobs",
)
TABLES: Tuple[str, ...] = (
    "jobs",
    "scrapes",
    "scrape_activity",
    "scrape_url_queue",
    "seen_job_urls",
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
    try:
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
    except subprocess.CalledProcessError as e:
        print(f"Error running convex command: {' '.join(args)}")
        if e.stderr:
            print(f"Error: {e.stderr.strip()}")
        return None
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON output from convex command: {e}")
        return None


def _build_convex_run_args(env: str, function_name: str, payload: Dict[str, Any]) -> List[str]:
    cmd = ["npx", "convex", "run"]
    if env == "prod":
        cmd.append("--prod")
    cmd.append(function_name)
    cmd.append(json.dumps(payload))
    return cmd


def _find_sites_by_domain(sites: List[Dict[str, Any]], domain: str) -> List[Dict[str, Any]]:
    lowered = domain.lower()
    matched = []
    for site in sites:
        url = site.get("url")
        if isinstance(url, str) and lowered in url.lower():
            matched.append(site)
    return matched


def _find_sites_by_company(sites: List[Dict[str, Any]], company: str) -> List[Dict[str, Any]]:
    lowered = company.lower()
    matched = []
    for site in sites:
        name = site.get("name")
        url = site.get("url")
        if isinstance(name, str) and lowered in name.lower():
            matched.append(site)
            continue
        if isinstance(url, str) and lowered in url.lower():
            matched.append(site)
    return matched


def _extract_greenhouse_board_slug(parsed: Any) -> str | None:
    if not parsed or not parsed.hostname:
        return None
    if parsed.hostname.lower() != GREENHOUSE_API_HOST:
        return None
    path = (parsed.path or "").strip("/")
    parts = [part for part in path.split("/") if part]
    if len(parts) >= 3 and parts[0] == "v1" and parts[1] == "boards":
        return parts[2]
    return None


def _site_wipe_targets(sites: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, Tuple[str, ...]]] = set()
    for site in sites:
        url = site.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.hostname:
            continue
        host = parsed.hostname.lower()
        prefix = f"{parsed.scheme}://{host}"
        key = (host, prefix, TABLES)
        if key not in seen:
            seen.add(key)
            targets.append({"domain": host, "prefix": prefix, "tables": TABLES})

        board_slug = _extract_greenhouse_board_slug(parsed)
        if board_slug:
            board_prefix = f"https://{GREENHOUSE_BOARD_HOST}/{board_slug}"
            board_key = (GREENHOUSE_BOARD_HOST, board_prefix, GREENHOUSE_BOARD_TABLES)
            if board_key not in seen:
                seen.add(board_key)
                targets.append(
                    {
                        "domain": GREENHOUSE_BOARD_HOST,
                        "prefix": board_prefix,
                        "tables": GREENHOUSE_BOARD_TABLES,
                    }
                )
    return targets


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wipe site data from Convex (by company or domain) and trigger run-now."
    )
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--domain", help="Match sites by domain (e.g. bloomberg.avature.net)")
    parser.add_argument("--company", help="Match sites by company name (substring, case-insensitive)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-run", action="store_true", help="Skip triggering runSiteNow after wipe")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=50)
    args = parser.parse_args()

    if not args.domain and not args.company:
        args.domain = DEFAULT_DOMAIN

    _load_env(args.env)
    env = os.environ.copy()

    sites_cmd = _build_convex_run_args(args.env, "router:listSites", {"enabledOnly": False})
    sites_result = _run_convex(sites_cmd, env=env)
    matched_sites: List[Dict[str, Any]]
    if args.company:
        matched_sites = _find_sites_by_company(sites_result or [], args.company)
    else:
        matched_sites = _find_sites_by_domain(sites_result or [], args.domain or DEFAULT_DOMAIN)
    wipe_targets = _site_wipe_targets(matched_sites)
    wipe_results: Dict[str, Any] = {}
    if not wipe_targets:
        print("No matching sites found for wipe; skipping.")
    else:
        for target in wipe_targets:
            host = target["domain"]
            prefix = target["prefix"]
            tables = target["tables"]
            wipe_results.setdefault(host, {})
            for table in tables:
                cursor = None
                pages = 0
                total_deleted = 0
                total_scanned = 0
                last_cursor = None
                last_has_more = False
                while pages < args.max_pages:
                    wipe_payload = {
                        "domain": host,
                        "prefix": prefix,
                        "table": table,
                        "dryRun": args.dry_run,
                        "batchSize": args.batch_size,
                    }
                    if cursor:
                        wipe_payload["cursor"] = cursor
                    wipe_cmd = _build_convex_run_args(
                        args.env,
                        "admin:wipeSiteDataByDomainPage",
                        wipe_payload,
                    )
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
                wipe_results[host][table] = {
                    "deleted": total_deleted,
                    "scanned": total_scanned,
                    "pages": pages,
                    "hasMore": last_has_more,
                    "cursor": last_cursor,
                    "prefix": prefix,
                }
        print(json.dumps({"wipe": wipe_results}, indent=2))

    if not matched_sites:
        label = args.company or args.domain or DEFAULT_DOMAIN
        print(f"No sites matched {label}; skipping run-now trigger.")
        return

    if args.skip_run:
        print(json.dumps({"runNow": []}, indent=2))
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
