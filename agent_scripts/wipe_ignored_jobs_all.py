#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"


def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _run_convex(args: List[str], *, env: Dict[str, str]) -> Any:
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


def _prefixes_for_url(url: str) -> Tuple[str, str] | None:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    if not parsed.scheme or not parsed.hostname:
        return None
    domain = parsed.hostname.lower()
    prefix = f"{parsed.scheme}://{domain}"
    return domain, prefix


def _collect_domain_prefixes(
    sites: Iterable[Dict[str, Any]],
    aliases: Iterable[Dict[str, Any]],
) -> Dict[str, Set[str]]:
    domains: Dict[str, Set[str]] = {}

    def _add(domain: str, prefix: str) -> None:
        if not domain or not prefix:
            return
        domains.setdefault(domain, set()).add(prefix)

    for site in sites:
        url = site.get("url")
        if isinstance(url, str):
            parsed = _prefixes_for_url(url.strip())
            if parsed:
                domain, prefix = parsed
                _add(domain, prefix)

    for row in aliases:
        domain = row.get("domain")
        if not isinstance(domain, str) or not domain.strip():
            continue
        domain = domain.strip().lower()
        site_url = row.get("siteUrl")
        if isinstance(site_url, str):
            parsed = _prefixes_for_url(site_url.strip())
            if parsed:
                _add(parsed[0], parsed[1])
                continue
        _add(domain, f"https://{domain}")
        _add(domain, f"http://{domain}")

    return domains


def main() -> None:
    parser = argparse.ArgumentParser(description="Wipe ignored_jobs for all known site domains.")
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    _load_env(args.env)
    env = os.environ.copy()

    sites_cmd = _build_convex_run_args(args.env, "router:listSites", {"enabledOnly": False})
    sites = _run_convex(sites_cmd, env=env) or []

    aliases_cmd = _build_convex_run_args(args.env, "router:listDomainAliases", {})
    aliases = _run_convex(aliases_cmd, env=env) or []

    domain_prefixes = _collect_domain_prefixes(sites, aliases)

    total_deleted = 0
    total_scanned = 0
    details: Dict[str, Any] = {}

    for domain, prefixes in sorted(domain_prefixes.items()):
        for prefix in sorted(prefixes):
            cursor: Optional[str] = None
            has_more = True
            deleted = 0
            scanned = 0
            pages = 0
            while has_more:
                payload: Dict[str, Any] = {
                    "domain": domain,
                    "prefix": prefix,
                    "table": "ignored_jobs",
                    "batchSize": args.batch_size,
                    "dryRun": args.dry_run,
                }
                if cursor:
                    payload["cursor"] = cursor
                cmd = _build_convex_run_args(args.env, "admin:wipeSiteDataByDomainPage", payload)
                result = _run_convex(cmd, env=env)
                if not isinstance(result, dict):
                    break
                deleted += int(result.get("deleted", 0) or 0)
                scanned += int(result.get("scanned", 0) or 0)
                pages += 1
                cursor = result.get("cursor")
                has_more = bool(result.get("hasMore"))
                if args.dry_run:
                    break
            if deleted or scanned or pages:
                details_key = f"{domain}::{prefix}"
                details[details_key] = {
                    "domain": domain,
                    "prefix": prefix,
                    "deleted": deleted,
                    "scanned": scanned,
                    "pages": pages,
                    "hasMore": has_more,
                }
            total_deleted += deleted
            total_scanned += scanned

    print(
        json.dumps(
            {
                "env": args.env,
                "dryRun": args.dry_run,
                "domains": len(domain_prefixes),
                "totalDeleted": total_deleted,
                "totalScanned": total_scanned,
                "details": details,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
