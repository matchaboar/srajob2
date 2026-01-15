#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

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


def _normalize(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _matches(company: str, candidate: Optional[str]) -> bool:
    if not candidate:
        return False
    return _normalize(company) in _normalize(candidate)

def _site_matches(company: str, site: Dict[str, Any]) -> bool:
    return any(
        _matches(company, site.get(field))
        for field in ("name", "url", "pattern", "type", "scrapeProvider")
    )


def _host_prefix(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.hostname:
        return None
    host = parsed.hostname.lower()
    return f"{parsed.scheme}://{host}"


def _strip_trailing_slash(value: str) -> str:
    return value[:-1] if value.endswith("/") else value


def _extract_greenhouse_slug(url: str) -> Optional[str]:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    parts = [p for p in path.split("/") if p]

    query = parse_qs(parsed.query)
    if "for" in query and query["for"]:
        return query["for"][0].strip() or None

    if "boards" in parts:
        idx = parts.index("boards")
        if idx + 1 < len(parts):
            return parts[idx + 1].strip() or None

    if host.endswith("greenhouse.io") and parts:
        # boards.greenhouse.io/<slug>
        return parts[0].strip() or None

    return None


def _candidate_prefixes_for_site(site: Dict[str, Any]) -> List[str]:
    prefixes: List[str] = []
    url = site.get("url")
    if isinstance(url, str) and url.strip():
        cleaned_url = _strip_trailing_slash(url.strip())
        prefixes.append(cleaned_url)
        host_prefix = _host_prefix(cleaned_url)
        if host_prefix:
            parsed = urlparse(cleaned_url)
            prefixes.append(host_prefix)
            if parsed.path and parsed.path != "/":
                prefixes.append(f"{host_prefix}{_strip_trailing_slash(parsed.path)}")

            slug = _extract_greenhouse_slug(cleaned_url)
            if slug:
                prefixes.extend(
                    [
                        f"https://boards-api.greenhouse.io/v1/boards/{slug}",
                        f"https://api.greenhouse.io/v1/boards/{slug}",
                        f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                        f"https://api.greenhouse.io/v1/boards/{slug}/jobs",
                        f"https://boards.greenhouse.io/{slug}",
                    ]
                )

    pattern = site.get("pattern")
    if isinstance(pattern, str) and pattern.strip():
        raw = pattern.strip()
        wildcard_index = raw.find("*")
        prefix = raw[:wildcard_index] if wildcard_index != -1 else raw
        prefix = _strip_trailing_slash(prefix)
        if prefix:
            prefixes.append(prefix)

    # Deduplicate while preserving order
    seen: set[str] = set()
    ordered: List[str] = []
    for prefix in prefixes:
        if prefix in seen:
            continue
        seen.add(prefix)
        ordered.append(prefix)
    return ordered


def _prefix_domain(prefix: str) -> Optional[str]:
    parsed = urlparse(prefix)
    if parsed.hostname:
        return parsed.hostname.lower()
    return None


def _find_matching_domains(company: str, aliases: Iterable[Dict[str, Any]]) -> List[Tuple[str, str]]:
    matches: List[Tuple[str, str]] = []
    for row in aliases:
        if not isinstance(row, dict):
            continue
        domain = (row.get("domain") or "").strip()
        if not domain:
            continue
        if any(
            _matches(company, row.get(field))
            for field in ("alias", "derivedName", "siteName", "siteUrl", "domain")
        ):
            prefix = _prefix_for_domain(domain, row.get("siteUrl"))
            matches.append((domain, prefix))
    return matches


def _prefix_for_domain(domain: str, site_url: Optional[str]) -> str:
    if site_url:
        parsed = urlparse(site_url)
        if parsed.scheme and parsed.hostname:
            return f"{parsed.scheme}://{parsed.hostname.lower()}"
    return f"https://{domain.lower()}"


def _collect_site_prefixes(company: str, sites: Sequence[Dict[str, Any]]) -> List[str]:
    prefixes: List[str] = []
    for site in sites:
        if not isinstance(site, dict):
            continue
        if not _site_matches(company, site):
            continue
        prefixes.extend(_candidate_prefixes_for_site(site))

    # Deduplicate while preserving order
    seen: set[str] = set()
    ordered: List[str] = []
    for prefix in prefixes:
        if prefix in seen:
            continue
        seen.add(prefix)
        ordered.append(prefix)
    return ordered


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete skipped/ignored jobs for a company (name or alias) from Convex."
    )
    parser.add_argument("--company", required=True, help="Company name or alias (case-insensitive).")
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=50)
    args = parser.parse_args()

    _load_env(args.env)
    env = os.environ.copy()

    sites_cmd = _build_convex_run_args(args.env, "router:listSites", {"enabledOnly": False})
    sites = _run_convex(sites_cmd, env=env) or []
    prefixes = _collect_site_prefixes(args.company, sites)

    if not prefixes:
        aliases_cmd = _build_convex_run_args(args.env, "router:listDomainAliases", {})
        aliases = _run_convex(aliases_cmd, env=env) or []
        domain_matches = _find_matching_domains(args.company, aliases)
        prefixes = [prefix for _, prefix in domain_matches]

    if not prefixes:
        print(f"No matching sites or domains found for company '{args.company}'.")
        return

    results: Dict[str, Any] = {}
    for prefix in prefixes:
        domain = _prefix_domain(prefix)
        if not domain:
            continue
        total_deleted = 0
        total_scanned = 0
        pages = 0
        cursor: Optional[str] = None
        has_more = True

        while has_more and pages < args.max_pages:
            payload: Dict[str, Any] = {
                "domain": domain,
                "prefix": prefix,
                "table": "ignored_jobs",
                "batchSize": args.batch_size,
            }
            if cursor:
                payload["cursor"] = cursor

            wipe_cmd = _build_convex_run_args(args.env, "admin:wipeSiteDataByDomainPage", payload)
            wipe_result = _run_convex(wipe_cmd, env=env)
            if not isinstance(wipe_result, dict):
                break

            total_deleted += int(wipe_result.get("deleted", 0) or 0)
            total_scanned += int(wipe_result.get("scanned", 0) or 0)
            pages += 1
            cursor = wipe_result.get("cursor")
            has_more = bool(wipe_result.get("hasMore"))

        results[prefix] = {
            "domain": domain,
            "deleted": total_deleted,
            "scanned": total_scanned,
            "pages": pages,
            "hasMore": has_more,
            "cursor": cursor,
        }

    print(
        json.dumps(
            {
                "company": args.company,
                "prefixes": prefixes,
                "ignored_jobs_wipe": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
