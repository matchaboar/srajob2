#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
FIXTURE_DIR = REPO_ROOT / "tests" / "job_scrape_application" / "fixtures"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return cleaned.strip("-") or "company"


def _matches_company(value: Any, needle: str) -> bool:
    if not isinstance(value, str):
        return False
    return needle.lower() in value.lower()

def _matches_any_company(value: Any, needles: Iterable[str]) -> bool:
    for needle in needles:
        if _matches_company(value, needle):
            return True
    return False


def _is_domain_like(value: str) -> bool:
    if not value:
        return False
    if "://" in value:
        return True
    return "." in value and " " not in value


def _extract_domain(value: str) -> Optional[str]:
    if not value:
        return None
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    host = parsed.hostname or ""
    return host.lower() if host else None


def _matches_any_domain(url: Any, domains: Iterable[str]) -> bool:
    if not isinstance(url, str) or not url.strip():
        return False
    url_domain = _extract_domain(url)
    if not url_domain:
        return False
    for domain in domains:
        if not isinstance(domain, str):
            continue
        domain_lower = domain.lower()
        if not domain_lower:
            continue
        if url_domain == domain_lower or url_domain.endswith(f".{domain_lower}"):
            return True
    return False


async def _collect_company_aliases(convex_query, company: str) -> Dict[str, Set[str]]:
    aliases = await convex_query("router:listDomainAliases", {}) or []
    name_matches: Set[str] = set()
    domain_matches: Set[str] = set()
    needle = company.strip()
    needle_lower = needle.lower()

    target_domain = _extract_domain(needle) if _is_domain_like(needle) else None

    for row in aliases:
        if not isinstance(row, dict):
            continue
        domain = row.get("domain")
        derived = row.get("derivedName")
        alias = row.get("alias")
        site_name = row.get("siteName")
        site_url = row.get("siteUrl")

        values = [v for v in [alias, derived, site_name, domain, site_url] if isinstance(v, str)]
        if target_domain:
            if isinstance(domain, str) and target_domain in domain.lower():
                domain_matches.add(domain)
            if isinstance(site_url, str) and target_domain in site_url.lower():
                domain_matches.add(target_domain)
        if any(needle_lower in v.lower() for v in values):
            if isinstance(domain, str):
                domain_matches.add(domain)
            if isinstance(alias, str):
                name_matches.add(alias)
            if isinstance(derived, str):
                name_matches.add(derived)
            if isinstance(site_name, str):
                name_matches.add(site_name)

    if not name_matches and target_domain:
        for row in aliases:
            if not isinstance(row, dict):
                continue
            domain = row.get("domain")
            if isinstance(domain, str) and target_domain in domain.lower():
                for key in ("alias", "derivedName", "siteName"):
                    value = row.get(key)
                    if isinstance(value, str) and value.strip():
                        name_matches.add(value.strip())

    return {"names": name_matches, "domains": domain_matches}


def _unique_by_id(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for row in rows:
        raw_id = row.get("_id") or row.get("id")
        if raw_id is None:
            continue
        row_id = str(raw_id)
        if row_id in seen:
            continue
        seen.add(row_id)
        unique.append(row)
    return unique


def _normalize_job(job: Dict[str, Any]) -> Dict[str, Any]:
    job_id = job.get("_id") or job.get("id")
    payload = {
        "id": str(job_id) if job_id is not None else None,
        "title": job.get("title"),
        "company": job.get("company"),
        "location": job.get("location"),
        "locations": job.get("locations"),
        "country": job.get("country"),
        "remote": job.get("remote"),
        "level": job.get("level"),
        "totalCompensation": job.get("totalCompensation"),
        "compensationUnknown": job.get("compensationUnknown"),
        "currencyCode": job.get("currencyCode"),
        "url": job.get("url"),
        "postedAt": job.get("postedAt"),
        "description": job.get("description"),
    }
    return {k: v for k, v in payload.items() if v is not None}


async def _fetch_recent_jobs(convex_query) -> List[Dict[str, Any]]:
    jobs = await convex_query("jobs:getRecentJobs", {})
    return jobs if isinstance(jobs, list) else []


async def _fetch_url_logs(convex_query, limit: int) -> List[Dict[str, Any]]:
    logs = await convex_query(
        "router:listUrlScrapeLogs",
        {"limit": limit, "includeJobLookup": True},
    )
    return logs if isinstance(logs, list) else []


async def _fetch_job(convex_query, job_id: str) -> Optional[Dict[str, Any]]:
    job = await convex_query("jobs:getJobById", {"id": job_id})
    return job if isinstance(job, dict) else None


async def _collect_company_jobs(
    convex_query,
    company: str,
    limit: int,
    log_limit: int,
    include_logs: bool,
    *,
    include_aliases: bool = True,
) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    candidate_names: Set[str] = {company}
    candidate_domains: Set[str] = set()

    if include_aliases:
        alias_info = await _collect_company_aliases(convex_query, company)
        candidate_names.update(alias_info.get("names", set()))
        candidate_domains.update(alias_info.get("domains", set()))

    recent_jobs = await _fetch_recent_jobs(convex_query)
    for job in recent_jobs:
        if not _matches_any_company(job.get("company"), candidate_names) and not _matches_any_domain(
            job.get("url"), candidate_domains
        ):
            continue
        job_id = job.get("_id") or job.get("id")
        if job_id is None:
            continue
        job_id_str = str(job_id)
        if job_id_str in seen:
            continue
        full_job = await _fetch_job(convex_query, job_id_str)
        if full_job:
            matches.append(full_job)
            seen.add(job_id_str)
        if len(matches) >= limit:
            return matches

    if not include_logs:
        return matches

    logs = await _fetch_url_logs(convex_query, log_limit)
    for entry in logs:
        if not _matches_any_company(entry.get("jobCompany"), candidate_names) and not _matches_any_domain(
            entry.get("url"), candidate_domains
        ):
            continue
        job_id = entry.get("jobId")
        if job_id is None:
            continue
        job_id_str = str(job_id)
        if job_id_str in seen:
            continue
        full_job = await _fetch_job(convex_query, job_id_str)
        if full_job:
            matches.append(full_job)
            seen.add(job_id_str)
        if len(matches) >= limit:
            break

    return matches


def _write_fixture(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export the first N Convex jobs for a company into a JSON fixture.",
    )
    parser.add_argument("--company", required=True, help="Company name to match (substring).")
    parser.add_argument("--limit", type=int, default=3, help="Number of jobs to export (default: 3).")
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="prod",
        help="Convex env to target (default: prod).",
    )
    parser.add_argument(
        "--log-limit",
        type=int,
        default=400,
        help="Max scrape log entries to scan when recent jobs are insufficient.",
    )
    parser.add_argument(
        "--skip-logs",
        action="store_true",
        help="Skip scrape-log fallback lookup.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override output path (defaults to tests/job_scrape_application/fixtures/convex_<company>_jobs.json).",
    )
    args = parser.parse_args()

    limit = max(1, args.limit)
    _load_env(args.env)

    from job_scrape_application.services import convex_query  # noqa: E402

    jobs = await _collect_company_jobs(
        convex_query,
        args.company,
        limit,
        args.log_limit,
        not args.skip_logs,
        include_aliases=True,
    )

    if not jobs:
        raise SystemExit(f"No jobs found for company match '{args.company}'.")

    jobs = _unique_by_id(jobs)
    jobs.sort(key=lambda row: int(row.get("postedAt") or 0), reverse=True)
    jobs = jobs[:limit]

    normalized = [_normalize_job(job) for job in jobs]

    slug = _slugify(args.company)
    output_path = Path(args.output) if args.output else (FIXTURE_DIR / f"convex_{slug}_jobs.json")

    now_ms = int(time.time() * 1000)
    payload = {
        "meta": {
            "company": args.company,
            "companySlug": slug,
            "limit": limit,
            "count": len(normalized),
            "env": args.env,
            "companyMatches": sorted({str(item.get("company")) for item in normalized if item.get("company")}),
            "generatedAtMs": now_ms,
            "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ms / 1000)),
        },
        "jobs": normalized,
    }

    _write_fixture(output_path, payload)
    print(json.dumps({"output": str(output_path), "count": len(normalized)}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
