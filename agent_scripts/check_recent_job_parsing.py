#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

UNKNOWN_LABELS = {
    "unknown",
    "n/a",
    "na",
    "unspecified",
    "not available",
}
UNKNOWN_TITLES = {
    "page_title",
    "title",
    "job_title",
    "untitled",
    "application",
}
GENERIC_COMPANY_TOKENS = {
    "ashby",
    "avature",
    "brassring",
    "greenhouse",
    "icims",
    "jibeapply",
    "lever",
    "smartrecruiters",
    "taleo",
    "workday",
}


def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _normalize_label(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def _is_unknown_label(value: Any) -> bool:
    normalized = _normalize_label(value)
    if not normalized:
        return True
    if "remote" in normalized:
        return False
    return normalized in UNKNOWN_LABELS


def _is_unknown_title(value: Any) -> bool:
    normalized = _normalize_label(value)
    if not normalized:
        return True
    if normalized in UNKNOWN_LABELS:
        return True
    return normalized in UNKNOWN_TITLES


def _is_generic_company(value: Any) -> bool:
    normalized = _normalize_label(value)
    if not normalized:
        return True
    return normalized in GENERIC_COMPANY_TOKENS


def _check_job(job: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    if _is_unknown_title(job.get("title")):
        issues.append("title")
    if _is_unknown_label(job.get("company")) or _is_generic_company(job.get("company")):
        issues.append("company")
    if _is_unknown_label(job.get("location")):
        issues.append("location")
    description = job.get("description")
    if not isinstance(description, str) or len(description.strip()) < 200:
        issues.append("description")
    if not isinstance(job.get("postedAt"), (int, float)):
        issues.append("postedAt")
    return issues


def _normalize_job(job: Dict[str, Any]) -> Dict[str, Any]:
    job_id = job.get("_id") or job.get("id")
    description = job.get("description")
    return {
        "id": str(job_id) if job_id is not None else None,
        "title": job.get("title"),
        "company": job.get("company"),
        "location": job.get("location"),
        "remote": job.get("remote"),
        "level": job.get("level"),
        "totalCompensation": job.get("totalCompensation"),
        "compensationUnknown": job.get("compensationUnknown"),
        "currencyCode": job.get("currencyCode"),
        "url": job.get("url"),
        "postedAt": job.get("postedAt"),
        "scrapedAt": job.get("scrapedAt"),
        "workflowName": job.get("workflowName"),
        "scrapedWith": job.get("scrapedWith"),
        "descriptionLength": len(description.strip()) if isinstance(description, str) else 0,
    }


async def _fetch_recent_job_ids(convex_query, limit: int) -> List[str]:
    logs = await convex_query(
        "router:listUrlScrapeLogs",
        {"limit": max(limit * 4, 100), "includeJobLookup": True},
    )
    if not isinstance(logs, list):
        return []
    job_ids: List[str] = []
    seen = set()
    for entry in logs:
        job_id = entry.get("jobId")
        if not job_id:
            continue
        job_id_str = str(job_id)
        if job_id_str in seen:
            continue
        seen.add(job_id_str)
        job_ids.append(job_id_str)
        if len(job_ids) >= limit:
            break
    return job_ids


async def _fetch_jobs(convex_query, job_ids: List[str], limit: int) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    if job_ids:
        for job_id in job_ids:
            job = await convex_query("jobs:getJobById", {"id": job_id})
            if isinstance(job, dict):
                jobs.append(job)
        return jobs

    recent = await convex_query("jobs:getRecentJobs", {})
    if isinstance(recent, list):
        for job in recent[:limit]:
            if not isinstance(job, dict):
                continue
            job_id = job.get("_id") or job.get("id")
            if job_id is None:
                continue
            full_job = await convex_query("jobs:getJobById", {"id": str(job_id)})
            if isinstance(full_job, dict):
                jobs.append(full_job)
    return jobs


async def main() -> None:
    parser = argparse.ArgumentParser(description="Check recent job parsing quality.")
    parser.add_argument("--env", choices=("dev", "prod"), default="dev")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--out", help="Optional output JSON path")
    args = parser.parse_args()

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    job_ids = await _fetch_recent_job_ids(convex_query, args.limit)
    jobs = await _fetch_jobs(convex_query, job_ids, args.limit)

    results: List[Dict[str, Any]] = []
    problems: List[Dict[str, Any]] = []
    for job in jobs:
        normalized = _normalize_job(job)
        issues = _check_job(job)
        normalized["issues"] = issues
        results.append(normalized)
        if issues:
            problems.append(normalized)

    payload = {
        "count": len(results),
        "issues": len(problems),
        "jobs": results,
        "problem_jobs": problems,
    }

    output = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output, encoding="utf-8")

    if problems:
        print(
            "\n".join(
                f"{job.get('id', '')} | {','.join(job.get('issues') or [])} | {job.get('url', '')}"
                for job in problems
            )
        )
    elif not args.out:
        print(output)


if __name__ == "__main__":
    asyncio.run(main())
