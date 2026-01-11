from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

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


def _chunk(items: List[Any], size: int) -> Iterable[List[Any]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _extract_problem_jobs(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    problems = payload.get("problem_jobs")
    if isinstance(problems, list):
        return [row for row in problems if isinstance(row, dict)]
    return []


def _extract_urls_and_ids(rows: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    urls: List[str] = []
    ids: List[str] = []
    seen_urls = set()
    for row in rows:
        url = row.get("url")
        job_id = row.get("id")
        if isinstance(url, str) and url.strip():
            cleaned = url.strip()
            if cleaned not in seen_urls:
                seen_urls.add(cleaned)
                urls.append(cleaned)
        if isinstance(job_id, str) and job_id.strip():
            ids.append(job_id.strip())
    return urls, ids



def _extract_source_url(log_entry: Dict[str, Any]) -> str | None:
    request_data = log_entry.get("requestData")
    if isinstance(request_data, dict):
        source = request_data.get("sourceUrl")
        if isinstance(source, str) and source.strip():
            return source.strip()
    return None


async def _build_job_source_map(convex_query, job_ids: List[str], limit: int) -> Dict[str, str]:
    logs = await convex_query(
        "router:listUrlScrapeLogs",
        {"limit": max(limit, 200), "includeJobLookup": True},
    )
    if not isinstance(logs, list):
        return {}
    mapping: Dict[str, str] = {}
    for entry in logs:
        if not isinstance(entry, dict):
            continue
        job_id = entry.get("jobId")
        if not isinstance(job_id, str) or job_id not in job_ids:
            continue
        source_url = _extract_source_url(entry)
        if source_url:
            mapping[job_id] = source_url
    return mapping


async def _list_matching_ignored(convex_query, urls: List[str], limit: int) -> List[str]:
    ignored_rows = await convex_query("router:listIgnoredJobs", {"limit": limit})
    if not isinstance(ignored_rows, list):
        return []
    url_set = {url.strip() for url in urls if isinstance(url, str)}
    ids: List[str] = []
    for row in ignored_rows:
        if not isinstance(row, dict):
            continue
        url = row.get("url")
        if isinstance(url, str) and url.strip() in url_set:
            row_id = row.get("_id")
            if isinstance(row_id, str):
                ids.append(row_id)
    return ids


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wipe ignored/seen rows and requeue problem job details for DBOS re-extract."
    )
    parser.add_argument("--env", choices=("dev", "prod"), default="dev")
    parser.add_argument(
        "--input",
        default="tmp/recent_job_parsing.json",
        help="Path to the recent parsing report JSON.",
    )
    parser.add_argument("--chunk-size", type=int, default=100)
    parser.add_argument("--ignored-limit", type=int, default=800)
    parser.add_argument("--delete-jobs", action="store_true")
    parser.add_argument("--delete-details", action="store_true", help="Delete job_details for job IDs")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report_path = Path(args.input)
    if not report_path.exists():
        raise SystemExit(f"Report not found: {report_path}")

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    problem_jobs = _extract_problem_jobs(payload)
    urls, job_ids = _extract_urls_and_ids(problem_jobs)
    if not urls:
        raise SystemExit("No problem job URLs found in report.")

    _load_env(args.env)
    from job_scrape_application.services import convex_query, convex_mutation  # noqa: E402

    ignored_ids = await _list_matching_ignored(convex_query, urls, args.ignored_limit)
    job_source_map = await _build_job_source_map(convex_query, job_ids, limit=len(urls) * 4)
    seen_entries = [
        {"sourceUrl": job_source_map[job_id], "url": job_url}
        for job_id, job_url in ((row.get("id"), row.get("url")) for row in problem_jobs)
        if isinstance(job_id, str) and isinstance(job_url, str) and job_id in job_source_map
    ]

    if args.dry_run:
        print(
            json.dumps(
                {
                    "env": args.env,
                    "problemUrls": len(urls),
                    "ignoredMatches": len(ignored_ids),
                    "seenMatches": len(seen_entries),
                    "deleteJobs": args.delete_jobs,
                    "deleteDetails": args.delete_details,
                    "jobsToDelete": len(job_ids),
                },
                indent=2,
            )
        )
        return

    deleted_jobs = 0
    deleted_details = 0
    if args.delete_jobs:
        for chunk in _chunk(job_ids, args.chunk_size):
            if not chunk:
                continue
            res = await convex_mutation("admin:deleteJobsById", {"jobIds": chunk})
            if isinstance(res, dict):
                deleted_jobs += int(res.get("deletedJobs", 0) or 0)
                deleted_details += int(res.get("deletedDetails", 0) or 0)

    if args.delete_details and not args.delete_jobs:
        for chunk in _chunk(job_ids, args.chunk_size):
            if not chunk:
                continue
            res = await convex_mutation("admin:deleteJobDetailsByJobIds", {"jobIds": chunk})
            if isinstance(res, dict):
                deleted_details += int(res.get("deletedDetails", 0) or 0)

    deleted_ignored = 0
    for chunk in _chunk(ignored_ids, args.chunk_size):
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

    print(
        json.dumps(
            {
                "env": args.env,
                "problemUrls": len(urls),
                "jobsDeleted": deleted_jobs,
                "detailsDeleted": deleted_details,
                "ignoredDeleted": deleted_ignored,
                "seenDeleted": deleted_seen,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
