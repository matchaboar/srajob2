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


def _normalize_job(job: Dict[str, Any]) -> Dict[str, Any]:
    job_id = job.get("_id") or job.get("id")
    payload = {
        "id": str(job_id) if job_id is not None else None,
        "title": job.get("title"),
        "company": job.get("company"),
        "location": job.get("location"),
        "url": job.get("url"),
        "postedAt": job.get("postedAt"),
        "description": job.get("description"),
    }
    return {k: v for k, v in payload.items() if v is not None}


async def _fetch_job(convex_query, job_id: str) -> Optional[Dict[str, Any]]:
    job = await convex_query("jobs:getJobById", {"id": job_id})
    return job if isinstance(job, dict) else None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Convex job records by id.")
    parser.add_argument("job_ids", nargs="+", help="Convex job ids to fetch.")
    parser.add_argument(
        "--env",
        default="prod",
        choices=("dev", "prod"),
        help="Convex environment to query (default: prod).",
    )
    parser.add_argument(
        "--out",
        help="Optional output JSON path (writes a list of jobs).",
    )
    args = parser.parse_args()

    _load_env(args.env)
    from job_scrape_application.services import convex_query  # noqa: E402

    results: List[Dict[str, Any]] = []
    for job_id in args.job_ids:
        job = await _fetch_job(convex_query, job_id)
        if not job:
            print(f"Missing job id={job_id}")
            continue
        results.append(_normalize_job(job))

    payload: Any
    if len(results) == 1:
        payload = results[0]
    else:
        payload = results

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
