from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

from job_scrape_application.services import convex_query
from job_scrape_application.workflows.activities import process_spidercloud_job_batch
from job_scrape_application.workflows.helpers.link_extractors import normalize_url

DEV_ENV_FILES: tuple[str, ...] = (
    ".env",
    "job_board_application/.env.local",
    "job_board_application/.env.development",
)


def _load_env() -> None:
    load_dotenv()
    for path in DEV_ENV_FILES:
        if Path(path).exists():
            load_dotenv(path, override=False)


def _normalize(candidate: str | None) -> str | None:
    normalized = normalize_url(candidate)
    return normalized or (candidate.strip() if isinstance(candidate, str) else None)


async def _find_job_log(url: str) -> Dict[str, Any] | None:
    logs = await convex_query("router:listUrlScrapeLogs", {"limit": 100, "includeJobLookup": True})
    normalized_target = _normalize(url)
    for entry in logs or []:
        if not isinstance(entry, dict):
            continue
        candidates = [_normalize(entry.get("url")), _normalize(entry.get("jobUrl"))]
        if normalized_target and normalized_target in candidates:
            return entry
    return None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape+ingest a job URL into Convex dev.")
    parser.add_argument("url", help="Job URL to scrape")
    args = parser.parse_args()

    _load_env()
    if not (os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")):
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is required")

    result = await process_spidercloud_job_batch(
        {"urls": [{"url": args.url, "sourceUrl": args.url}]},
        persist_scrapes=True,
    )

    log_entry = await _find_job_log(args.url)
    if not log_entry:
        raise SystemExit("Could not find scrape log entry for URL")
    job_id = log_entry.get("jobId")
    if not job_id:
        raise SystemExit("Scrape log entry missing jobId")

    details = await convex_query("jobs:getJobDetails", {"jobId": job_id})
    if not isinstance(details, dict):
        raise SystemExit("Unexpected job details response")

    summary = {
        "jobId": job_id,
        "stored": result.get("stored"),
        "descriptionStorageAvailable": details.get("descriptionStorageAvailable"),
        "descriptionStorageJobId": details.get("descriptionStorageJobId"),
        "descriptionLength": len(details.get("description") or ""),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
