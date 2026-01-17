#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"


def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _run_convex(
    function_name: str, payload: Dict[str, Any], *, env: str
) -> Optional[Dict[str, Any]]:
    cmd = ["npx", "convex", "run"]
    if env == "prod":
        cmd.append("--prod")
    cmd.append(function_name)
    cmd.append(json.dumps(payload))
    result = subprocess.run(
        cmd,
        cwd=str(CONVEX_DIR),
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        check=True,
    )
    stdout = result.stdout.strip()
    if not stdout:
        return None
    return json.loads(stdout)


def _run_delete_loop(
    function_name: str,
    payload: Dict[str, Any],
    *,
    env: str,
    max_iterations: int,
    sleep_ms: int,
    sum_keys: Sequence[str],
) -> Dict[str, Any]:
    totals = {key: 0 for key in sum_keys}
    cursor: Optional[str] = None
    iterations = 0
    has_more = True

    while has_more and iterations < max_iterations:
        iterations += 1
        call_payload = dict(payload)
        if cursor:
            call_payload["cursor"] = cursor
        result = _run_convex(function_name, call_payload, env=env)
        if not isinstance(result, dict):
            return {
                "iterations": iterations,
                "hasMore": False,
                "error": "No JSON response returned",
                **totals,
            }

        for key in sum_keys:
            totals[key] += int(result.get(key, 0) or 0)

        cursor = result.get("cursor") or None
        has_more = bool(result.get("hasMore"))

    return {
        "iterations": iterations,
        "hasMore": has_more,
        **totals,
    }


def _format_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete jobs and related scrape metadata from the last N hours in Convex."
    )
    parser.add_argument("--hours", type=float, required=True, help="Hours back from now to delete.")
    parser.add_argument("--env", choices=["dev", "prod"], default="prod")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--max-iterations", type=int, default=200)
    parser.add_argument("--sleep-ms", type=int, default=200)
    args = parser.parse_args()

    if args.hours <= 0:
        raise SystemExit("--hours must be > 0")

    _load_env(args.env)

    now_ms = int(time.time() * 1000)
    window_start = now_ms - int(args.hours * 60 * 60 * 1000)
    window_end = now_ms

    delete_jobs = _run_delete_loop(
        "admin:deleteRecentJobsPage",
        {
            "sinceMs": window_start,
            "untilMs": window_end,
            "batchSize": args.batch_size,
        },
        env=args.env,
        max_iterations=args.max_iterations,
        sleep_ms=args.sleep_ms,
        sum_keys=[
            "scanned",
            "deletedJobs",
            "deletedDetails",
            "deletedJobUrlKeys",
            "deletedSeenUrls",
            "deletedSeenUrlIndex",
            "deletedIgnoredJobs",
        ],
    )
    delete_scrapes = _run_delete_loop(
        "admin:deleteRecentScrapesPage",
        {
            "sinceMs": window_start,
            "untilMs": window_end,
            "batchSize": args.batch_size,
        },
        env=args.env,
        max_iterations=args.max_iterations,
        sleep_ms=args.sleep_ms,
        sum_keys=["scanned", "deleted"],
    )
    delete_scrape_activity = _run_delete_loop(
        "admin:deleteRecentScrapeActivityPage",
        {
            "sinceMs": window_start,
            "untilMs": window_end,
            "batchSize": args.batch_size,
        },
        env=args.env,
        max_iterations=args.max_iterations,
        sleep_ms=args.sleep_ms,
        sum_keys=["scanned", "deleted"],
    )
    delete_scrape_errors = _run_delete_loop(
        "admin:deleteRecentScrapeErrorsPage",
        {
            "sinceMs": window_start,
            "untilMs": window_end,
            "batchSize": args.batch_size,
        },
        env=args.env,
        max_iterations=args.max_iterations,
        sleep_ms=args.sleep_ms,
        sum_keys=["scanned", "deleted"],
    )
    delete_ignored = _run_delete_loop(
        "admin:deleteRecentIgnoredJobsPage",
        {
            "sinceMs": window_start,
            "batchSize": args.batch_size,
        },
        env=args.env,
        max_iterations=args.max_iterations,
        sleep_ms=args.sleep_ms,
        sum_keys=["scanned", "deleted"],
    )
    delete_seen_urls = _run_delete_loop(
        "admin:deleteRecentSeenJobUrlsPage",
        {
            "sinceMs": window_start,
            "batchSize": args.batch_size,
        },
        env=args.env,
        max_iterations=args.max_iterations,
        sleep_ms=args.sleep_ms,
        sum_keys=["scanned", "deleted"],
    )
    delete_seen_index = _run_delete_loop(
        "admin:deleteRecentSeenJobUrlIndexPage",
        {
            "sinceMs": window_start,
            "batchSize": args.batch_size,
        },
        env=args.env,
        max_iterations=args.max_iterations,
        sleep_ms=args.sleep_ms,
        sum_keys=["scanned", "deleted"],
    )

    totals = {
        "jobs": delete_jobs,
        "scrapes": delete_scrapes,
        "scrapeActivity": delete_scrape_activity,
        "scrapeErrors": delete_scrape_errors,
        "ignoredJobs": delete_ignored,
        "seenJobUrls": delete_seen_urls,
        "seenJobUrlIndex": delete_seen_index,
    }

    has_more = any(result.get("hasMore") for result in totals.values())

    print(
        json.dumps(
            {
                "windowStart": window_start,
                "windowEnd": window_end,
                "windowStartIso": _format_ms(window_start),
                "windowEndIso": _format_ms(window_end),
                "totals": totals,
                "hasMore": has_more,
            },
            indent=2,
        )
    )

    if has_more:
        print(
            "Warning: reached max iterations before completion. Increase --max-iterations or rerun."
        )


if __name__ == "__main__":
    main()
