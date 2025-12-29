#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"


def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _run_convex(payload: Dict[str, Any], *, env: str) -> Optional[Dict[str, Any]]:
    cmd = ["npx", "convex", "run"]
    if env == "prod":
        cmd.append("--prod")
    cmd.append("router:resetTodayAndRunAllScheduled")
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


def _format_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete jobs/scrapes/ignored/queue entries from the last N hours in Convex."
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

    totals = {
        "jobsDeleted": 0,
        "scrapesDeleted": 0,
        "queueDeleted": 0,
        "skippedDeleted": 0,
    }
    last_sites_triggered = 0
    has_more = True
    iterations = 0

    while has_more and iterations < args.max_iterations:
        iterations += 1
        payload = {
            "windowStart": window_start,
            "windowEnd": window_end,
            "batchSize": args.batch_size,
        }
        result = _run_convex(payload, env=args.env)
        if not isinstance(result, dict):
            print("No JSON response returned; stopping.")
            break

        totals["jobsDeleted"] += int(result.get("jobsDeleted", 0) or 0)
        totals["scrapesDeleted"] += int(result.get("scrapesDeleted", 0) or 0)
        totals["queueDeleted"] += int(result.get("queueDeleted", 0) or 0)
        totals["skippedDeleted"] += int(result.get("skippedDeleted", 0) or 0)
        last_sites_triggered = int(result.get("sitesTriggered", 0) or 0)
        has_more = bool(result.get("hasMore"))

        if has_more and args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000)

    print(
        json.dumps(
            {
                "windowStart": window_start,
                "windowEnd": window_end,
                "windowStartIso": _format_ms(window_start),
                "windowEndIso": _format_ms(window_end),
                "iterations": iterations,
                "hasMore": has_more,
                "sitesTriggered": last_sites_triggered,
                "totals": totals,
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
