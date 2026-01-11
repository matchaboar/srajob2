#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"
DBOS_SQLITE_PATH = REPO_ROOT / "job_scrape_application" / "dbos_runtime" / "dbos.sqlite"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _remove_dbos_sqlite(path: Path) -> None:
    if path.exists():
        path.unlink()


def _max_pages(value: int) -> int:
    return max(1, min(value, 500))


async def _enqueue_all_sites() -> int:
    from job_scrape_application.dbos_runtime import runner

    return await runner._enqueue_listing_sites()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Reset dev DBOS sqlite state.")
    parser.add_argument("--env", choices=("dev", "prod"), default="dev")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--skip-enqueue", action="store_true")
    parser.add_argument("--skip-db", action="store_true")
    args = parser.parse_args()

    if args.env != "dev":
        raise SystemExit("Refusing to run against prod. Use dev only.")

    _load_env(args.env)

    batch_size = _max_pages(args.batch_size)

    if not args.skip_db:
        _remove_dbos_sqlite(DBOS_SQLITE_PATH)

    enqueue_count = 0
    if not args.skip_enqueue:
        enqueue_count = await _enqueue_all_sites()

    print(
        {
            "env": args.env,
            "dbos_sqlite_removed": not args.skip_db,
            "listing_sites_enqueued": enqueue_count,
            "batch_size": batch_size,
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
