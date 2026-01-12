#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from collections import Counter
from typing import Any, cast

from dbos import DBOS, DBOSConfig


def _build_config(system_database_url: str) -> DBOSConfig:
    config: dict[str, Any] = {
        "name": "local-dbos",
        "system_database_url": system_database_url,
    }
    return cast(DBOSConfig, config)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report DBOS workflow counts by status.",
    )
    parser.add_argument(
        "--system-db-url",
        default=os.environ.get("DBOS_SYSTEM_DATABASE_URL", ""),
        help="DBOS system database URL. Defaults to DBOS_SYSTEM_DATABASE_URL.",
    )
    args = parser.parse_args()

    if not args.system_db_url:
        raise SystemExit(
            "Missing DBOS system database URL. Set DBOS_SYSTEM_DATABASE_URL or pass --system-db-url.",
        )

    DBOS(config=_build_config(args.system_db_url))
    DBOS.launch()

    workflows = DBOS.list_workflows(load_input=False, load_output=False)
    status_counts = Counter(workflow.status for workflow in workflows)

    print({"total_workflows": len(workflows)})
    print({"status_counts": dict(status_counts)})


if __name__ == "__main__":
    main()
