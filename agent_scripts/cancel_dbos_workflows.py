#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from typing import Any

from dbos import DBOS


def _build_config(system_database_url: str) -> dict[str, Any]:
    return {
        "name": "local-dbos",
        "system_database_url": system_database_url,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cancel queued DBOS workflows in the system database.",
    )
    parser.add_argument(
        "--system-db-url",
        default=os.environ.get("DBOS_SYSTEM_DATABASE_URL", ""),
        help="DBOS system database URL. Defaults to DBOS_SYSTEM_DATABASE_URL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report queued workflows without cancelling.",
    )
    args = parser.parse_args()

    if not args.system_db_url:
        raise SystemExit(
            "Missing DBOS system database URL. Set DBOS_SYSTEM_DATABASE_URL or pass --system-db-url.",
        )

    DBOS(config=_build_config(args.system_db_url))
    DBOS.launch()

    workflows = DBOS.list_queued_workflows()
    queued_count = len(workflows)
    print({"queued_workflows": queued_count})

    if args.dry_run:
        return

    for workflow in workflows:
        workflow_id = workflow.get("workflow_id") if isinstance(workflow, dict) else None
        if workflow_id:
            DBOS.cancel_workflow(workflow_id)

    print({"cancelled_workflows": queued_count})


if __name__ == "__main__":
    main()
