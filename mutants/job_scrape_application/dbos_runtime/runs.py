from __future__ import annotations

import uuid

from .sqlite import initialize_schema, now_ms, transaction


def record_run(
    *,
    workflow_name: str,
    queue_name: str,
    status: str,
    error: str | None = None,
    started_at: int | None = None,
    completed_at: int | None = None,
) -> str:
    initialize_schema()
    run_id = str(uuid.uuid4())
    start = started_at or now_ms()
    completed = completed_at
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO workflow_runs (id, workflow_name, queue_name, status, started_at, completed_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, workflow_name, queue_name, status, start, completed, error),
        )
    return run_id


def last_completed_at(workflow_name: str) -> int | None:
    initialize_schema()
    with transaction() as conn:
        row = conn.execute(
            """
            SELECT completed_at FROM workflow_runs
            WHERE workflow_name = ? AND completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            (workflow_name,),
        ).fetchone()
        if not row:
            return None
        return int(row["completed_at"]) if row["completed_at"] is not None else None
