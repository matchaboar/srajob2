"""DBOS step function for completing scrape URLs."""

from __future__ import annotations

from typing import Any

from dbos import DBOS

from ..queue import complete_scrape_urls as _complete_scrape_urls


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=0.5, backoff_rate=2.0)
def complete_scrape_urls_step(
    items: list[dict[str, Any]],
    status: str,
    error: str | None = None,
    run_after_ms: int | None = None,
) -> dict[str, Any]:
    """Record scrape URL completion for DBOS queue processing.

    This step records detail URL completion for deduplication and
    maintains legacy queue metadata when present.

    Args:
        items: List of item dicts with "id" and/or "url" keys
        status: New status ("completed", "failed", "pending", "invalid")
        error: Optional error message for failed items
        run_after_ms: Optional delay in ms before retry (for pending status)

    Returns:
        Dict with "updated" count
    """
    payload: dict[str, Any] = {
        "items": items,
        "status": status,
    }
    if error:
        payload["error"] = error
    if run_after_ms is not None:
        payload["runAfterMs"] = run_after_ms

    return _complete_scrape_urls(payload)
