"""DBOS step function for checking if detail queue has pending work."""

from __future__ import annotations

from dbos import DBOS

from ..queue import detail_queue_has_pending


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=0.5, backoff_rate=2.0)
def check_detail_queue_pending_step(*, include_processing: bool = False) -> bool:
    """Check if the detail queue has pending items.

    This step queries the SQLite queue to determine if there are pending
    job detail URLs waiting to be processed. Used by the scheduler to
    decide whether to pause listing scraping.

    Args:
        include_processing: If True, also count items currently being processed

    Returns:
        True if there are pending items, False otherwise
    """
    return detail_queue_has_pending(include_processing=include_processing)
