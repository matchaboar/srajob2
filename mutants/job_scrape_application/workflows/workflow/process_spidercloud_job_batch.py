"""Workflow for processing SpiderCloud job detail batches.

This module provides the process_spidercloud_job_batch function using step-based
architecture. Uses scrape_job_detail_batch workflow internally.
"""

from __future__ import annotations

from typing import Any, Dict

from .scrape_job_detail_batch import scrape_job_detail_batch
from .store_scrape import store_scrape
from ..result import Success


async def process_spidercloud_job_batch(  # noqa: DBOS004 - convex calls are in DBOS steps
    batch: Dict[str, Any],
    persist_scrapes: bool = True,
) -> Dict[str, Any]:
    """Process a batch of job URLs via SpiderCloud.

    Args:
        batch: Dict with "urls" list containing job URL entries
        persist_scrapes: Whether to store scrapes to Convex (default True)

    Returns:
        Dict with scrape results including items, stored counts, etc.
    """
    result = await scrape_job_detail_batch(batch, persist_scrapes=persist_scrapes)

    # Convert Result to dict format expected by tests
    if isinstance(result, Success):
        data = result.value
        return {
            "provider": "spidercloud",
            "sourceUrl": data.source_url,
            "stored": data.stored,
            "invalid": data.invalid,
            "failed": data.failed,
            "items": {"normalized": []},
        }
    else:
        # Failure case
        return {
            "provider": "spidercloud",
            "sourceUrl": "",
            "stored": 0,
            "invalid": 0,
            "failed": 0,
            "error": result.message if hasattr(result, "message") else str(result),
            "items": {"normalized": []},
        }


__all__ = [
    "process_spidercloud_job_batch",
    "store_scrape",
]
