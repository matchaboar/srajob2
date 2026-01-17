"""DBOS step function for fetching Firecrawl job status."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from dbos import DBOS
from firecrawl import Firecrawl
from firecrawl.v2.types import PaginationConfig

from ....config import settings

logger = logging.getLogger("dbos.step.firecrawl")


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=2.0, backoff_rate=2.0)
async def fetch_firecrawl_status_step(
    job_id: str,
    auto_paginate: bool = True,
    max_wait_time: int = 30,
    max_results: int = 5000,
) -> Dict[str, Any]:
    """Fetch the status of a Firecrawl batch scrape job.

    This step calls the Firecrawl SDK asynchronously using asyncio.to_thread()
    since the SDK is synchronous.

    Args:
        job_id: The Firecrawl job ID to check status for.
        auto_paginate: Whether to enable auto-pagination (default True).
        max_wait_time: Maximum wait time for pagination (default 30s).
        max_results: Maximum number of results to fetch (default 5000).

    Returns:
        Dict containing the job status and any scraped data.
        Returns {"error": "...", "status": "error"} on failure.

    Raises:
        Exception: On non-retryable errors after retry attempts exhausted.
    """
    firecrawl_api_key = settings.firecrawl_api_key
    if not firecrawl_api_key:
        return {
            "error": "FIRECRAWL_API_KEY env var is required",
            "status": "error",
            "jobId": job_id,
        }

    pagination = PaginationConfig(
        auto_paginate=auto_paginate,
        max_wait_time=max_wait_time,
        max_results=max_results,
    )

    def _get_status() -> Any:
        client = Firecrawl(api_key=firecrawl_api_key)
        return client.get_batch_scrape_status(job_id, pagination_config=pagination)

    logger.info("fetch_firecrawl_status_step job_id=%s", job_id)
    status = await asyncio.to_thread(_get_status)

    # Normalize the response to a dict
    if hasattr(status, "model_dump"):
        return status.model_dump(mode="json", exclude_none=True)
    if isinstance(status, dict):
        return status

    # Fallback: extract common attributes
    result: Dict[str, Any] = {"jobId": job_id}
    for attr in ("status", "data", "completed", "total", "error"):
        if hasattr(status, attr):
            result[attr] = getattr(status, attr)
    return result


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=2.0, backoff_rate=2.0)
async def fetch_firecrawl_status_raw_step(
    job_id: str,
) -> Optional[Any]:
    """Fetch raw Firecrawl status object (for advanced processing).

    This returns the raw SDK response object rather than a normalized dict.
    Useful when the caller needs to access SDK-specific methods.

    Args:
        job_id: The Firecrawl job ID to check status for.

    Returns:
        The raw Firecrawl status object, or None on error.
    """
    firecrawl_api_key = settings.firecrawl_api_key
    if not firecrawl_api_key:
        return None

    pagination = PaginationConfig(
        auto_paginate=True,
        max_wait_time=30,
        max_results=5000,
    )

    def _get_status() -> Any:
        client = Firecrawl(api_key=firecrawl_api_key)
        return client.get_batch_scrape_status(job_id, pagination_config=pagination)

    return await asyncio.to_thread(_get_status)
