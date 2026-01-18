"""DBOS step function for batch storing scrapes to Convex."""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List

from dbos import DBOS
from temporalio.exceptions import ApplicationError

from job_scrape_application.dbos_runtime.step import complete_scrape_urls_step
from job_scrape_application.workflows.activities.step.store_scrape import insert_scrape_record_step

logger = logging.getLogger("dbos.step.store")


def _extract_url_from_scrape(scrape: Dict[str, Any]) -> str | None:
    """Extract URL from scrape payload for tracking."""
    # Try subUrls first
    sub_urls = scrape.get("subUrls")
    if isinstance(sub_urls, list):
        for entry in sub_urls:
            if isinstance(entry, str) and entry.strip():
                return entry.strip()

    # Try sourceUrl
    source_val = scrape.get("sourceUrl")
    if isinstance(source_val, str) and source_val.strip():
        return source_val.strip()

    return None


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=1.0, backoff_rate=2.0)
def batch_store_scrapes_step(
    scrapes: List[Dict[str, Any]],
    url_completion_data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Store scrapes sequentially and complete URLs in queue.

    This step replaces the async batch_store_scrapes_background activity.
    Since Convex is synchronous, we iterate through scrapes sequentially
    rather than using asyncio concurrency.

    Args:
        scrapes: List of scrape payloads to store.
        url_completion_data: Optional metadata for URL completion (unused, kept for API compat).

    Returns:
        Dictionary containing:
        - operationId: Unique ID for this storage operation
        - stored: Count of successfully stored scrapes
        - scrapeIds: List of scrape IDs from Convex
        - failed: Count of failed stores
        - invalid: Count of invalid scrapes
    """
    operation_id = str(uuid.uuid4())
    scrape_ids: List[str] = []
    failed_urls: List[str] = []
    invalid_urls: List[str] = []
    completed_urls: List[str] = []

    valid_scrapes = [s for s in scrapes if isinstance(s, dict)]

    logger.info("Storing %d scrapes sequentially", len(valid_scrapes))

    for scrape in valid_scrapes:
        url = _extract_url_from_scrape(scrape)
        try:
            scrape_id = insert_scrape_record_step(scrape)
            if scrape_id:
                scrape_ids.append(scrape_id)
                if url:
                    completed_urls.append(url)
        except ApplicationError as exc:
            if getattr(exc, "type", None) == "invalid_scrape":
                logger.info("Invalid scrape for URL %s: %s", url, exc)
                if url:
                    invalid_urls.append(url)
            else:
                logger.warning("Failed to store scrape for URL %s: %s", url, exc)
                if url:
                    failed_urls.append(url)
        except Exception as e:
            logger.error("Unexpected error storing scrape for URL %s: %s", url, e)
            if url:
                failed_urls.append(url)

    # Complete URLs in queue with appropriate status
    if completed_urls:
        try:
            complete_scrape_urls_step(
                items=[{"url": url} for url in completed_urls],
                status="completed",
            )
        except Exception as e:
            logger.warning("Failed to complete URLs in queue: %s", e)

    if invalid_urls:
        try:
            complete_scrape_urls_step(
                items=[{"url": url} for url in invalid_urls],
                status="invalid",
                error="invalid_job_data",
            )
        except Exception as e:
            logger.warning("Failed to mark invalid URLs in queue: %s", e)

    if failed_urls:
        try:
            complete_scrape_urls_step(
                items=[{"url": url} for url in failed_urls],
                status="failed",
                error="store_failed",
            )
        except Exception as e:
            logger.warning("Failed to mark failed URLs in queue: %s", e)

    result = {
        "operationId": operation_id,
        "stored": len(scrape_ids),
        "scrapeIds": scrape_ids,
        "failed": len(failed_urls),
        "invalid": len(invalid_urls),
    }

    logger.info(
        "Batch storage complete: operation=%s stored=%d failed=%d invalid=%d",
        operation_id,
        len(scrape_ids),
        len(failed_urls),
        len(invalid_urls),
    )

    return result
