"""DBOS step function for storing a job description via HTTP."""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

import httpx
from dbos import DBOS

from ....config import runtime_config, settings

logger = logging.getLogger(__name__)


def _convex_http_base_url() -> Optional[str]:
    """Return the Convex HTTP base URL for file storage routes."""
    if settings.convex_http_url:
        base = settings.convex_http_url.rstrip("/")
    elif settings.convex_url:
        base = settings.convex_url.rstrip("/").replace(".convex.cloud", ".convex.site")
    else:
        return None

    if ".convex.site" not in base and ".convex.cloud" in base:
        base = base.replace(".convex.cloud", ".convex.site")

    return base


def _should_skip_description_uploads() -> bool:
    """Skip Convex HTTP uploads in test environments."""
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    return "pytest" in sys.modules


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def store_job_description_step(  # noqa: DBOS002 - using sync httpx.Client correctly
    base_url: str,
    job_id: str,
    description: str,
) -> bool:
    """Store a job description via HTTP POST to Convex.

    Args:
        base_url: The Convex HTTP base URL.
        job_id: The Convex job document ID.
        description: The job description text to store.

    Returns:
        True if the description was stored successfully, False otherwise.
    """
    try:
        with httpx.Client(
            timeout=runtime_config.spidercloud_http_timeout_seconds
        ) as client:
            response = client.post(
                f"{base_url}/api/job-description",
                json={"jobId": job_id, "description": description},
            )
            return response.status_code < 400
    except Exception:
        return False


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def store_job_descriptions_step(
    jobs: list[dict],
    source_url: Optional[str] = None,
    provider: Optional[str] = None,
    workflow_name: Optional[str] = None,
    convex_http_base_url: Optional[str] = None,
) -> None:
    """Store job descriptions for a batch of jobs via HTTP to Convex."""
    if _should_skip_description_uploads():
        logger.debug("Skipping job description uploads during tests.")
        return

    from ...helpers.link_extractors import normalize_url
    from ...helpers.scrape_utils import looks_like_truncated_description
    from .lookup_job_id_for_url import lookup_job_id_for_url

    store_job_descriptions_via_http(
        jobs=jobs,
        source_url=source_url,
        provider=provider,
        workflow_name=workflow_name,
        convex_http_base_url=convex_http_base_url or _convex_http_base_url(),
        lookup_job_id_fn=lookup_job_id_for_url,
        looks_like_truncated_fn=looks_like_truncated_description,
        normalize_url_fn=normalize_url,
        logger=logger,
    )


def store_job_descriptions_via_http(
    jobs: list[dict],
    source_url: Optional[str],
    provider: Optional[str],
    workflow_name: Optional[str],
    convex_http_base_url: Optional[str],
    lookup_job_id_fn,
    looks_like_truncated_fn,
    normalize_url_fn,
    logger,
) -> None:
    """Store job descriptions for a batch of jobs via HTTP.

    This is a helper that iterates over jobs and calls the step function
    for each valid job with a description.
    """
    if not convex_http_base_url:
        logger.warning("Convex HTTP URL missing; skipping description uploads")
        return

    stored = 0
    for job in jobs:
        description = job.get("description")
        if not isinstance(description, str) or not description.strip():
            continue
        if looks_like_truncated_fn(description):
            continue
        raw_url = job.get("url")
        if not isinstance(raw_url, str) or not raw_url.strip():
            continue
        normalized = normalize_url_fn(raw_url) or raw_url.strip()
        job_id = lookup_job_id_fn(normalized)
        if not job_id:
            continue

        success = store_job_description_step(
            base_url=convex_http_base_url,
            job_id=job_id,
            description=description,
        )
        if success:
            stored += 1
        else:
            logger.warning("Description upload failed for %s", normalized)

    if stored:
        logger.info(
            "Stored %s job descriptions for %s (provider=%s workflow=%s)",
            stored,
            source_url or "unknown site",
            provider or "unknown",
            workflow_name or "unknown",
        )
