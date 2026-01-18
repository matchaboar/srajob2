"""DBOS step functions for job detail heuristics."""

from __future__ import annotations

from typing import Any, Dict, List

from dbos import DBOS


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def list_job_detail_configs_step(domain: str) -> List[Dict[str, Any]]:
    """Fetch job detail configs for a domain from Convex.

    Args:
        domain: The domain to fetch configs for.

    Returns:
        List of config dictionaries for the domain.

    Raises:
        Exception: On Convex query failure (for error tracking).
    """
    from ....services.convex_client import convex_query

    fetched = convex_query("router:listJobDetailConfigs", {"domain": domain})
    return fetched if isinstance(fetched, list) else []


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def record_job_detail_heuristic_step(record: Dict[str, Any]) -> None:
    """Record a job detail heuristic in Convex.

    Args:
        record: The heuristic record to store.
    """
    from ....services.convex_client import convex_mutation

    convex_mutation("router:recordJobDetailHeuristic", record)


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def list_pending_job_details_step(limit: int = 25) -> List[Dict[str, Any]]:
    """Fetch pending job details for heuristic processing.

    Args:
        limit: Maximum number of pending jobs to fetch.

    Returns:
        List of pending job detail dictionaries.
    """
    from ....services.convex_client import convex_query

    result = convex_query("router:listPendingJobDetails", {"limit": limit})
    return result if isinstance(result, list) else []


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def update_job_with_heuristic_step(job_id: str, patch: Dict[str, Any]) -> None:
    """Apply heuristic patch to a job.

    Args:
        job_id: The job ID to update.
        patch: The heuristic patch to apply.

    Raises:
        Exception: On Convex mutation failure (for error tracking).
    """
    from ....services.convex_client import convex_mutation

    convex_mutation("router:updateJobWithHeuristic", {"id": job_id, **patch})


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def count_pending_job_details_step() -> int | None:
    """Count remaining pending job details.

    Returns:
        Count of pending job details, or None if the query fails.
    """
    from ....services.convex_client import convex_query

    try:
        result = convex_query("router:countPendingJobDetails", {})
        if isinstance(result, dict):
            count = result.get("pending")
            if isinstance(count, (int, float)):
                return int(count)
        if isinstance(result, (int, float)):
            return int(result)
        return None
    except Exception:
        return None
